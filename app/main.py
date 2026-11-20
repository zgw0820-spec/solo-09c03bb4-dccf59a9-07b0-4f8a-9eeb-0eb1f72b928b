"""巡检补跑 FastAPI 入口。"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from .jobindex import JobIndexManager
from .kube import KubeClient, KubeConfig, KubeError
from .models import (
    CatchupResponse,
    HealthResponse,
    RecordResponse,
    ScheduleResponse,
)
from .schedule import CronError
from .service import CatchupService, ServiceError
from .store import CatchupStore

DB_PATH = os.environ.get("CATCHUP_DB", "/data/catchup.db")


def parse_rehearsal_time(value: datetime | None) -> datetime:
    """演练时间参数：缺省取当前 UTC；无时区的输入按 UTC 处理。"""
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = KubeConfig.from_env()
    app.state.kube = KubeClient(config)
    app.state.store = CatchupStore(DB_PATH)
    # 重启恢复：创建到一半的记录转待核实
    app.state.store.recover_creating()
    app.state.indexes = JobIndexManager(
        app.state.kube,
        initial_wait=float(os.environ.get("JOB_INDEX_INITIAL_WAIT", "5")),
    )
    app.state.service = CatchupService(
        app.state.kube, app.state.store, app.state.indexes
    )
    try:
        yield
    finally:
        await app.state.indexes.close()
        await app.state.kube.aclose()
        app.state.store.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="CronJob 巡检补跑服务",
        version="1.2.0",
        description=(
            "针对检修期间暂停的 CronJob（batch/v1，Kubernetes 1.32+），\n"
            "- 查询设有时区、每天固定时刻的 CronJob 最近一次应到点的计划；\n"
            "- Job 盘点按命名空间复用实时索引：初始 LIST + WATCH 增量维护，\n"
            "  只按 CronJob UID + 计划时刻命中，不再每次扫描命名空间全部 Job；\n"
            "- 仅在 CronJob 仍暂停且到点后 2 小时内（UTC）按 jobTemplate 补建 Job；\n"
            "- WATCH 断线/410 期间保留上一次完整索引并标记过期；首次索引未就绪\n"
            "  时查询返回 503，补跑返回 pending_verification，均不把空结果当成无 Job；\n"
            "- 创建回包超时先回查集群，查不清报“待核实”，重启后仍可查回。\n\n"
            "所有接口支持 `at` 参数（ISO8601）指定演练时间，缺省为当前 UTC。"
        ),
        lifespan=lifespan,
    )

    def svc() -> CatchupService:
        return app.state.service

    @app.exception_handler(ServiceError)
    async def _service_error_handler(request: Request, exc: ServiceError):
        headers = {}
        if exc.index_status is not None:
            # 明确返回未就绪/过期状态，便于调用方识别“不可判定”
            headers["X-Job-Index-State"] = str(exc.index_status.get("state"))
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.message, "index_status": exc.index_status},
            headers=headers,
        )

    @app.get("/health", response_model=HealthResponse, tags=["运维"])
    async def health() -> Any:
        return {
            "status": "ok",
            "kubernetes_api": app.state.kube.config.base_url,
            "job_indexes": app.state.indexes.statuses(),
        }

    @app.get(
        "/job-indexes",
        tags=["运维"],
        summary="查看各命名空间 Job 实时索引状态（ready/stale/initializing）",
    )
    async def job_indexes() -> Any:
        return app.state.indexes.statuses()

    # ---------- 演练控制：Swagger 上演示索引断线 / 410 / 恢复 ----------

    async def _wait_index_state(
        namespace: str, states: set[str], timeout: float = 5.0
    ) -> Any:
        index = app.state.indexes.get(namespace)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if index.status().state in states:
                break
            await asyncio.sleep(0.05)
        return index

    @app.post(
        "/rehearsal/job-index/{namespace}/disconnect",
        tags=["演练"],
        summary="模拟 WATCH 断线（索引转 stale，保留上次完整清单）",
    )
    async def rehearsal_disconnect(namespace: str) -> Any:
        await app.state.kube.mock_control("watch-disconnect")
        index = await _wait_index_state(namespace, {"stale"})
        return {"action": "disconnect", "index": index.status().as_dict()}

    @app.post(
        "/rehearsal/job-index/{namespace}/expire",
        tags=["演练"],
        summary="模拟 resourceVersion 过期（WATCH 返回 410，索引转 stale）",
    )
    async def rehearsal_expire(namespace: str) -> Any:
        await app.state.kube.mock_control("expire-resource-version")
        index = await _wait_index_state(namespace, {"stale"})
        return {"action": "expire", "index": index.status().as_dict()}

    @app.post(
        "/rehearsal/job-index/{namespace}/recover",
        tags=["演练"],
        summary="恢复 WATCH：重新 LIST 重建索引并回到 ready",
    )
    async def rehearsal_recover(namespace: str) -> Any:
        await app.state.kube.mock_control("watch-recover")
        app.state.indexes.get(namespace).start()
        index = await _wait_index_state(namespace, {"ready"}, timeout=8)
        return {"action": "recover", "index": index.status().as_dict()}

    @app.post(
        "/rehearsal/job-index/{namespace}/not-ready",
        tags=["演练"],
        summary="模拟首次索引未就绪（LIST 变慢 + 重建索引），用于演示 503/待核实",
    )
    async def rehearsal_not_ready(
        namespace: str,
        list_delay: float = Query(6.0, description="模拟 Job LIST 的延迟秒数"),
    ) -> Any:
        await app.state.kube.mock_control(
            "list-delay", params={"seconds": str(list_delay)}
        )
        index = await app.state.indexes.restart(namespace)
        status = await index.ensure_started(timeout=0.5)
        return {"action": "not-ready", "index": status.status.as_dict()}

    @app.get(
        "/rehearsal/mock-stats",
        tags=["演练"],
        summary="模拟 K8s 计数（核对只 LIST 一次、后续均由 WATCH 增量维护）",
    )
    async def rehearsal_mock_stats() -> Any:
        return await app.state.kube.mock_control("stats", method="GET")

    @app.get(
        "/namespaces/{namespace}/cronjobs/{name}/latest-schedule",
        response_model=ScheduleResponse,
        tags=["巡检"],
        summary="查最近一次到点计划及是否已有 Job",
    )
    async def latest_schedule(
        namespace: str,
        name: str,
        at: datetime | None = Query(
            None, description="演练时间（ISO8601，可带时区；缺省当前 UTC）"
        ),
    ) -> Any:
        now = parse_rehearsal_time(at)
        try:
            return await svc().inspect(namespace, name, now)
        except ServiceError:
            raise
        except CronError as e:
            raise HTTPException(status_code=422, detail=f"调度计算失败: {e}")

    @app.post(
        "/namespaces/{namespace}/cronjobs/{name}/catchup",
        response_model=CatchupResponse,
        tags=["巡检"],
        summary="按最近一轮计划补跑（有 Job 则原样返回）",
    )
    async def catchup(
        namespace: str,
        name: str,
        at: datetime | None = Query(
            None, description="演练时间（ISO8601，可带时区；缺省当前 UTC）"
        ),
    ) -> Any:
        now = parse_rehearsal_time(at)
        try:
            return await svc().catchup(namespace, name, now)
        except ServiceError:
            raise
        except CronError as e:
            raise HTTPException(status_code=422, detail=f"调度计算失败: {e}")
        except KubeError as e:
            raise HTTPException(status_code=502, detail=f"Kubernetes API 错误: {e}")

    @app.post(
        "/namespaces/{namespace}/cronjobs/{name}/reverify",
        response_model=CatchupResponse,
        tags=["巡检"],
        summary="对“待核实”的补跑记录重新回查集群",
    )
    async def reverify(
        namespace: str,
        name: str,
        at: datetime | None = Query(None, description="演练时间（ISO8601）"),
    ) -> Any:
        now = parse_rehearsal_time(at)
        try:
            return await svc().reverify(namespace, name, now)
        except ServiceError:
            raise
        except CronError as e:
            raise HTTPException(status_code=422, detail=f"调度计算失败: {e}")

    @app.get(
        "/records",
        response_model=list[RecordResponse],
        tags=["台账"],
        summary="查看补跑台账（重启后仍可查回）",
    )
    async def records(limit: int = Query(100, ge=1, le=500)) -> Any:
        return app.state.store.list_records(limit)

    return app


app = create_app()
