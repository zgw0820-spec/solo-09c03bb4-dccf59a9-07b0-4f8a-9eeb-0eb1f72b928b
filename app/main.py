"""巡检补跑 FastAPI 入口。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query

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
    app.state.service = CatchupService(app.state.kube, app.state.store)
    try:
        yield
    finally:
        await app.state.kube.aclose()
        app.state.store.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="CronJob 巡检补跑服务",
        version="1.0.0",
        description=(
            "针对检修期间暂停的 CronJob（batch/v1，Kubernetes 1.32+），\n"
            "- 查询设有时区、每天固定时刻的 CronJob 最近一次应到点的计划；\n"
            "- 按 CronJob UID + 计划时刻匹配已有 Job（原调度器创建的也算）；\n"
            "- 仅在 CronJob 仍暂停且到点后 2 小时内（UTC）按 jobTemplate 补建 Job；\n"
            "- 创建回包超时先回查集群，查不清报“待核实”，重启后仍可查回。\n\n"
            "所有接口支持 `at` 参数（ISO8601）指定演练时间，缺省为当前 UTC。"
        ),
        lifespan=lifespan,
    )

    def svc() -> CatchupService:
        return app.state.service

    @app.get("/health", response_model=HealthResponse, tags=["运维"])
    async def health() -> Any:
        return {"status": "ok", "kubernetes_api": app.state.kube.config.base_url}

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
        except ServiceError as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
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
        except ServiceError as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
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
        except ServiceError as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
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
