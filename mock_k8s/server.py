"""本地 Kubernetes API 模拟服务（batch/v1 子集）。

用途：
- 提供 CronJob / Job 的读取、列表、创建，行为贴近 1.32 的标签与注解；
- 通过请求头注入故障，复现“创建成功后回包超时”：
  * X-Mock-Fault: create-timeout        —— 已落库但延迟到客户端超时；
  * X-Mock-Fault: create-timeout-lose   —— 已落库但直接断开连接（彻底无回包）；
  * X-Mock-Fault: create-conflict-once  —— 第一次创建返回 409，之后正常。
- 种子数据覆盖：暂停的每日巡检 Job、带 DST 的纽约时区 Job、未暂停 Job、无时区 Job。
"""
from __future__ import annotations

import asyncio
import copy
import os
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.kube import (
    ANN_CRONJOB_UID,
    ANN_JOB_NAME,
    LABEL_CRONJOB_NAME,
)

API_PREFIX = "/apis/batch/v1"


def rfc3339_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _uid(prefix: str, seed: str) -> str:
    # 确定性 UID，方便测试断言
    return f"{prefix}-{uuid.uuid5(uuid.NAMESPACE_URL, seed)}"


def seed_state() -> dict:
    cj_daily = {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "inspection-daily",
            "namespace": "default",
            "uid": _uid("cronjob", "inspection-daily"),
            "resourceVersion": "1",
        },
        "spec": {
            "schedule": "0 9 * * *",
            "timeZone": "Asia/Shanghai",
            "suspend": True,
            "concurrencyPolicy": "Forbid",
            "jobTemplate": {
                "metadata": {"labels": {"app": "inspection"}},
                "spec": {
                    "template": {
                        "metadata": {"labels": {"app": "inspection"}},
                        "spec": {
                            "restartPolicy": "OnFailure",
                            "containers": [
                                {"name": "worker", "image": "busybox:1.36",
                                 "command": ["sh", "-c", "echo inspect"]}
                            ],
                        },
                    }
                },
            },
        },
    }
    cj_dst = {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "inspection-ny",
            "namespace": "default",
            "uid": _uid("cronjob", "inspection-ny"),
            "resourceVersion": "1",
        },
        "spec": {
            "schedule": "30 1 * * *",
            "timeZone": "America/New_York",
            "suspend": True,
            "jobTemplate": {
                "metadata": {"labels": {"app": "inspection-ny"}},
                "spec": {
                    "template": {
                        "spec": {
                            "restartPolicy": "Never",
                            "containers": [
                                {"name": "worker", "image": "busybox:1.36",
                                 "command": ["sh", "-c", "echo ny"]}
                            ],
                        }
                    }
                },
            },
        },
    }
    cj_running = copy.deepcopy(cj_daily)
    cj_running["metadata"]["name"] = "inspection-running"
    cj_running["metadata"]["uid"] = _uid("cronjob", "inspection-running")
    cj_running["spec"]["suspend"] = False

    cj_no_tz = copy.deepcopy(cj_daily)
    cj_no_tz["metadata"]["name"] = "legacy-no-tz"
    cj_no_tz["metadata"]["uid"] = _uid("cronjob", "legacy-no-tz")
    del cj_no_tz["spec"]["timeZone"]

    cj_weekly = copy.deepcopy(cj_daily)
    cj_weekly["metadata"]["name"] = "inspection-weekly"
    cj_weekly["metadata"]["uid"] = _uid("cronjob", "inspection-weekly")
    cj_weekly["spec"]["schedule"] = "0 9 * * MON"

    cj_utc = copy.deepcopy(cj_daily)
    cj_utc["metadata"]["name"] = "inspection-utc"
    cj_utc["metadata"]["uid"] = _uid("cronjob", "inspection-utc")
    cj_utc["spec"]["schedule"] = "30 0 * * *"
    cj_utc["spec"]["timeZone"] = "UTC"
    # 无任何预置 Job，用于窗口/到点边界测试

    cronjobs = {
        ("default", "inspection-daily"): cj_daily,
        ("default", "inspection-ny"): cj_dst,
        ("default", "inspection-running"): cj_running,
        ("default", "legacy-no-tz"): cj_no_tz,
        ("default", "inspection-weekly"): cj_weekly,
        ("default", "inspection-utc"): cj_utc,
    }

    # 原调度器已为 daily 建过一轮 Job（1.32 风格标签/注解）
    scheduler_job = _scheduler_job(cj_daily, "2026-11-20T01:00:00Z")
    jobs: dict[tuple[str, str], dict] = {
        ("default", scheduler_job["metadata"]["name"]): scheduler_job
    }
    return {"cronjobs": cronjobs, "jobs": jobs, "conflict_used": set()}


def _scheduler_job(cronjob: dict, scheduled_iso: str) -> dict:
    cj_name = cronjob["metadata"]["name"]
    cj_uid = cronjob["metadata"]["uid"]
    stamp = scheduled_iso.replace("-", "").replace(":", "").replace("T", "")[:10]
    name = f"{cj_name}-{stamp}"
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": "default",
            "uid": _uid("job", name),
            "creationTimestamp": rfc3339_now(),
            "resourceVersion": "2",
            "labels": {
                LABEL_CRONJOB_NAME: cj_name,
                **cronjob["spec"]["jobTemplate"].get("metadata", {})
                .get("labels", {}),
            },
            "annotations": {
                ANN_CRONJOB_UID: cj_uid,
                ANN_JOB_NAME: scheduled_iso,
            },
            "ownerReferences": [
                {"apiVersion": "batch/v1", "kind": "CronJob",
                 "name": cj_name, "uid": cj_uid}
            ],
        },
        "spec": copy.deepcopy(cronjob["spec"]["jobTemplate"]["spec"]),
    }


def create_mock_app(state: dict | None = None) -> FastAPI:
    app = FastAPI(title="Kubernetes API 模拟服务", version="1.32-mock")
    app.state.store = state or seed_state()
    # 模拟服务端处理延迟（秒），故障时配合使用
    app.state.slow_delay = float(os.environ.get("MOCK_SLOW_DELAY", "8"))

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get(f"{API_PREFIX}/namespaces/{{namespace}}/cronjobs/{{name}}")
    async def get_cronjob(namespace: str, name: str):
        obj = app.state.store["cronjobs"].get((namespace, name))
        if obj is None:
            return _status(404, "NotFound", f"cronjobs {name} not found")
        return copy.deepcopy(obj)

    @app.get(f"{API_PREFIX}/namespaces/{{namespace}}/cronjobs")
    async def list_cronjobs(namespace: str):
        items = [
            copy.deepcopy(cj)
            for (ns, _), cj in app.state.store["cronjobs"].items()
            if ns == namespace
        ]
        return {"apiVersion": "batch/v1", "kind": "CronJobList", "items": items}

    @app.get(f"{API_PREFIX}/namespaces/{{namespace}}/jobs")
    async def list_jobs(namespace: str):
        items = [
            copy.deepcopy(job)
            for (ns, _), job in app.state.store["jobs"].items()
            if ns == namespace
        ]
        return {"apiVersion": "batch/v1", "kind": "JobList", "items": items}

    @app.get(f"{API_PREFIX}/namespaces/{{namespace}}/jobs/{{name}}")
    async def get_job(namespace: str, name: str):
        obj = app.state.store["jobs"].get((namespace, name))
        if obj is None:
            return _status(404, "NotFound", f"jobs {name} not found")
        return copy.deepcopy(obj)

    @app.post(f"{API_PREFIX}/namespaces/{{namespace}}/jobs")
    async def create_job(namespace: str, request: Request):
        body = await request.json()
        name = body["metadata"]["name"]
        key = (namespace, name)
        fault = request.headers.get("X-Mock-Fault", "")

        # 409 一次：模拟同名并发冲突
        if fault == "create-conflict-once" and key not in app.state.store["conflict_used"]:
            app.state.store["conflict_used"].add(key)
            return _status(409, "AlreadyExists", f"jobs {name} already exists")

        if key in app.state.store["jobs"]:
            return _status(409, "AlreadyExists", f"jobs {name} already exists")

        stored = copy.deepcopy(body)
        stored["metadata"].setdefault("uid", str(uuid.uuid4()))
        stored["metadata"].setdefault("creationTimestamp", rfc3339_now())
        stored["metadata"].setdefault("resourceVersion", "1")
        app.state.store["jobs"][key] = stored

        if fault in ("create-timeout", "create-timeout-lose"):
            # 关键：对象已落库，再制造回包超时（滞留时间由 MOCK_SLOW_DELAY 控制，
            # 需大于客户端 KUBE_CREATE_TIMEOUT）。
            # ASGI/HTTP 下客户端会先超时断开；lose 模式在断开后直接吞掉响应。
            await asyncio.sleep(app.state.slow_delay)
            if fault == "create-timeout-lose":
                # 模拟连接被对端静默丢弃：返回非标准网关错误且无业务体
                return JSONResponse(
                    status_code=599,
                    content={"reason": "ConnectionLost", "message": "response lost"},
                )
            return _status(504, "Timeout", "simulated gateway timeout after persist")

        return JSONResponse(status_code=201, content=copy.deepcopy(stored))

    @app.post("/_mock/reset")
    async def reset():
        app.state.store = seed_state()
        return {"reset": True}

    return app


def _status(code: int, reason: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=code,
        content={
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "reason": reason,
            "message": message,
            "code": code,
        },
    )


app = create_mock_app()
