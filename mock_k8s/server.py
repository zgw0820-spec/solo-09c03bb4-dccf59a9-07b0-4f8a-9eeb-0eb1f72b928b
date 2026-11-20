"""本地 Kubernetes API 模拟服务（batch/v1 子集）。

用途：
- 提供 CronJob / Job 的读取、列表、创建、WATCH，行为贴近 1.32 的标签与注解；
- Job 带单调递增的 resourceVersion，LIST 返回列表级 resourceVersion；
- 通过请求头注入故障，复现“创建成功后回包超时”：
  * X-Mock-Fault: create-timeout        —— 已落库但延迟到客户端超时；
  * X-Mock-Fault: create-timeout-lose   —— 已落库但直接断开连接（彻底无回包）；
  * X-Mock-Fault: create-conflict-once  —— 第一次创建返回 409，之后正常。
- 通过 `/_mock/*` 控制面演示索引断线/410/恢复：
  * POST /_mock/watch-disconnect        —— 断开全部 WATCH 连接（正常结束，可重连）；
  * POST /_mock/expire-resource-version —— 让旧 resourceVersion 的 WATCH 返回 410；
  * POST /_mock/watch-recover           —— 恢复 WATCH 服务并从最新清单继续；
  * GET  /_mock/stats                   —— LIST/WATCH 计数，核对“不再每次扫描全部 Job”。
- 种子数据覆盖：暂停的每日巡检 Job、带 DST 的纽约时区 Job、未暂停 Job、无时区 Job。
"""
from __future__ import annotations

import asyncio
import copy
import json
import os
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

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
    scheduler_job["metadata"]["resourceVersion"] = "1"
    jobs: dict[tuple[str, str], dict] = {
        ("default", scheduler_job["metadata"]["name"]): scheduler_job
    }
    return {
        "cronjobs": cronjobs,
        "jobs": jobs,
        "conflict_used": set(),
        # 单调递增的集群 resourceVersion（简化：整个模拟集群共用一个计数器）
        "rv": 1,
        # WATCH 事件日志：LIST 之后按 (rv > 起始 rv) 增量补齐，语义贴近真实 watch
        "events": [],
        # 控制面：watch_active=False 时新建 WATCH 返回 503（模拟 apiserver 不可达）
        "watch_active": True,
        "watch_generation": 0,
        # WATCH 起始 rv 小于该值 -> 410 ResourceExpired
        "expire_before": None,
        # Job LIST 人为延迟（秒），用于演示“首次索引未就绪”
        "list_delay": 0.0,
        "stats": {
            "job_list": 0,
            "job_watch": 0,
            "watch_events": 0,
            "watch_410": 0,
            "watch_disconnects": 0,
        },
    }


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
    # 每个 WATCH 连接的唤醒事件；断线/恢复时统一 set
    app.state.watch_wakeup = asyncio.Event()
    # WATCH 每次最多推送多少个事件后正常结束（模拟 apiserver 超时重连）
    app.state.watch_batch = int(os.environ.get("MOCK_WATCH_BATCH", "200"))

    def _st() -> dict:
        # reset 会整体替换 store，这里始终取当前对象
        return app.state.store

    def _bump_rv() -> str:
        st = _st()
        st["rv"] += 1
        return str(st["rv"])

    def _record_event(event_type: str, obj: dict) -> None:
        _st()["events"].append(
            {
                "type": event_type,
                "object": copy.deepcopy(obj),
                "rv": int(obj["metadata"]["resourceVersion"]),
            }
        )

    app.state.on_job_changed = _record_event

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
    async def list_jobs(namespace: str, request: Request):
        st = _st()
        watch = request.query_params.get("watch")
        if watch in ("1", "true"):
            return await _watch_jobs(namespace, request)
        st["stats"]["job_list"] += 1
        if st["list_delay"] > 0:
            await asyncio.sleep(st["list_delay"])
        items = [
            copy.deepcopy(job)
            for (ns, _), job in st["jobs"].items()
            if ns == namespace
        ]
        return {
            "apiVersion": "batch/v1",
            "kind": "JobList",
            "items": items,
            "metadata": {"resourceVersion": str(st["rv"])},
        }

    async def _watch_jobs(namespace: str, request: Request):
        st = _st()
        try:
            since = int(request.query_params.get("resourceVersion") or "0")
        except ValueError:
            return _status(400, "BadRequest", "非法 resourceVersion")

        if not st["watch_active"]:
            return _status(503, "ServiceUnavailable", "watch endpoint unavailable")

        expire_before = st["expire_before"]
        if expire_before is not None and since < int(expire_before):
            st["stats"]["watch_410"] += 1
            return _status(
                410, "Expired",
                f"too old resource version: {since} ({expire_before})",
            )

        st["stats"]["job_watch"] += 1
        generation = st["watch_generation"]
        wakeup = app.state.watch_wakeup

        async def stream():
            cursor = since
            emitted = 0
            # 立即回一个 BOOKMARK：真实 apiserver 会先发响应头/心跳事件，
            # 这里也让客户端能立刻确认 WATCH 已建立（ASGI 流式传输需要首个分片）。
            yield json.dumps(
                {
                    "type": "BOOKMARK",
                    "object": {
                        "apiVersion": "batch/v1",
                        "kind": "Job",
                        "metadata": {"resourceVersion": str(st["rv"])},
                    },
                }
            ) + "\n"
            idle_since = asyncio.get_running_loop().time()
            while True:
                st_now = _st()
                if not st_now["watch_active"]:
                    return  # 模拟 apiserver 主动结束 WATCH
                # 断线控制：控制面触发后结束本连接（客户端需重连并重新 LIST）
                if st_now["watch_generation"] != generation:
                    return
                pending = [e for e in list(st_now["events"]) if e["rv"] > cursor]
                if pending:
                    for ev in pending:
                        payload = {"type": ev["type"], "object": ev["object"]}
                        yield json.dumps(payload) + "\n"
                        st_now["stats"]["watch_events"] += 1
                        cursor = ev["rv"]
                        emitted += 1
                        if emitted >= app.state.watch_batch:
                            return  # 模拟 apiserver 超时正常结束，客户端重连
                    idle_since = asyncio.get_running_loop().time()
                    continue
                try:
                    await asyncio.wait_for(wakeup.wait(), timeout=0.2)
                except asyncio.TimeoutError:
                    pass
                if asyncio.get_running_loop().time() - idle_since > float(
                    request.query_params.get("timeoutSeconds") or 300
                ):
                    return  # watch 超时正常结束

        return StreamingResponse(stream(), media_type="application/json")

    @app.get(f"{API_PREFIX}/namespaces/{{namespace}}/jobs/{{name}}")
    async def get_job(namespace: str, name: str):
        obj = app.state.store["jobs"].get((namespace, name))
        if obj is None:
            return _status(404, "NotFound", f"jobs {name} not found")
        return copy.deepcopy(obj)

    @app.post(f"{API_PREFIX}/namespaces/{{namespace}}/jobs")
    async def create_job(namespace: str, request: Request):
        st = _st()
        body = await request.json()
        name = body["metadata"]["name"]
        key = (namespace, name)
        fault = request.headers.get("X-Mock-Fault", "")

        # 409 一次：模拟同名并发冲突
        if fault == "create-conflict-once" and key not in st["conflict_used"]:
            st["conflict_used"].add(key)
            return _status(409, "AlreadyExists", f"jobs {name} already exists")

        if key in st["jobs"]:
            return _status(409, "AlreadyExists", f"jobs {name} already exists")

        stored = copy.deepcopy(body)
        stored["metadata"].setdefault("uid", str(uuid.uuid4()))
        stored["metadata"].setdefault("creationTimestamp", rfc3339_now())
        stored["metadata"]["resourceVersion"] = _bump_rv()
        st["jobs"][key] = stored
        _record_event("ADDED", stored)

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

    # ---------- 控制面：演示索引断线 / 410 / 恢复 ----------

    @app.post("/_mock/watch-disconnect")
    async def watch_disconnect():
        """模拟连接丢失：断开现有 WATCH 且暂不接受重连（WATCH 返回 503）。

        客户端会立即重连失败、保留上一次完整清单并标记过期；
        调用 `/_mock/watch-recover` 恢复（客户端重连后重新 LIST 重建索引）。
        """
        st = _st()
        st["watch_active"] = False
        st["watch_generation"] += 1
        st["stats"]["watch_disconnects"] += 1
        old, app.state.watch_wakeup = app.state.watch_wakeup, asyncio.Event()
        old.set()
        return {"disconnected": True, "generation": st["watch_generation"]}

    @app.post("/_mock/expire-resource-version")
    async def expire_resource_version(before: int | None = None, persistent: bool = True):
        """模拟 resourceVersion 过期：起始 rv 小于 before 的 WATCH 返回 410。

        before 缺省取当前 rv（persistent=True 时再抬高，使重新 LIST 后仍 410，
        直到 `/_mock/watch-recover`）；persistent=False 时只让现有连接过期一次，
        重新 LIST 后可立即恢复。
        """
        st = _st()
        if before is not None:
            target = int(before)
        else:
            target = st["rv"] + (1000 if persistent else 0)
        st["expire_before"] = target
        st["watch_generation"] += 1
        old, app.state.watch_wakeup = app.state.watch_wakeup, asyncio.Event()
        old.set()
        return {"expire_before": st["expire_before"]}

    @app.post("/_mock/watch-recover")
    async def watch_recover():
        """恢复 WATCH 服务：清除 410 规则，允许重连并从最新清单重建。"""
        st = _st()
        st["expire_before"] = None
        st["watch_active"] = True
        st["watch_generation"] += 1
        old, app.state.watch_wakeup = app.state.watch_wakeup, asyncio.Event()
        old.set()
        return {"recovered": True, "resource_version": str(st["rv"])}

    @app.post("/_mock/watch-down")
    async def watch_down():
        """模拟 apiserver 不可达：新 WATCH 请求返回 503（与断线同义，保留别名）。"""
        return await watch_disconnect()

    @app.post("/_mock/list-delay")
    async def list_delay(seconds: float = 0.0):
        """让后续 Job LIST 延迟返回，用于演示“首次索引未就绪”。"""
        st = _st()
        st["list_delay"] = float(seconds)
        return {"list_delay": st["list_delay"]}

    @app.get("/_mock/stats")
    async def stats():
        st = _st()
        return {
            "resource_version": str(st["rv"]),
            "watch_active": st["watch_active"],
            "expire_before": st["expire_before"],
            "list_delay": st["list_delay"],
            "job_count": len(st["jobs"]),
            **st["stats"],
        }

    @app.post("/_mock/reset")
    async def reset():
        app.state.store = seed_state()
        old, app.state.watch_wakeup = app.state.watch_wakeup, asyncio.Event()
        old.set()
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
