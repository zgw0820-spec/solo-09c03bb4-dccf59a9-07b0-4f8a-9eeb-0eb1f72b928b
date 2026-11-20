"""按命名空间复用的 Job 实时索引：LIST 一次 + WATCH 增量、断线/410/恢复语义。"""
import asyncio
from datetime import datetime, timedelta, timezone

from app.kube import build_catchup_job

UTC = timezone.utc
DAILY = "inspection-daily"
NY_CJ = "inspection-ny"
SCHEDULED_DAILY = "2026-11-20T01:00:00Z"


def _stats(harness) -> dict:
    return harness["state"]["stats"]


async def _wait_state(harness, namespace: str, state: str, timeout: float = 5.0):
    """等待索引进入指定状态，避免依赖固定 sleep。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        index = harness["indexes"].get(namespace)
        if index.status().state == state:
            return index.status()
        await asyncio.sleep(0.05)
    index = harness["indexes"].get(namespace)
    raise AssertionError(
        f"索引未达到 {state}，当前 {index.status() if index else None}"
    )


async def _latest(harness, cj: str = DAILY, at: str = "2026-11-20T01:30:00Z"):
    return await harness["api"].get(
        f"/namespaces/default/cronjobs/{cj}/latest-schedule", params={"at": at}
    )


async def test_initial_list_then_query_uses_index_without_rescan(harness):
    first = await _latest(harness)
    assert first.status_code == 200, first.text
    body = first.json()
    assert body["index_state"] == "ready"
    assert body["index_stale"] is False

    for _ in range(5):
        resp = await _latest(harness)
        assert resp.status_code == 200
        assert resp.json()["existing_job"] is not None

    # 连续查询只做过一次初始 LIST
    assert _stats(harness)["job_list"] == 1


async def test_watch_incrementally_adds_job_for_other_round(harness):
    # 先建立索引
    await _latest(harness)
    assert _stats(harness)["job_list"] == 1

    # 集群新增一个不属于当前轮次的 Job：应通过 WATCH 增量进入索引
    cj = harness["state"]["cronjobs"][("default", DAILY)]
    job = build_catchup_job(cj, datetime(2026, 11, 19, 1, 0, tzinfo=UTC), "daily-old")
    # 模拟端：落库并产生 ADDED 事件
    harness["state"]["rv"] += 1
    job["metadata"]["resourceVersion"] = str(harness["state"]["rv"])
    harness["state"]["jobs"][("default", "daily-old")] = job
    harness["state"]["events"].append(
        {"type": "ADDED", "object": job, "rv": harness["state"]["rv"]}
    )

    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5
    while loop.time() < deadline:
        view = harness["indexes"].get("default").view()
        if (cj["metadata"]["uid"], "2026-11-19T01:00:00Z") in view.entries:
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError(f"WATCH 未把新 Job 增量进索引: {view.entries.keys()}")

    # 仍然是同一次 LIST，没有重新扫描
    assert _stats(harness)["job_list"] == 1

    # 只按 (UID, 计划时刻) 命中：查询昨天那轮能看到，但今天的查询不受影响
    view = harness["indexes"].get("default").view()
    assert view.lookup(cj["metadata"]["uid"], "2026-11-20T01:00:00Z") is not None
    assert view.lookup(cj["metadata"]["uid"], "2026-11-19T01:00:00Z") is not None
    # 同一 CronJob 的其它计划时刻不会被误命中
    assert view.lookup(cj["metadata"]["uid"], "2026-11-18T01:00:00Z") is None


async def test_disconnect_serves_last_full_index_with_stale_flag(harness):
    await _latest(harness)

    # 断开 WATCH：索引保留上次完整清单，但必须标记过期
    await _mock_call(harness, "watch-disconnect")

    status = await _wait_state(harness, "default", "stale")
    assert status.has_snapshot is True
    assert status.job_count >= 1

    resp = await _latest(harness)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # 查询带上一次完整索引 + 过期状态
    assert body["index_state"] == "stale"
    assert body["index_stale"] is True
    assert body["index_status"]["has_snapshot"] is True
    assert body["index_status"]["stale_since"] is not None
    # 过期期间已有 Job 仍可命中（基于上次完整索引），但结果明确标注过期
    assert body["existing_job"] is not None
    assert "索引已过期" in (body.get("reason") or "") or body["index_stale"] is True


async def _mock_call(harness, action: str, method: str = "POST", **kwargs):
    """通过被测服务自己的 Kube 客户端调控制面（与生产演练走同一条链路）。"""
    return await harness["kube"].mock_control(action, method=method, **kwargs)


async def test_catchup_returns_pending_while_stale_not_empty_success(harness):
    probe = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    sched_resp = await _latest(harness, NY_CJ, probe.isoformat())
    sched = datetime.fromisoformat(
        sched_resp.json()["scheduled_at_utc"].replace("Z", "+00:00")
    )
    at = (sched + timedelta(minutes=20)).isoformat()

    await _mock_call(harness, "watch-disconnect")
    await _wait_state(harness, "default", "stale")

    resp = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup", params={"at": at}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # 索引过期：不得创建，也不得把空结果当作“无 Job”
    assert body["status"] == "pending_verification"
    assert body["index_stale"] is True
    assert body["job"] is None
    assert "索引" in body["reason"]
    # 未落台账、未创建 Job
    assert harness["store"].list_records() == []
    assert len(harness["state"]["jobs"]) == 1


async def test_recover_after_disconnect_rebuilds_and_continues(harness):
    await _latest(harness)
    await _mock_call(harness, "watch-disconnect")
    await _wait_state(harness, "default", "stale")

    await _mock_call(harness, "watch-recover")
    status = await _wait_state(harness, "default", "ready")
    assert status.stale_since is None

    # 恢复后重新 LIST 重建索引并继续查询
    resp = await _latest(harness)
    assert resp.status_code == 200
    body = resp.json()
    assert body["index_state"] == "ready"
    assert body["index_stale"] is False
    assert body["existing_job"] is not None
    assert _stats(harness)["job_list"] >= 2

    # 恢复后新增 Job 仍能被 WATCH 增量维护
    from app.kube import build_catchup_job

    cj = harness["state"]["cronjobs"][("default", DAILY)]
    job = build_catchup_job(cj, datetime(2026, 11, 18, 1, 0, tzinfo=UTC), "daily-older")
    harness["state"]["rv"] += 1
    job["metadata"]["resourceVersion"] = str(harness["state"]["rv"])
    harness["state"]["jobs"][("default", "daily-older")] = job
    harness["state"]["events"].append(
        {"type": "ADDED", "object": job, "rv": harness["state"]["rv"]}
    )

    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5
    while loop.time() < deadline:
        if harness["indexes"].get("default").view().lookup(
            cj["metadata"]["uid"], "2026-11-18T01:00:00Z"
        ):
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("恢复后 WATCH 未能增量维护")

    # 恢复后补跑可正常创建
    probe = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    sched_resp = await _latest(harness, NY_CJ, probe.isoformat())
    sched = datetime.fromisoformat(
        sched_resp.json()["scheduled_at_utc"].replace("Z", "+00:00")
    )
    created = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
        params={"at": (sched + timedelta(minutes=20)).isoformat()},
    )
    assert created.status_code == 200
    assert created.json()["status"] == "created"


async def test_resource_version_expired_marks_stale_then_recovers(harness):
    await _latest(harness)
    assert _stats(harness)["job_list"] == 1

    # 让现有 WATCH 的 rv 过期：模拟 apiserver 压缩历史（410，持续到 recover）
    await _mock_call(harness, "expire-resource-version")
    status = await _wait_state(harness, "default", "stale")
    assert status.has_snapshot is True
    assert "410" in (status.last_error or "") or "过期" in (status.last_error or "")
    assert _stats(harness)["watch_410"] >= 1

    body = (await _latest(harness)).json()
    assert body["index_state"] == "stale"
    assert body["index_stale"] is True
    assert body["index_status"]["last_error"]

    # 410 期间补跑同样只能待核实
    probe = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    sched_resp = await _latest(harness, NY_CJ, probe.isoformat())
    sched = datetime.fromisoformat(
        sched_resp.json()["scheduled_at_utc"].replace("Z", "+00:00")
    )
    resp = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
        params={"at": (sched + timedelta(minutes=20)).isoformat()},
    )
    assert resp.json()["status"] == "pending_verification"
    assert harness["store"].list_records() == []

    # 恢复后重新 LIST 并回到 ready
    await _mock_call(harness, "watch-recover")
    status = await _wait_state(harness, "default", "ready")
    assert status.has_snapshot
    body = (await _latest(harness)).json()
    assert body["index_state"] == "ready"
    assert body["existing_job"] is not None


async def test_first_index_not_ready_returns_503_and_pending(harness):
    """首次 LIST 未就绪：查询明确 503 未就绪，补跑返回待核实。"""
    # 先让索引建立过一次，确保 _indexes 中存在 "default"
    await _latest(harness)

    original = harness["kube"].list_jobs_page
    gate = asyncio.Event()

    async def slow_list(namespace):
        await gate.wait()
        return await original(namespace)

    harness["kube"].list_jobs_page = slow_list
    try:
        # 换上一个仍在 initializing 的索引实例
        from app.jobindex import NamespaceJobIndex

        fresh = NamespaceJobIndex("default", harness["kube"], initial_wait=0.2)
        await harness["indexes"]._indexes["default"].close()
        harness["indexes"]._indexes["default"] = fresh
        fresh.start()
        await asyncio.sleep(0.1)
        assert fresh.status().state == "initializing"
        assert fresh.status().has_snapshot is False

        resp = await harness["api"].get(
            "/namespaces/default/cronjobs/inspection-daily/latest-schedule"
        )
        assert resp.status_code == 503, resp.text
        detail = resp.json()
        assert "索引尚未就绪" in detail["detail"]
        assert detail["index_status"]["state"] == "initializing"
        assert resp.headers["X-Job-Index-State"] == "initializing"

        catchup = await harness["api"].post(
            "/namespaces/default/cronjobs/inspection-daily/catchup",
            params={"at": "2026-11-20T01:30:00Z"},
        )
        assert catchup.status_code == 200, catchup.text
        body = catchup.json()
        assert body["status"] == "pending_verification"
        assert body["index_state"] == "initializing"
        assert "未就绪" in body["reason"]

        assert harness["store"].list_records() == []
    finally:
        gate.set()
        harness["kube"].list_jobs_page = original
        await _wait_state(harness, "default", "ready")


async def test_health_and_job_indexes_report_state(harness):
    await _latest(harness)
    health = (await harness["api"].get("/health")).json()
    assert health["status"] == "ok"
    assert health["job_indexes"][0]["namespace"] == "default"
    assert health["job_indexes"][0]["state"] == "ready"

    listing = (await harness["api"].get("/job-indexes")).json()
    assert {item["namespace"] for item in listing} == {"default"}
    assert listing[0]["has_snapshot"] is True
