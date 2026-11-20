"""创建回包超时：先回查集群；查不清报待核实；重启后可继续查回。"""
from datetime import datetime, timedelta, timezone

import pytest

UTC = timezone.utc
NY_CJ = "inspection-ny"


async def _windowed_at(harness, cj: str = NY_CJ) -> tuple[str, str]:
    probe = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    sched_resp = await harness["api"].get(
        f"/namespaces/default/cronjobs/{cj}/latest-schedule",
        params={"at": probe.isoformat()},
    )
    body = sched_resp.json()
    sched = datetime.fromisoformat(body["scheduled_at_utc"].replace("Z", "+00:00"))
    return (sched + timedelta(minutes=20)).isoformat(), body["cronjob_uid"]


def _patch_kube_verify_failure(kube):
    """在实例上打补丁：创建超时 + 回查全失败。返回恢复函数。"""
    from app.kube import KubeError

    async def boom_create(namespace, manifest):
        raise KubeError("Kubernetes API 请求超时: 模拟")

    async def boom_get_job(namespace, name):
        raise KubeError("模拟回查网络故障")

    async def boom_list_jobs(namespace):
        raise KubeError("模拟回查网络故障")

    kube.create_job = boom_create
    kube.get_job = boom_get_job
    kube.list_jobs = boom_list_jobs

    def restore():
        del kube.create_job
        del kube.get_job
        del kube.list_jobs

    return restore


@pytest.mark.mock_fault("create-timeout")
@pytest.mark.mock_slow(1.5)
async def test_create_timeout_then_verify_finds_job(harness):
    at, uid = await _windowed_at(harness)
    resp = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
        params={"at": at},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # 客户端先超时，模拟端对象已落库 -> 回查命中
    assert body["status"] == "created"
    assert "超时" in body["reason"]
    assert body["job"]["metadata"]["annotations"][
        "inspection.example.com/cronjob-uid"
    ] == uid

    records = harness["store"].list_records()
    assert records[0]["state"] == "created"


@pytest.mark.mock_fault("create-timeout-lose")
@pytest.mark.mock_slow(1.5)
async def test_create_timeout_lose_connection_still_verifies(harness):
    at, _ = await _windowed_at(harness)
    resp = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
        params={"at": at},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # lose 模式同样先落库；按名字回查应能查到 -> created
    assert body["status"] == "created"
    assert body["job_source"] == "catchup"


async def test_pending_when_cluster_cannot_confirm(harness):
    """模拟“超时且回查全部失败”：应报待核实。"""
    at, _ = await _windowed_at(harness)
    restore = _patch_kube_verify_failure(harness["kube"])
    try:
        resp = await harness["api"].post(
            f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
            params={"at": at},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "pending_verification"
        assert "未能确认" in body["reason"] or "待核实" in body["reason"]

        records = harness["store"].list_records()
        assert records[0]["state"] == "pending_verification"
    finally:
        restore()


async def test_reverify_after_recovery_confirms_job(harness):
    """台账处于 pending_verification，集群里其实有 Job -> reverify 确认。"""
    at, uid = await _windowed_at(harness)
    restore = _patch_kube_verify_failure(harness["kube"])

    resp = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
        params={"at": at},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending_verification"
    restore()

    # 模拟运维确认：按台账里的名字在集群补写 Job（现实中可能是控制器其实建成功了）
    record = harness["store"].list_records()[0]
    from app.kube import build_catchup_job

    cj = harness["state"]["cronjobs"][("default", NY_CJ)]
    sched = datetime.fromisoformat(record["scheduled_at"].replace("Z", "+00:00"))
    manifest = build_catchup_job(cj, sched, record["job_name"])
    manifest["metadata"]["uid"] = "uid-recovered"
    manifest["metadata"]["creationTimestamp"] = "2026-11-20T00:00:00Z"
    harness["state"]["jobs"][("default", record["job_name"])] = manifest

    resp2 = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/reverify",
        params={"at": at},
    )
    assert resp2.status_code == 200
    body2 = resp2.json()
    assert body2["status"] == "created"
    assert body2["job"]["metadata"]["uid"] == "uid-recovered"
    assert harness["store"].list_records()[0]["state"] == "created"


async def test_restart_recovers_creating_to_pending(harness):
    """重启时 creating 记录应转 pending_verification，且仍可查回。"""
    at, _ = await _windowed_at(harness)
    # 正常建一个 Job
    resp = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
        params={"at": at},
    )
    assert resp.json()["status"] == "created"
    job_name = resp.json()["job"]["metadata"]["name"]

    # 手工把台账打回 creating，模拟进程在创建中途崩溃
    record = harness["store"].list_records()[0]
    with harness["store"]._lock:
        harness["store"]._conn.execute(
            "UPDATE catchup_records SET state='creating' WHERE id=?",
            (record["id"],),
        )
        harness["store"]._conn.commit()

    recovered = harness["store"].recover_creating()
    assert len(recovered) == 1
    assert recovered[0]["state"] == "pending_verification"

    # reverify 能从集群查回
    resp2 = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/reverify",
        params={"at": at},
    )
    assert resp2.json()["status"] == "created"
    assert resp2.json()["job"]["metadata"]["name"] == job_name
