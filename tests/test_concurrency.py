"""同轮并发：同一 CronJob 同一计划时刻并发补跑，只能建出一个 Job。"""
import asyncio
from datetime import datetime, timedelta, timezone

UTC = timezone.utc
NY_CJ = "inspection-ny"


async def test_concurrent_catchup_creates_single_job(harness):
    probe = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    sched_resp = await harness["api"].get(
        f"/namespaces/default/cronjobs/{NY_CJ}/latest-schedule",
        params={"at": probe.isoformat()},
    )
    sched = datetime.fromisoformat(
        sched_resp.json()["scheduled_at_utc"].replace("Z", "+00:00")
    )
    at = (sched + timedelta(minutes=20)).isoformat()

    async def call():
        return await harness["api"].post(
            f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
            params={"at": at},
        )

    results = await asyncio.gather(*(call() for _ in range(5)))
    statuses = [r.json()["status"] for r in results]
    # 恰好一个 created，其余都返回已存在的那个
    assert statuses.count("created") == 1, statuses
    assert statuses.count("returned_existing") == 4, statuses

    job_names = {r.json()["job"]["metadata"]["name"] for r in results}
    assert len(job_names) == 1

    # 模拟集群里同轮也只有一个 Job
    jobs = [
        j for j in harness["state"]["jobs"].values()
        if j["metadata"]["annotations"].get(
            "inspection.example.com/cronjob-uid"
        )
        == sched_resp.json()["cronjob_uid"]
    ]
    assert len(jobs) == 1

    # 台账唯一
    records = harness["store"].list_records()
    assert len(records) == 1
    assert records[0]["state"] == "created"
