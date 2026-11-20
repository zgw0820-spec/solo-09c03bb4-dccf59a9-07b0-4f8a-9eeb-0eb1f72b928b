"""接口级测试：已有 Job、补建、窗口/暂停限制、台账查回。"""
from datetime import datetime, timedelta, timezone

import pytest

UTC = timezone.utc

# 种子数据：inspection-daily 时区 Asia/Shanghai，cron 09:00
# 2026-11-20 09:00 上海 = 01:00Z；模拟端已存在该轮调度器 Job
DAILY = "inspection-daily"
NY_CJ = "inspection-ny"


async def test_health(harness):
    resp = await harness["api"].get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


async def test_latest_schedule_finds_existing_scheduler_job(harness):
    # 01:00Z 到点后 30 分钟
    at = datetime(2026, 11, 20, 1, 30, tzinfo=UTC)
    resp = await harness["api"].get(
        f"/namespaces/default/cronjobs/{DAILY}/latest-schedule",
        params={"at": at.isoformat()},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["scheduled_at_utc"].endswith("01:00:00Z")
    assert body["suspended"] is True
    assert body["within_catchup_window"] is True
    assert body["existing_job"] is not None
    assert body["existing_job_source"] == "scheduler"
    assert body["existing_job"]["metadata"]["annotations"][
        "batch.kubernetes.io/cronjob-uid"
    ] == body["cronjob_uid"]


async def test_catchup_returns_existing_job_instead_of_creating(harness):
    at = datetime(2026, 11, 20, 1, 30, tzinfo=UTC)
    resp = await harness["api"].post(
        f"/namespaces/default/cronjobs/{DAILY}/catchup",
        params={"at": at.isoformat()},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "returned_existing"
    assert body["job_source"] == "scheduler"
    # 不应产生补跑台账
    assert harness["store"].list_records() == []


async def test_catchup_creates_job_from_template_when_missing(harness):
    # inspection-ny 没有预置 Job；取一个明确在窗口内的时刻。
    # 先用查计划接口拿到计划时刻，再把演练时间设为该时刻 +30 分钟
    probe = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    resp = await harness["api"].get(
        f"/namespaces/default/cronjobs/{NY_CJ}/latest-schedule",
        params={"at": probe.isoformat()},
    )
    sched = datetime.fromisoformat(
        resp.json()["scheduled_at_utc"].replace("Z", "+00:00")
    )
    at = sched + timedelta(minutes=30)

    resp = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
        params={"at": at.isoformat()},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "created"
    assert body["job_source"] == "catchup"
    job_meta = body["job"]["metadata"]
    assert job_meta["labels"]["inspection.example.com/catchup"] == "true"
    assert job_meta["annotations"][
        "inspection.example.com/cronjob-uid"
    ] == body["cronjob_uid"]
    # 模板内容被沿用
    assert body["job"]["spec"]["template"]["spec"]["restartPolicy"] == "Never"

    # 再调一次：返回已建 Job，不重复创建
    resp2 = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
        params={"at": at.isoformat()},
    )
    body2 = resp2.json()
    assert body2["status"] == "returned_existing"
    assert body2["job"]["metadata"]["name"] == job_meta["name"]


async def test_catchup_rejected_when_not_suspended(harness):
    at = datetime(2026, 11, 20, 2, 0, tzinfo=UTC)
    resp = await harness["api"].post(
        "/namespaces/default/cronjobs/inspection-running/catchup",
        params={"at": at.isoformat()},
    )
    assert resp.status_code == 409
    assert "未暂停" in resp.json()["detail"]


async def test_catchup_rejected_after_two_hour_window(harness):
    # daily 计划 01:00Z；03:00:01 已超 2 小时
    at = datetime(2026, 11, 20, 3, 0, 1, tzinfo=UTC)
    resp = await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
        params={"at": at.isoformat()},
    )
    assert resp.status_code == 409
    assert "补跑期限" in resp.json()["detail"]


async def test_catchup_rejected_before_scheduled_moment(harness):
    # inspection-utc：cron 30 0 * * *（UTC），无预置 Job。
    # 2026-11-20 03:00Z 时最近一轮是 00:30Z，已过 2.5 小时 -> 超期拒绝
    at = datetime(2026, 11, 20, 3, 0, tzinfo=UTC)
    resp = await harness["api"].post(
        "/namespaces/default/cronjobs/inspection-utc/catchup",
        params={"at": at.isoformat()},
    )
    assert resp.status_code == 409
    assert "补跑期限" in resp.json()["detail"]
    # 台账不应留记录（窗口校验在落库之前）
    assert harness["store"].list_records() == []

    # 窗口内（00:30Z + 30 分钟）应允许创建
    ok = await harness["api"].post(
        "/namespaces/default/cronjobs/inspection-utc/catchup",
        params={"at": datetime(2026, 11, 20, 1, 0, tzinfo=UTC).isoformat()},
    )
    assert ok.status_code == 200
    assert ok.json()["status"] == "created"
    assert ok.json()["scheduled_at_utc"].endswith("00:30:00Z")


async def test_cronjob_without_timezone_rejected(harness):
    resp = await harness["api"].get(
        "/namespaces/default/cronjobs/legacy-no-tz/latest-schedule",
        params={"at": datetime(2026, 11, 20, 2, 0, tzinfo=UTC).isoformat()},
    )
    assert resp.status_code == 409
    assert "timeZone" in resp.json()["detail"]


async def test_non_daily_cronjob_rejected(harness):
    resp = await harness["api"].get(
        "/namespaces/default/cronjobs/inspection-weekly/latest-schedule",
        params={"at": datetime(2026, 11, 20, 2, 0, tzinfo=UTC).isoformat()},
    )
    assert resp.status_code == 409
    assert "每天固定时刻" in resp.json()["detail"]


async def test_missing_cronjob_404(harness):
    resp = await harness["api"].get(
        "/namespaces/default/cronjobs/nope/latest-schedule",
        params={"at": datetime(2026, 11, 20, 2, 0, tzinfo=UTC).isoformat()},
    )
    assert resp.status_code == 404


async def test_records_persist_and_list(harness):
    probe = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    sched_resp = await harness["api"].get(
        f"/namespaces/default/cronjobs/{NY_CJ}/latest-schedule",
        params={"at": probe.isoformat()},
    )
    sched = datetime.fromisoformat(
        sched_resp.json()["scheduled_at_utc"].replace("Z", "+00:00")
    )
    await harness["api"].post(
        f"/namespaces/default/cronjobs/{NY_CJ}/catchup",
        params={"at": (sched + timedelta(minutes=10)).isoformat()},
    )
    records = (await harness["api"].get("/records")).json()
    assert len(records) == 1
    assert records[0]["state"] == "created"
    assert records[0]["cronjob_name"] == NY_CJ
