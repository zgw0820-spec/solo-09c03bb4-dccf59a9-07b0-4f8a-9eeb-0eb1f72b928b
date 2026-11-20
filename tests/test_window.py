"""补跑窗口边界与 Job 命名等服务级规则。"""
from datetime import datetime, timedelta, timezone

import pytest

UTC = timezone.utc
UTC_CJ = "inspection-utc"  # cron 30 0 * * * UTC，无预置 Job


async def test_window_boundary_exactly_two_hours_allowed(harness):
    # 计划 00:30Z；02:30:00Z 恰好 2 小时整 -> 允许（<= 窗口）
    at = datetime(2026, 11, 20, 2, 30, 0, tzinfo=UTC)
    resp = await harness["api"].post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={"at": at.isoformat()},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "created"


async def test_window_one_second_past_rejected(harness):
    # 02:30:01Z 超出 2 小时 1 秒 -> 拒绝
    at = datetime(2026, 11, 20, 2, 30, 1, tzinfo=UTC)
    resp = await harness["api"].post(
        f"/namespaces/default/cronjobs/{UTC_CJ}/catchup",
        params={"at": at.isoformat()},
    )
    assert resp.status_code == 409
    assert "补跑期限" in resp.json()["detail"]
    assert harness["store"].list_records() == []


async def test_inspect_window_flag_reflects_boundary(harness):
    # 恰好 2 小时：within=True
    at = datetime(2026, 11, 20, 2, 30, 0, tzinfo=UTC)
    resp = await harness["api"].get(
        f"/namespaces/default/cronjobs/{UTC_CJ}/latest-schedule",
        params={"at": at.isoformat()},
    )
    assert resp.json()["within_catchup_window"] is True

    # 超出：within=False，但查询本身仍 200
    at2 = datetime(2026, 11, 20, 3, 0, 0, tzinfo=UTC)
    resp2 = await harness["api"].get(
        f"/namespaces/default/cronjobs/{UTC_CJ}/latest-schedule",
        params={"at": at2.isoformat()},
    )
    assert resp2.status_code == 200
    assert resp2.json()["within_catchup_window"] is False


def test_job_name_truncation():
    from app.service import CatchupService

    long_name = "x" * 80
    name = CatchupService._compose_job_name(
        long_name, datetime(2026, 11, 20, 1, 0, tzinfo=UTC)
    )
    assert len(name) <= 63
    assert name.endswith("catchup-202611200100")


def test_job_name_normal_length():
    from app.service import CatchupService

    name = CatchupService._compose_job_name(
        "inspection", datetime(2026, 11, 20, 1, 0, tzinfo=UTC)
    )
    assert name == "inspection-catchup-202611200100"
