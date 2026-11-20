"""调度模块单测：cron 解析、DST 回拨/春进、边界。"""
from datetime import datetime, timezone

import pytest

from app.schedule import (
    CronError,
    latest_scheduled_time,
    parse_cron,
    resolve_local_time,
)
from zoneinfo import ZoneInfo

UTC = timezone.utc
NY = "America/New_York"
BERLIN = "Europe/Berlin"


def test_dst_fall_back_takes_first_occurrence():
    tz = ZoneInfo(NY)
    # 2026-11-01 01:30 在纽约出现两次；第一次仍为 EDT（UTC-4）
    dt = resolve_local_time(datetime(2026, 11, 1, 1, 30), tz)
    assert dt is not None
    assert dt.utcoffset().total_seconds() == -4 * 3600
    assert dt.astimezone(UTC) == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)


def test_dst_spring_forward_nonexistent_returns_none():
    tz = ZoneInfo(NY)
    # 2026-03-08 02:30 本地不存在
    assert resolve_local_time(datetime(2026, 3, 8, 2, 30), tz) is None


def test_spring_forward_day_skips_gap_and_uses_previous_existing_time():
    # 春进当天扫描 02:30 时跳过缺口；当天没有合法 02:30 -> 前一天 EST 的 02:30
    sched = latest_scheduled_time(
        "30 2 * * *", NY, datetime(2026, 3, 8, 12, 0, tzinfo=UTC)
    )
    assert sched == datetime(2026, 3, 7, 7, 30, tzinfo=UTC)


def test_fall_back_schedule_uses_first_occurrence():
    sched = latest_scheduled_time(
        "30 1 * * *", NY, datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
    )
    assert sched == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)


def test_berlin_spring_and_fall():
    sched = latest_scheduled_time(
        "30 2 * * *", BERLIN, datetime(2026, 3, 29, 12, 0, tzinfo=UTC)
    )
    # 柏林 2026-03-29 02:30 不存在 -> 前一天 CET 02:30 = 01:30Z
    assert sched == datetime(2026, 3, 28, 1, 30, tzinfo=UTC)

    sched = latest_scheduled_time(
        "30 2 * * *", BERLIN, datetime(2026, 10, 25, 12, 0, tzinfo=UTC)
    )
    # 回拨日第一次 02:30 CEST(UTC+2) = 00:30Z
    assert sched == datetime(2026, 10, 25, 0, 30, tzinfo=UTC)


def test_shanghai_daily_fixed():
    sched = latest_scheduled_time(
        "0 9 * * *", "Asia/Shanghai", datetime(2026, 11, 20, 3, 0, tzinfo=UTC)
    )
    assert sched == datetime(2026, 11, 20, 1, 0, tzinfo=UTC)


def test_exact_scheduled_moment_is_included():
    sched = latest_scheduled_time(
        "0 9 * * *", "UTC", datetime(2026, 11, 20, 9, 0, tzinfo=UTC)
    )
    assert sched == datetime(2026, 11, 20, 9, 0, tzinfo=UTC)


def test_timezone_date_boundary():
    # UTC 11-20 16:30 = 东京 11-21 01:30；当天 09:00 未到，取东京 11-20 09:00 = 11-20 00:00Z
    sched = latest_scheduled_time(
        "0 9 * * *", "Asia/Tokyo", datetime(2026, 11, 20, 16, 30, tzinfo=UTC)
    )
    assert sched == datetime(2026, 11, 20, 0, 0, tzinfo=UTC)


def test_monthly_and_weekday_restrictions():
    sched = latest_scheduled_time(
        "0 0 1 * *", "UTC", datetime(2026, 11, 20, 0, 0, tzinfo=UTC)
    )
    assert sched == datetime(2026, 11, 1, 0, 0, tzinfo=UTC)

    sched = latest_scheduled_time(
        "0 9 * * MON", "UTC", datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
    )
    assert sched == datetime(2026, 11, 16, 9, 0, tzinfo=UTC)


def test_steps_ranges_macros():
    sched = latest_scheduled_time(
        "*/20 9-18 * * *", "UTC", datetime(2026, 11, 20, 10, 5, tzinfo=UTC)
    )
    assert sched == datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
    sched = latest_scheduled_time(
        "@daily", "UTC", datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
    )
    assert sched == datetime(2026, 11, 20, 0, 0, tzinfo=UTC)


def test_daily_fixed_time_classification():
    assert parse_cron("0 9 * * *").is_daily_fixed_time
    assert not parse_cron("0 9 * * MON").is_daily_fixed_time
    assert not parse_cron("0 0 1 * *").is_daily_fixed_time


@pytest.mark.parametrize("bad", ["* * * *", "99 9 * * *", "x y * * *", "@bogus"])
def test_invalid_cron_rejected(bad):
    with pytest.raises(CronError):
        parse_cron(bad)


def test_unknown_timezone_rejected():
    with pytest.raises(CronError):
        latest_scheduled_time(
            "0 9 * * *", "Mars/Olympus", datetime(2026, 11, 20, tzinfo=UTC)
        )


def test_dst_edt_time_exists_after_transition():
    # 春进次日 02:30 EDT 存在 = 06:30Z
    sched = latest_scheduled_time(
        "30 2 * * *", NY, datetime(2026, 3, 9, 12, 0, tzinfo=UTC)
    )
    assert sched == datetime(2026, 3, 9, 6, 30, tzinfo=UTC)
