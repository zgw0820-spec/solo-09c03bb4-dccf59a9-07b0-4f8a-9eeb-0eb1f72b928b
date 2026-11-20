"""Cron 解析与时区/夏令时感知的“最近一次计划时刻”计算。

仅依赖标准库（zoneinfo 由 tzdata 或系统时区库提供）。

业务约束：
- CronJob 必须声明 spec.timeZone（题目要求“设有时区、每天固定时刻”）。
- 夏令时回拨产生的重复时刻取第一次（fold=0）。
- 夏令时春进跳过的不存在时刻直接跳过，不做平移。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# cron 宏（与 Kubernetes 支持的 Vixie cron 宏一致）
_MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

# cron 星期表示 -> Python weekday()（Mon=0 ... Sun=6）
# cron 里 0 和 7 都表示周日
_DOW_TO_PY = {0: 6, 1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6}
_DOW_NAMES = {
    "SUN": 0, "MON": 1, "TUE": 2, "WED": 3,
    "THU": 4, "FRI": 5, "SAT": 6,
}
_MONTH_NAMES = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


class CronError(ValueError):
    """cron 表达式或时区配置错误。"""


@dataclass(frozen=True)
class CronSpec:
    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int] | None  # None 表示 *
    months: frozenset[int]
    days_of_week: frozenset[int] | None   # None 表示 *；元素为 Python weekday
    dom_restricted: bool
    dow_restricted: bool

    @property
    def is_daily_fixed_time(self) -> bool:
        """每天固定时刻：日、月、星期均为通配。"""
        return (
            not self.dom_restricted
            and not self.dow_restricted
            and self.months == frozenset(range(1, 13))
        )


def _parse_field(
    raw: str, low: int, high: int, names: dict[str, int] | None = None
) -> frozenset[int]:
    values: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        step = 1
        if "/" in part:
            base, step_raw = part.split("/", 1)
            if not step_raw.isdigit() or int(step_raw) <= 0:
                raise CronError(f"非法步长: {part}")
            step = int(step_raw)
        else:
            base = part
        if base == "*" or base == "?":
            start, end = low, high
        elif base == "":
            # "/5" 这种写法等价于 "*/5"
            start, end = low, high
        elif "-" in base:
            lo_raw, hi_raw = base.split("-", 1)
            start = _resolve_name(lo_raw, names)
            end = _resolve_name(hi_raw, names)
        else:
            start = _resolve_name(base, names)
            end = start
        if not (low <= start <= high and low <= end <= high and start <= end):
            raise CronError(f"字段超出范围 [{low},{high}]: {part}")
        values.update(range(start, end + 1, step))
    if not values:
        raise CronError(f"字段未匹配任何值: {raw}")
    return frozenset(values)


def _resolve_name(token: str, names: dict[str, int] | None) -> int:
    if token.isdigit():
        return int(token)
    if names and token.upper() in names:
        return names[token.upper()]
    raise CronError(f"无法识别的取值: {token}")


def parse_cron(expression: str) -> CronSpec:
    """解析标准 5 段 cron（支持宏、列表、范围、步长、名称）。"""
    expr = expression.strip()
    if expr.startswith("@"):
        if expr not in _MACROS:
            raise CronError(f"不支持的宏: {expr}")
        expr = _MACROS[expr]
    fields = expr.split()
    if len(fields) != 5:
        raise CronError(f"cron 必须是 5 段，收到 {len(fields)} 段: {expression}")
    minute_f, hour_f, dom_f, month_f, dow_f = fields

    minutes = _parse_field(minute_f, 0, 59)
    hours = _parse_field(hour_f, 0, 23)
    dom_restricted = dom_f not in ("*", "?", "*/1")
    dow_restricted = dow_f not in ("*", "?", "*/1")
    dom = None if not dom_restricted else _parse_field(dom_f, 1, 31)
    month = _parse_field(month_f, 1, 12, _MONTH_NAMES)
    dow_cron = (
        None
        if not dow_restricted
        else _parse_field(dow_f, 0, 7, _DOW_NAMES)
    )
    dow = (
        None
        if dow_cron is None
        else frozenset(_DOW_TO_PY[d] for d in dow_cron)
    )
    return CronSpec(
        minutes=minutes,
        hours=hours,
        days_of_month=dom,
        months=month,
        days_of_week=dow,
        dom_restricted=dom_restricted,
        dow_restricted=dow_restricted,
    )


def _day_matches(spec: CronSpec, naive_date_day: datetime) -> bool:
    if naive_date_day.month not in spec.months:
        return False
    if not spec.dom_restricted and not spec.dow_restricted:
        return True
    # Vixie cron 语义：日和星期都受限 -> OR；只有一个受限 -> 只看受限的那个
    if spec.dom_restricted and spec.dow_restricted:
        dom_ok = naive_date_day.day in spec.days_of_month  # type: ignore[operator]
        dow_ok = naive_date_day.weekday() in spec.days_of_week  # type: ignore[operator]
        return dom_ok or dow_ok
    if spec.dom_restricted:
        return naive_date_day.day in spec.days_of_month  # type: ignore[operator]
    return naive_date_day.weekday() in spec.days_of_week  # type: ignore[operator]


def resolve_local_time(naive: datetime, tz: ZoneInfo) -> datetime | None:
    """把本地墙上时间附着到时区。

    返回 None 表示该墙上时间在春进中被跳过（不存在）。
    对回拨产生的歧义时间取第一次（fold=0）。
    """
    candidate = naive.replace(tzinfo=tz, fold=0)
    # 回查：fold=0 的 UTC 偏移下，本地墙上时间是否还是它自己
    roundtrip = candidate.astimezone(timezone.utc).astimezone(tz)
    if roundtrip.replace(tzinfo=None) != naive:
        return None  # 不存在的时刻（春进缺口）
    return candidate


def latest_scheduled_time(
    cron_expression: str,
    tz_name: str,
    now_utc: datetime,
    max_days: int = 400,
) -> datetime:
    """求 now_utc 之前（含）最近一次应到点的计划时刻，返回 UTC datetime。

    每天固定时刻场景每天至多一次；通用 cron 按分钟自后向前扫描。
    """
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError as exc:
        raise CronError(f"未知时区: {tz_name}") from exc

    spec = parse_cron(cron_expression)

    # 以“本地日历日”为粒度自后向前扫描，避免按分钟扫描 400 天的开销。
    # 起点取本地今天（含今天尚未到点的时刻会被 utc_dt <= now 过滤），
    # 再向前多推一天，兜住 UTC 与本地日期不一致的边界情形。
    local_now = now_utc.astimezone(tz)
    local_today = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_cursor = local_today + timedelta(days=1)
    deadline = local_today - timedelta(days=max_days)

    while day_cursor >= deadline:
        if _day_matches(spec, day_cursor):
            for hour in sorted(spec.hours, reverse=True):
                for minute in sorted(spec.minutes, reverse=True):
                    # 只取日历字段，构造“本地墙上时间”（naive）
                    naive = day_cursor.replace(
                        hour=hour, minute=minute,
                        second=0, microsecond=0, tzinfo=None,
                    )
                    local_dt = resolve_local_time(naive, tz)
                    if local_dt is None:
                        continue  # 春进跳过的不存在时刻
                    utc_dt = local_dt.astimezone(timezone.utc)
                    if utc_dt <= now_utc:
                        return utc_dt
        day_cursor -= timedelta(days=1)

    raise CronError(
        f"在最近 {max_days} 天内找不到计划时刻（cron={cron_expression}, tz={tz_name}）"
    )
