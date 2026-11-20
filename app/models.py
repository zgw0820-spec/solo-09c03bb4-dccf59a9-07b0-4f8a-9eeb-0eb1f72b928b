"""Pydantic 响应模型（Swagger 文档用）。"""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class JobMatchSource(str, Enum):
    scheduler = "scheduler"   # 原调度器创建的 Job
    catchup = "catchup"       # 本服务补建的 Job


class IndexState(str, Enum):
    initializing = "initializing"  # 首次 LIST 未完成，索引不可用
    ready = "ready"                # WATCH 已建立，索引实时
    stale = "stale"                # 有上一次完整清单，但 WATCH 断开/410


class IndexStatus(BaseModel):
    namespace: str
    state: IndexState
    ready: bool = Field(..., description="索引实时可用")
    stale: bool = Field(..., description="有完整清单但已过期（断线/410 期间）")
    has_snapshot: bool = Field(..., description="是否至少完成过一次 LIST")
    job_count: int
    resource_version: str | None = None
    synced_at: datetime | None = Field(None, description="最近一次完整 LIST 同步时间")
    stale_since: datetime | None = Field(None, description="本轮索引变为过期的时间")
    last_error: str | None = None


class ScheduleResponse(BaseModel):
    namespace: str
    cronjob: str
    cronjob_uid: str
    time_zone: str
    schedule: str
    suspended: bool
    scheduled_at_utc: datetime = Field(..., description="最近一次应到点的计划时刻（UTC）")
    is_daily_fixed_time: bool
    existing_job: dict | None = Field(
        None, description="已存在的匹配 Job（原调度器或补跑 Job），无则 null"
    )
    existing_job_source: JobMatchSource | None = None
    within_catchup_window: bool = Field(..., description="距到点是否在 2 小时补跑期限内（UTC）")
    index_state: IndexState = Field(..., description="Job 索引状态")
    index_stale: bool = Field(
        ..., description="索引是否过期：此时 existing_job=null 不能当作“无 Job”"
    )
    index_status: IndexStatus = Field(..., description="命名空间 Job 索引明细")


class CatchupStatus(str, Enum):
    returned_existing = "returned_existing"
    created = "created"
    pending_verification = "pending_verification"
    rejected = "rejected"


class CatchupResponse(BaseModel):
    status: CatchupStatus
    namespace: str
    cronjob: str
    cronjob_uid: str
    scheduled_at_utc: datetime
    job: dict | None = None
    job_source: JobMatchSource | None = None
    reason: str | None = Field(None, description="拒绝或待核实原因")
    record_id: int | None = Field(None, description="本地补跑台账记录 ID")
    index_state: IndexState | None = Field(
        None, description="本次判定时 Job 索引状态"
    )
    index_stale: bool | None = Field(
        None, description="索引过期：pending_verification 不代表“无 Job”"
    )


class RecordResponse(BaseModel):
    id: int
    namespace: str
    cronjob_name: str
    cronjob_uid: str
    scheduled_at: datetime
    job_name: str | None
    job_uid: str | None
    source: str
    state: str
    detail: str | None
    created_at: datetime
    updated_at: datetime


class HealthResponse(BaseModel):
    status: str
    kubernetes_api: str
    job_indexes: list[IndexStatus] = Field(
        default_factory=list, description="各命名空间 Job 索引状态（按命名空间复用）"
    )
