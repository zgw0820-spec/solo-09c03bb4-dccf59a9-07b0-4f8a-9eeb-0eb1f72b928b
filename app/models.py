"""Pydantic 响应模型（Swagger 文档用）。"""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class JobMatchSource(str, Enum):
    scheduler = "scheduler"   # 原调度器创建的 Job
    catchup = "catchup"       # 本服务补建的 Job


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
