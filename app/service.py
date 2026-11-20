"""补跑业务编排：查计划、按索引匹配 Job、按模板补建、超时回查。

Job 匹配一律走按命名空间复用的实时索引（LIST + WATCH），
查询路径不再扫描命名空间下的全部 Job。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .jobindex import (
    STATE_INITIALIZING,
    IndexView,
    JobIndexManager,
)
from .kube import (
    KubeClient,
    KubeError,
    KubeNotFound,
    build_catchup_job,
    cronjob_summary,
    is_catchup_job,
    scheduled_at_iso,
)
from .schedule import CronError, latest_scheduled_time, parse_cron
from .store import (
    STATE_CREATED,
    STATE_FAILED,
    STATE_PENDING,
    CatchupStore,
)

CATCHUP_WINDOW = timedelta(hours=2)


class ServiceError(Exception):
    def __init__(
        self,
        message: str,
        status_code: int = 409,
        index_status: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.index_status = index_status


class CatchupService:
    def __init__(
        self,
        kube: KubeClient,
        store: CatchupStore,
        indexes: JobIndexManager | None = None,
    ):
        self.kube = kube
        self.store = store
        # 允许旧调用方不传索引：退化为按需自建（仍按命名空间复用）
        self.indexes = indexes or JobIndexManager(kube)

    # ---------- 查计划 ----------

    async def inspect(
        self, namespace: str, name: str, now_utc: datetime
    ) -> dict[str, Any]:
        cronjob = await self._load_valid_cronjob(namespace, name)
        info = cronjob_summary(cronjob)
        scheduled = latest_scheduled_time(
            info["schedule"], info["time_zone"], now_utc
        )
        view = await self._index_view(namespace)
        if not view.ready and not view.stale:
            # initializing：首次索引未就绪（是否已拿到清单都一样不可用于判定）
            hint = (
                "已取得清单但 WATCH 尚未建立"
                if view.has_snapshot
                else "首次 LIST 尚未完成"
            )
            raise ServiceError(
                f"Job 索引尚未就绪：命名空间 {namespace} 的{hint}，"
                "暂无法确认是否已有 Job",
                status_code=503,
                index_status=view.status.as_dict(),
            )
        job = view.lookup(info["uid"], scheduled_at_iso(scheduled))
        within = timedelta(0) <= (now_utc - scheduled) <= CATCHUP_WINDOW
        return {
            "namespace": namespace,
            "cronjob": name,
            "cronjob_uid": info["uid"],
            "time_zone": info["time_zone"],
            "schedule": info["schedule"],
            "suspended": info["suspended"],
            "scheduled_at_utc": scheduled,
            "is_daily_fixed_time": parse_cron(info["schedule"]).is_daily_fixed_time,
            "existing_job": self._job_ref(job) if job else None,
            "existing_job_source": self._source_of(job) if job else None,
            "within_catchup_window": within,
            "index_state": view.state,
            "index_stale": view.stale,
            "index_status": view.status.as_dict(),
        }

    # ---------- 补跑 ----------

    async def catchup(
        self, namespace: str, name: str, now_utc: datetime
    ) -> dict[str, Any]:
        cronjob = await self._load_valid_cronjob(namespace, name)
        info = cronjob_summary(cronjob)
        scheduled = latest_scheduled_time(
            info["schedule"], info["time_zone"], now_utc
        )
        scheduled_iso = scheduled_at_iso(scheduled)
        lock_key = f"{namespace}/{name}/{scheduled_iso}"

        async with self.store.async_lock_for(lock_key):
            # 1. 只在索引里按 (CronJob UID, 计划时刻) 命中，不扫描 Job 列表。
            view = await self._index_view(namespace)
            existing = (
                view.lookup(info["uid"], scheduled_iso) if view.has_snapshot else None
            )
            if existing:
                return self._existing_response(
                    info, scheduled, existing, self._source_of(existing), view=view
                )

            # 2. 补跑前置条件：仍暂停 + 2 小时窗口内（UTC）
            self._assert_catchup_allowed(info, scheduled, now_utc)

            # 3. 索引不实时（未就绪/断线/410 期间）：空结果不等于“没有 Job”，
            #    不得创建，返回待核实。
            if not view.ready:
                return self._index_unavailable_response(info, scheduled, view)

            # 4. 台账查重：重启后或窗口内重复调用都能查回
            record = self.store.get_by_round(info["uid"], scheduled_iso)
            if record and record["state"] == STATE_CREATED:
                job = await self._safe_get_job(namespace, record["job_name"])
                if job is not None:
                    return self._existing_response(
                        info, scheduled, job, self._source_of(job),
                        record_id=record["id"], view=view,
                    )
                # 台账说建过但集群查不到：进入待核实
                self.store.update_state(record["id"], STATE_PENDING,
                                        detail="台账已记录但集群回查不到 Job")
                return self._pending_response(info, scheduled, record,
                                              "记录显示已创建，但集群中查不到该 Job",
                                              view=view)
            if record and record["state"] == STATE_PENDING:
                return await self._reverify(namespace, info, scheduled, record, view)

            # 5. 构造 Job 并落台账（同轮唯一约束兜底）
            job_name = self._compose_job_name(name, scheduled)
            manifest = build_catchup_job(cronjob, scheduled, job_name)
            try:
                record = self.store.insert_creating(
                    namespace, name, info["uid"], scheduled_iso, job_name
                )
            except Exception:
                # 并发下唯一约束冲突 -> 重新走查回逻辑
                record = self.store.get_by_round(info["uid"], scheduled_iso)
                if record:
                    return await self._reverify(
                        namespace, info, scheduled, record, view
                    )
                raise

            # 6. 调 Kubernetes 创建；回包超时先回查集群
            return await self._create_with_verify(
                namespace, info, scheduled, manifest, record, view
            )

    # ---------- 待核实记录的手动/再次回查 ----------

    async def reverify(
        self, namespace: str, name: str, now_utc: datetime
    ) -> dict[str, Any]:
        cronjob = await self._load_valid_cronjob(namespace, name)
        info = cronjob_summary(cronjob)
        scheduled = latest_scheduled_time(
            info["schedule"], info["time_zone"], now_utc
        )
        record = self.store.get_by_round(info["uid"], scheduled_at_iso(scheduled))
        if not record:
            raise ServiceError("该轮没有补跑台账记录，无需核实", status_code=404)
        view = await self._index_view(namespace)
        return await self._reverify(namespace, info, scheduled, record, view)

    # ---------- 内部方法 ----------

    async def _index_view(self, namespace: str) -> IndexView:
        return await self.indexes.get(namespace).ensure_started()

    async def _create_with_verify(
        self, namespace, info, scheduled, manifest, record, view: IndexView
    ) -> dict[str, Any]:
        try:
            job = await self.kube.create_job(namespace, manifest)
        except KubeError as exc:
            if "超时" not in str(exc) and "timeout" not in str(exc).lower():
                self.store.update_state(record["id"], STATE_FAILED, detail=str(exc))
                raise ServiceError(f"创建 Job 失败: {exc}", status_code=502) from exc
            # 创建回包超时：先回查集群
            return await self._verify_after_timeout(
                namespace, info, scheduled, record, str(exc), view
            )

        job_ref = self._job_ref(job)
        self.store.update_state(
            record["id"], STATE_CREATED,
            job_name=job_ref["metadata"]["name"],
            job_uid=job_ref["metadata"].get("uid"),
            detail="创建成功",
        )
        return {
            "status": "created",
            **self._base(info, scheduled, record["id"]),
            "job": job_ref,
            "job_source": "catchup",
            "reason": None,
            **self._index_fields(view),
        }

    async def _verify_after_timeout(
        self, namespace, info, scheduled, record, timeout_detail, view: IndexView
    ) -> dict[str, Any]:
        job = await self._safe_get_job(namespace, record["job_name"])
        if job is not None:
            ref = self._job_ref(job)
            self.store.update_state(
                record["id"], STATE_CREATED,
                job_name=ref["metadata"]["name"],
                job_uid=ref["metadata"].get("uid"),
                detail=f"创建回包超时，回查确认已存在: {timeout_detail}",
            )
            return {
                "status": "created",
                **self._base(info, scheduled, record["id"]),
                "job": ref,
                "job_source": "catchup",
                "reason": "创建请求回包超时，回查集群确认 Job 已存在",
                **self._index_fields(view),
            }
        # 再按 (UID, 计划时刻) 在索引里查一遍，覆盖“名字不同但已建出”的小概率情况；
        # 索引不实时时不能把“查不到”当成“没建出来”。
        matched = (
            view.lookup(info["uid"], scheduled_at_iso(scheduled))
            if view.ready
            else None
        )
        if matched is not None:
            ref = self._job_ref(matched)
            self.store.update_state(
                record["id"], STATE_CREATED,
                job_name=ref["metadata"]["name"],
                job_uid=ref["metadata"].get("uid"),
                detail="超时后按 UID+计划时刻在索引中命中",
            )
            return {
                "status": "created",
                **self._base(info, scheduled, record["id"]),
                "job": ref,
                "job_source": self._source_of(matched),
                "reason": "创建请求回包超时，按 UID+计划时刻命中已有 Job",
                **self._index_fields(view),
            }
        self.store.update_state(record["id"], STATE_PENDING, detail=timeout_detail)
        return self._pending_response(
            info, scheduled, record,
            f"创建请求回包超时且回查集群未能确认: {timeout_detail}",
            view=view,
        )

    async def _reverify(
        self, namespace, info, scheduled, record, view: IndexView
    ) -> dict[str, Any]:
        job = await self._safe_get_job(namespace, record["job_name"])
        if job is None:
            matched = (
                view.lookup(info["uid"], scheduled_at_iso(scheduled))
                if view.ready
                else None
            )
            if matched is not None:
                job = matched
            elif not view.ready:
                # 索引不实时：无法排除“Job 已存在但索引未同步”
                self.store.update_state(
                    record["id"], STATE_PENDING,
                    detail="Job 索引不可用（未就绪/过期），仍需人工核实",
                )
                return self._pending_response(
                    info, scheduled, record,
                    "Job 索引未就绪或已过期，无法确认 Job 是否已创建，仍需人工核实",
                    view=view,
                )
        if job is None:
            self.store.update_state(record["id"], STATE_PENDING,
                                    detail="再次回查仍未找到 Job")
            return self._pending_response(
                info, scheduled, record, "回查集群未找到对应 Job，需人工核实",
                view=view,
            )
        ref = self._job_ref(job)
        self.store.update_state(
            record["id"], STATE_CREATED,
            job_name=ref["metadata"]["name"],
            job_uid=ref["metadata"].get("uid"),
            detail="回查确认 Job 存在",
        )
        return {
            "status": "created",
            **self._base(info, scheduled, record["id"]),
            "job": ref,
            "job_source": self._source_of(job),
            "reason": "回查确认 Job 已存在",
            **self._index_fields(view),
        }

    async def _safe_get_job(self, namespace: str, name: str) -> dict | None:
        if not name:
            return None
        try:
            return await self.kube.get_job(namespace, name)
        except KubeNotFound:
            return None
        except KubeError:
            return None

    def _assert_catchup_allowed(self, info, scheduled: datetime, now_utc: datetime) -> None:
        if not info["suspended"]:
            raise ServiceError(
                f"CronJob {info['name']} 当前未暂停（suspend=false），不应由本服务补建 Job"
            )
        delta = now_utc - scheduled  # 计划时刻由 latest_scheduled_time 保证 <= now
        if delta > CATCHUP_WINDOW:
            raise ServiceError(
                f"已超过补跑期限：计划时刻 {scheduled_at_iso(scheduled)}，"
                f"补跑窗口为到点后 2 小时（UTC）"
            )

    async def _load_valid_cronjob(self, namespace: str, name: str) -> dict[str, Any]:
        try:
            cronjob = await self.kube.get_cronjob(namespace, name)
        except KubeNotFound as exc:
            raise ServiceError(f"CronJob {namespace}/{name} 不存在",
                               status_code=404) from exc
        except KubeError as exc:
            raise ServiceError(f"读取 CronJob 失败: {exc}", status_code=502) from exc

        info = cronjob_summary(cronjob)
        if not info["time_zone"]:
            raise ServiceError(
                "该 CronJob 未设置 spec.timeZone，本服务只支持显式时区的 CronJob"
            )
        try:
            spec = parse_cron(info["schedule"])
        except CronError as exc:
            raise ServiceError(f"CronJob schedule 非法: {exc}") from exc
        if not spec.is_daily_fixed_time:
            raise ServiceError(
                f"本服务只处理每天固定时刻的 CronJob，当前 schedule={info['schedule']} "
                "含星期/日期/月份限制"
            )
        return cronjob

    @staticmethod
    def _compose_job_name(cronjob_name: str, scheduled: datetime) -> str:
        stamp = scheduled.astimezone(timezone.utc).strftime("%Y%m%d%H%M")
        suffix = f"-catchup-{stamp}"
        # Job 名最长 63：超长时截断 CronJob 名前缀，完整保留后缀
        max_prefix = 63 - len(suffix)
        prefix = cronjob_name[:max_prefix].rstrip("-")
        return f"{prefix}{suffix}"

    @staticmethod
    def _job_ref(job: dict) -> dict:
        """裁剪返回体，保留关键字段，避免 Swagger 输出过大。"""
        meta = job.get("metadata", {})
        ref = {
            "apiVersion": job.get("apiVersion", "batch/v1"),
            "kind": "Job",
            "metadata": {
                "name": meta.get("name"),
                "namespace": meta.get("namespace"),
                "uid": meta.get("uid"),
                "labels": meta.get("labels", {}),
                "annotations": meta.get("annotations", {}),
                "creationTimestamp": meta.get("creationTimestamp"),
            },
        }
        if "spec" in job:
            # 保留 Pod 模板片段，便于核对补跑 Job 是否沿用原 jobTemplate
            ref["spec"] = {"template": job["spec"].get("template", {})}
        return ref

    @staticmethod
    def _source_of(job: dict) -> str:
        return "catchup" if is_catchup_job(job) else "scheduler"

    @staticmethod
    def _index_fields(view: IndexView) -> dict[str, Any]:
        return {
            "index_state": view.state,
            "index_stale": view.stale,
        }

    @staticmethod
    def _base(info, scheduled, record_id: int | None) -> dict:
        return {
            "namespace": info["namespace"],
            "cronjob": info["name"],
            "cronjob_uid": info["uid"],
            "scheduled_at_utc": scheduled,
            "record_id": record_id,
        }

    def _existing_response(
        self, info, scheduled, job, source, record_id=None, view: IndexView | None = None
    ) -> dict:
        return {
            "status": "returned_existing",
            **self._base(info, scheduled, record_id),
            "job": self._job_ref(job),
            "job_source": source or self._source_of(job),
            **(self._index_fields(view) if view is not None else {}),
            "reason": "已存在匹配 Job（按 CronJob UID + 计划时刻匹配）"
            + (
                "；注意：Job 索引已过期，结果基于上一次完整索引，可能不是最新状态"
                if view is not None and view.stale
                else ""
            ),
        }

    def _pending_response(
        self, info, scheduled, record, reason, view: IndexView | None = None
    ) -> dict:
        return {
            "status": "pending_verification",
            **self._base(info, scheduled, record["id"] if record else None),
            "job": None,
            "job_source": None,
            "reason": reason,
            **(self._index_fields(view) if view is not None else {}),
        }

    def _index_unavailable_response(
        self, info, scheduled, view: IndexView
    ) -> dict:
        if view.state == STATE_INITIALIZING:
            if view.has_snapshot:
                reason = (
                    "Job 索引尚未就绪（已取得清单但 WATCH 未建立），"
                    "无法确认是否已有 Job；不创建、不判定为无 Job，请稍后重试或人工核实"
                )
            else:
                reason = (
                    "Job 索引尚未就绪（首次 LIST 未完成），无法确认是否已有 Job；"
                    "不创建、不判定为无 Job，请稍后重试或人工核实"
                )
        else:
            since = view.status.stale_since
            reason = (
                "Job 索引已过期（WATCH 断开或 resourceVersion 过期），"
                f"上次完整同步于 {view.status.synced_at.isoformat() if view.status.synced_at else '未知'}"
                + (f"，过期起始 {since.isoformat()}" if since else "")
                + "；空结果不能当作无 Job，需人工核实"
            )
        return {
            "status": "pending_verification",
            **self._base(info, scheduled, None),
            "job": None,
            "job_source": None,
            "reason": reason,
            **self._index_fields(view),
        }
