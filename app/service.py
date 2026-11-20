"""补跑业务编排：查计划、匹配 Job、按模板补建、超时回查。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .kube import (
    KubeClient,
    KubeError,
    KubeNotFound,
    build_catchup_job,
    cronjob_summary,
    find_matching_job,
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
    def __init__(self, message: str, status_code: int = 409):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class CatchupService:
    def __init__(self, kube: KubeClient, store: CatchupStore):
        self.kube = kube
        self.store = store

    # ---------- 查计划 ----------

    async def inspect(
        self, namespace: str, name: str, now_utc: datetime
    ) -> dict[str, Any]:
        cronjob = await self._load_valid_cronjob(namespace, name)
        info = cronjob_summary(cronjob)
        scheduled = latest_scheduled_time(
            info["schedule"], info["time_zone"], now_utc
        )
        jobs = await self.kube.list_jobs(namespace)
        job, source = find_matching_job(jobs, info["uid"], scheduled)
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
            "existing_job_source": source,
            "within_catchup_window": within,
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
            # 1. 先按 (UID, 计划时刻) 匹配已有 Job（原调度器创建的也算）。
            #    查询失败不阻断补跑主流程，按“没有匹配 Job”处理，
            #    后续创建若也异常会走超时回查/待核实。
            jobs = await self._safe_list_jobs(namespace)
            existing, source = find_matching_job(jobs, info["uid"], scheduled)
            if existing:
                return self._existing_response(info, scheduled, existing, source)

            # 2. 补跑前置条件：仍暂停 + 2 小时窗口内（UTC）
            self._assert_catchup_allowed(info, scheduled, now_utc)

            # 3. 台账查重：重启后或窗口内重复调用都能查回
            record = self.store.get_by_round(info["uid"], scheduled_iso)
            if record and record["state"] == STATE_CREATED:
                job = await self._safe_get_job(namespace, record["job_name"])
                if job is not None:
                    return self._existing_response(
                        info, scheduled, job,
                        "catchup" if is_catchup_job(job) else "scheduler",
                        record_id=record["id"],
                    )
                # 台账说建过但集群查不到：进入待核实
                self.store.update_state(record["id"], STATE_PENDING,
                                        detail="台账已记录但集群回查不到 Job")
                return self._pending_response(info, scheduled, record,
                                              "记录显示已创建，但集群中查不到该 Job")
            if record and record["state"] == STATE_PENDING:
                return await self._reverify(namespace, info, scheduled, record)

            # 4. 构造 Job 并落台账（同轮唯一约束兜底）
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
                    return await self._reverify(namespace, info, scheduled, record)
                raise

            # 5. 调 Kubernetes 创建；回包超时先回查集群
            return await self._create_with_verify(
                namespace, info, scheduled, manifest, record
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
        return await self._reverify(namespace, info, scheduled, record)

    # ---------- 内部方法 ----------

    async def _create_with_verify(
        self, namespace, info, scheduled, manifest, record
    ) -> dict[str, Any]:
        try:
            job = await self.kube.create_job(namespace, manifest)
        except KubeError as exc:
            if "超时" not in str(exc) and "timeout" not in str(exc).lower():
                self.store.update_state(record["id"], STATE_FAILED, detail=str(exc))
                raise ServiceError(f"创建 Job 失败: {exc}", status_code=502) from exc
            # 创建回包超时：先回查集群
            return await self._verify_after_timeout(
                namespace, info, scheduled, record, str(exc)
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
        }

    async def _verify_after_timeout(
        self, namespace, info, scheduled, record, timeout_detail
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
            }
        # 再按 (UID, 计划时刻) 列查一遍，覆盖“名字不同但已建出”的小概率情况
        jobs = await self._safe_list_jobs(namespace)
        matched, source = find_matching_job(jobs, info["uid"], scheduled)
        if matched is not None:
            ref = self._job_ref(matched)
            self.store.update_state(
                record["id"], STATE_CREATED,
                job_name=ref["metadata"]["name"],
                job_uid=ref["metadata"].get("uid"),
                detail="超时后按 UID+计划时刻回查命中",
            )
            return {
                "status": "created",
                **self._base(info, scheduled, record["id"]),
                "job": ref,
                "job_source": source or "catchup",
                "reason": "创建请求回包超时，按 UID+计划时刻回查命中已有 Job",
            }
        self.store.update_state(record["id"], STATE_PENDING, detail=timeout_detail)
        return self._pending_response(
            info, scheduled, record,
            f"创建请求回包超时且回查集群未能确认: {timeout_detail}",
        )

    async def _reverify(self, namespace, info, scheduled, record) -> dict[str, Any]:
        job = await self._safe_get_job(namespace, record["job_name"])
        if job is None:
            try:
                jobs = await self.kube.list_jobs(namespace)
                job, _ = find_matching_job(jobs, info["uid"], scheduled)
            except KubeError as exc:
                raise ServiceError(f"回查集群失败，仍待核实: {exc}",
                                   status_code=502) from exc
        if job is None:
            self.store.update_state(record["id"], STATE_PENDING,
                                    detail="再次回查仍未找到 Job")
            return self._pending_response(
                info, scheduled, record, "回查集群未找到对应 Job，需人工核实"
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
            "job_source": "catchup" if is_catchup_job(job) else "scheduler",
            "reason": "回查确认 Job 已存在",
        }

    async def _safe_get_job(self, namespace: str, name: str) -> dict | None:
        try:
            return await self.kube.get_job(namespace, name)
        except KubeNotFound:
            return None
        except KubeError:
            return None

    async def _safe_list_jobs(self, namespace: str) -> list[dict]:
        try:
            return await self.kube.list_jobs(namespace)
        except KubeError:
            return []

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
    def _base(info, scheduled, record_id: int | None) -> dict:
        return {
            "namespace": info["namespace"],
            "cronjob": info["name"],
            "cronjob_uid": info["uid"],
            "scheduled_at_utc": scheduled,
            "record_id": record_id,
        }

    def _existing_response(self, info, scheduled, job, source, record_id=None) -> dict:
        return {
            "status": "returned_existing",
            **self._base(info, scheduled, record_id),
            "job": self._job_ref(job),
            "job_source": source or (
                "catchup" if is_catchup_job(job) else "scheduler"
            ),
            "reason": "已存在匹配 Job（按 CronJob UID + 计划时刻匹配）",
        }

    def _pending_response(self, info, scheduled, record, reason) -> dict:
        return {
            "status": "pending_verification",
            **self._base(info, scheduled, record["id"] if record else None),
            "job": None,
            "job_source": None,
            "reason": reason,
        }
