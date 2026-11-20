"""极简 Kubernetes API 客户端（batch/v1），基于 httpx，直连 REST。

面向 Kubernetes 1.32+：
- CronJob 使用 batch/v1（GA），字段 spec.timeZone / spec.suspend。
- Job 标签 batch.kubernetes.io/cronjob-name、
  注解 batch.kubernetes.io/cronjob-uid 与 .../job-name（KEP-3335 新格式）。
- 同时兼容旧格式 controller-uid 注解与 job-name 注解。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

# 1.32 新格式标签/注解
LABEL_CRONJOB_NAME = "batch.kubernetes.io/cronjob-name"
ANN_CRONJOB_UID = "batch.kubernetes.io/cronjob-uid"
ANN_JOB_NAME = "batch.kubernetes.io/job-name"       # value: Job 创建时间（RFC3339 UTC）
ANN_SCHEDULED_AT = "batch.kubernetes.io/scheduled-at"  # 部分版本写入
# 旧格式（兼容原调度器历史 Job）
OLD_LABEL_CRONJOB_NAME = "job-template"  # 不存在统一旧标签，旧 Job 靠 controller-uid 反查
OLD_ANN_CONTROLLER_UID = "kubernetes.io/controller-uid"
OLD_ANN_JOB_NAME = "kubernetes.io/job-name"
OLD_ANN_SCHEDULED_AT = "scheduled-at"
# 本服务补跑标记
LABEL_CATCHUP = "inspection.example.com/catchup"
ANN_CATCHUP_CRONJOB_UID = "inspection.example.com/cronjob-uid"
ANN_CATCHUP_SCHEDULED_AT = "inspection.example.com/scheduled-at"


class KubeError(RuntimeError):
    """与 Kubernetes API 交互失败。"""


class KubeNotFound(KubeError):
    """资源不存在（HTTP 404）。"""


@dataclass
class KubeConfig:
    base_url: str
    token: str | None = None
    verify_tls: bool = True
    timeout: float = 10.0
    create_timeout: float = 5.0
    verify_timeout: float = 10.0
    extra_headers: dict[str, str] | None = None

    @classmethod
    def from_env(cls) -> "KubeConfig":
        extra = None
        fault = os.environ.get("KUBE_MOCK_FAULT")
        if fault:
            extra = {"X-Mock-Fault": fault}
        return cls(
            base_url=os.environ.get("KUBE_API_URL", "http://localhost:8001"),
            token=os.environ.get("KUBE_TOKEN") or None,
            verify_tls=os.environ.get("KUBE_VERIFY_TLS", "true").lower() == "true",
            timeout=float(os.environ.get("KUBE_TIMEOUT", "10")),
            create_timeout=float(os.environ.get("KUBE_CREATE_TIMEOUT", "5")),
            verify_timeout=float(os.environ.get("KUBE_VERIFY_TIMEOUT", "10")),
            extra_headers=extra,
        )


class KubeClient:
    def __init__(self, config: KubeConfig):
        self.config = config
        headers = config.extra_headers.copy() if config.extra_headers else {}
        if config.token:
            headers["Authorization"] = f"Bearer {config.token}"
        self._client = httpx.AsyncClient(
            base_url=config.base_url.rstrip("/"),
            headers=headers or None,
            verify=config.verify_tls,
            timeout=config.timeout,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "KubeClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def get_cronjob(self, namespace: str, name: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/apis/batch/v1/namespaces/{namespace}/cronjobs/{name}",
        )

    async def list_jobs(self, namespace: str) -> list[dict[str, Any]]:
        data = await self._request(
            "GET", f"/apis/batch/v1/namespaces/{namespace}/jobs"
        )
        return data.get("items", [])

    async def create_job(self, namespace: str, job_manifest: dict[str, Any]) -> dict[str, Any]:
        # 创建单独使用更短的超时，以便触发“回包超时先回查”
        return await self._request(
            "POST",
            f"/apis/batch/v1/namespaces/{namespace}/jobs",
            json=job_manifest,
            timeout=self.config.create_timeout,
        )

    async def get_job(self, namespace: str, name: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/apis/batch/v1/namespaces/{namespace}/jobs/{name}",
            timeout=self.config.verify_timeout,
        )

    async def _request(
        self,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        try:
            resp = await self._client.request(
                method, path, json=json, timeout=timeout or self.config.timeout
            )
        except httpx.TimeoutException as exc:
            raise KubeError(f"Kubernetes API 请求超时: {method} {path}: {exc}") from exc
        except httpx.HTTPError as exc:
            raise KubeError(f"Kubernetes API 请求失败: {method} {path}: {exc}") from exc

        if resp.status_code == 404:
            raise KubeNotFound(f"{path} 不存在")
        if resp.status_code in (504, 599):
            # 网关超时/连接静默丢弃：对象可能已落库，调用方需按超时语义回查
            raise KubeError(
                f"Kubernetes API 回包超时/丢失: {method} {path} "
                f"-> HTTP {resp.status_code}"
            )
        if resp.status_code >= 400:
            raise KubeError(
                f"Kubernetes API {resp.status_code}: {method} {path} -> {resp.text[:500]}"
            )
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()


# ---------- 资源解析与匹配辅助 ----------

def cronjob_summary(cronjob: dict[str, Any]) -> dict[str, Any]:
    spec = cronjob.get("spec", {})
    meta = cronjob.get("metadata", {})
    return {
        "name": meta.get("name"),
        "uid": meta.get("uid"),
        "namespace": meta.get("namespace"),
        "schedule": spec.get("schedule"),
        "time_zone": spec.get("timeZone"),
        "suspended": bool(spec.get("suspend", False)),
    }


def _job_annotations(job: dict) -> dict[str, str]:
    return job.get("metadata", {}).get("annotations", {}) or {}


def _job_labels(job: dict) -> dict[str, str]:
    return job.get("metadata", {}).get("labels", {}) or {}


def job_owner_cronjob_uid(job: dict) -> str | None:
    """提取 Job 归属的 CronJob UID（兼容 1.32 新注解、旧 controller-uid、补跑标签）。"""
    ann = _job_annotations(job)
    if ann.get(ANN_CRONJOB_UID):
        return ann[ANN_CRONJOB_UID]
    if ann.get(ANN_CATCHUP_CRONJOB_UID):
        return ann[ANN_CATCHUP_CRONJOB_UID]
    if ann.get(OLD_ANN_CONTROLLER_UID):
        return ann[OLD_ANN_CONTROLLER_UID]
    # ownerReferences（CronJob 不直接拥有 Job，但模拟/自定义控制器可能这样写）
    for owner in job.get("metadata", {}).get("ownerReferences", []) or []:
        if owner.get("kind") == "CronJob":
            return owner.get("uid")
    return None


def job_scheduled_at_iso(job: dict) -> str | None:
    """提取 Job 上记录的计划时刻（UTC RFC3339），归一化为带 Z 的 ISO 串。"""
    ann = _job_annotations(job)
    raw = (
        ann.get(ANN_CATCHUP_SCHEDULED_AT)
        or ann.get(ANN_SCHEDULED_AT)
        or ann.get(OLD_ANN_SCHEDULED_AT)
        or ann.get(ANN_JOB_NAME)
        or ann.get(OLD_ANN_JOB_NAME)
    )
    return _normalize_iso(raw) if raw else None


def _normalize_iso(raw: str) -> str:
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def scheduled_at_iso(dt_utc: datetime) -> str:
    return dt_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_catchup_job(job: dict) -> bool:
    return _job_labels(job).get(LABEL_CATCHUP) == "true" or bool(
        _job_annotations(job).get(ANN_CATCHUP_CRONJOB_UID)
    )


def find_matching_job(
    jobs: list[dict], cronjob_uid: str, scheduled_at_utc: datetime
) -> tuple[dict | None, str | None]:
    """按 (CronJob UID, 计划时刻) 匹配已有 Job。

    返回 (job, source)；source 为 'scheduler' 或 'catchup'。
    """
    target = scheduled_at_iso(scheduled_at_utc)
    for job in jobs:
        uid = job_owner_cronjob_uid(job)
        if uid != cronjob_uid:
            continue
        if job_scheduled_at_iso(job) == target:
            return job, ("catchup" if is_catchup_job(job) else "scheduler")
    return None, None


def build_catchup_job(
    cronjob: dict, scheduled_at_utc: datetime, job_name: str
) -> dict[str, Any]:
    """按 CronJob 的 jobTemplate 构造补跑 Job 清单。"""
    spec = cronjob.get("spec", {})
    template = spec.get("jobTemplate", {})
    cj_meta = cronjob.get("metadata", {})
    scheduled_iso = scheduled_at_iso(scheduled_at_utc)

    job_spec = dict(template.get("spec", {}))
    # 补跑 Job 不允许再派生新对象，保持模板中的 selector 等配置
    labels = dict(template.get("metadata", {}).get("labels", {}) or {})
    annotations = dict(template.get("metadata", {}).get("annotations", {}) or {})

    labels.update({
        LABEL_CRONJOB_NAME: cj_meta["name"],
        LABEL_CATCHUP: "true",
    })
    annotations.update({
        ANN_CRONJOB_UID: cj_meta["uid"],
        ANN_CATCHUP_CRONJOB_UID: cj_meta["uid"],
        ANN_CATCHUP_SCHEDULED_AT: scheduled_iso,
        ANN_JOB_NAME: scheduled_iso,
    })

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": cj_meta.get("namespace"),
            "labels": labels,
            "annotations": annotations,
            "ownerReferences": [
                {
                    "apiVersion": "batch/v1",
                    "kind": "CronJob",
                    "name": cj_meta["name"],
                    "uid": cj_meta["uid"],
                    "controller": False,
                    "blockOwnerDeletion": True,
                }
            ],
        },
        "spec": job_spec,
    }
