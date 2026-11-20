"""按命名空间复用的 Job 实时索引（LIST + WATCH 增量维护）。

设计要点：
- 每个命名空间只做一次初始 LIST，之后用 WATCH 增量维护；
- 索引按 ``(CronJob UID, 计划时刻)`` 建键，查询 O(1)，不再每次扫描全部 Job；
- WATCH 断开或 resourceVersion 过期（410）期间，保留“上一次完整清单”并标记为过期，
  查询会同时带上该索引与过期状态；
- 首次 LIST 尚未完成时索引为 initializing，查询方需明确返回“未就绪”；
- 重新连上 WATCH 前先重新 LIST，用最新清单原子替换旧索引，再继续增量维护。
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .kube import (
    KubeClient,
    KubeError,
    KubeGone,
    job_owner_cronjob_uid,
    job_scheduled_at_iso,
)

log = logging.getLogger(__name__)

STATE_INITIALIZING = "initializing"  # 首次 LIST 未完成，索引不可用
STATE_READY = "ready"                # WATCH 已建立，索引实时
STATE_STALE = "stale"                # 有完整清单但 WATCH 已断开/过期

JobKey = tuple[str, str]  # (cronjob_uid, scheduled_at_iso)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _resource_version(obj: dict[str, Any]) -> str | None:
    rv = (obj.get("metadata") or {}).get("resourceVersion")
    return str(rv) if rv is not None else None


@dataclass(frozen=True)
class IndexStatus:
    """可安全对外输出的索引快照状态。"""

    namespace: str
    state: str
    has_snapshot: bool
    job_count: int
    resource_version: str | None
    synced_at: datetime | None
    stale_since: datetime | None
    last_error: str | None

    @property
    def ready(self) -> bool:
        return self.state == STATE_READY

    @property
    def stale(self) -> bool:
        return self.state == STATE_STALE

    def as_dict(self) -> dict[str, Any]:
        """给 API 用的纯 JSON 安全字典（时间转 ISO 串）。"""
        return {
            "namespace": self.namespace,
            "state": self.state,
            "ready": self.ready,
            "stale": self.stale,
            "has_snapshot": self.has_snapshot,
            "job_count": self.job_count,
            "resource_version": self.resource_version,
            "synced_at": self.synced_at.isoformat() if self.synced_at else None,
            "stale_since": self.stale_since.isoformat() if self.stale_since else None,
            "last_error": self.last_error,
        }


@dataclass(frozen=True)
class IndexView:
    """某一时刻的索引只读视图（entries 为不可变快照）。"""

    status: IndexStatus
    entries: dict[JobKey, dict[str, Any]] = field(repr=False)

    @property
    def state(self) -> str:
        return self.status.state

    @property
    def ready(self) -> bool:
        return self.status.ready

    @property
    def has_snapshot(self) -> bool:
        return self.status.has_snapshot

    @property
    def stale(self) -> bool:
        """有完整清单但当前不是实时状态（断线/410 期间）。"""
        return self.status.state == STATE_STALE

    def lookup(self, cronjob_uid: str, scheduled_iso: str) -> dict[str, Any] | None:
        """只按 (CronJob UID, 计划时刻) 命中，不扫描其他 Job。"""
        return self.entries.get((cronjob_uid, scheduled_iso))


class NamespaceJobIndex:
    """单个命名空间的常驻 Job 索引。首次查询时懒启动。"""

    def __init__(
        self,
        namespace: str,
        kube: KubeClient,
        *,
        initial_wait: float = 2.0,
        retry_min: float = 0.2,
        retry_max: float = 5.0,
    ):
        self.namespace = namespace
        self.kube = kube
        self.initial_wait = initial_wait
        self.retry_min = retry_min
        self.retry_max = retry_max

        self._entries: dict[JobKey, dict[str, Any]] = {}
        self._by_name: dict[str, JobKey] = {}
        self._state = STATE_INITIALIZING
        self._ever_ready = False
        self._rv: str | None = None
        self._synced_at: datetime | None = None
        self._stale_since: datetime | None = None
        self._last_error: str | None = None

        self._task: asyncio.Task | None = None
        self._closed = False
        self._progress = asyncio.Event()

    # ---------- 生命周期 ----------

    @property
    def started(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if not self._closed and not self.started:
            self._task = asyncio.create_task(
                self._run(), name=f"job-index-{self.namespace}"
            )

    async def close(self) -> None:
        self._closed = True
        self._progress.set()
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001 - 关闭时忽略
                pass

    def _set_state(self, state: str, error: str | None = None) -> None:
        now = _now()
        if state == STATE_READY:
            self._state = STATE_READY
            self._ever_ready = True
            self._stale_since = None
            self._last_error = None
        else:
            self._state = state
            if state == STATE_STALE and self._stale_since is None:
                self._stale_since = now
            if error is not None:
                self._last_error = error
        self._progress.set()
        self._progress = asyncio.Event()

    # ---------- 查询 ----------

    def status(self) -> IndexStatus:
        return IndexStatus(
            namespace=self.namespace,
            state=self._state,
            has_snapshot=self._synced_at is not None,
            job_count=len(self._entries),
            resource_version=self._rv,
            synced_at=self._synced_at,
            stale_since=self._stale_since,
            last_error=self._last_error,
        )

    def view(self) -> IndexView:
        # dict 浅拷贝即不可变快照：值对象在替换/删除时整体换掉，不原地修改
        return IndexView(status=self.status(), entries=dict(self._entries))

    async def ensure_started(self, timeout: float | None = None) -> IndexView:
        """启动索引，最多等待 timeout 秒；返回当时视图（可能未就绪/过期）。

        已有上一次完整清单（stale）时立即返回，避免断线期间每次查询都阻塞等待。
        """
        self.start()
        deadline = asyncio.get_running_loop().time() + (
            self.initial_wait if timeout is None else timeout
        )
        while True:
            # ready：WATCH 已建立；stale：有上一次完整清单（断线/410，带过期状态）。
            # 只有仍在 initializing（首次 LIST 未完成、WATCH 未建立）才继续等。
            if self._state in (STATE_READY, STATE_STALE):
                break
            # 首次 LIST 已明确失败：不必让每个请求都等满超时，直接带错误返回
            if self._synced_at is None and self._last_error is not None:
                break
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            wait_for = self._progress
            try:
                await asyncio.wait_for(wait_for.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                break
        return self.view()

    # ---------- 内部：索引维护 ----------

    def _install_snapshot(self, items: list[dict[str, Any]], rv: str | None) -> None:
        entries: dict[JobKey, dict[str, Any]] = {}
        by_name: dict[str, JobKey] = {}
        for job in items:
            key = self._key_of(job)
            if key is None:
                continue
            entries[key] = job
            name = (job.get("metadata") or {}).get("name")
            if name:
                by_name[name] = key
        self._entries = entries
        self._by_name = by_name
        self._rv = rv
        self._synced_at = _now()
        if not self._ever_ready:
            # 首次 LIST 完成但 WATCH 尚未建立：仍视为未就绪（不可用于判定“无 Job”）
            self._state = STATE_INITIALIZING
        # 已就绪过的索引保持原状态：重建期间继续提供上一次完整清单，
        # 由 _run() 在进入重建时标记 stale，WATCH 建立后回到 ready。
        self._progress.set()
        self._progress = asyncio.Event()

    @staticmethod
    def _key_of(job: dict[str, Any]) -> JobKey | None:
        uid = job_owner_cronjob_uid(job)
        scheduled = job_scheduled_at_iso(job)
        if not uid or not scheduled:
            return None
        return uid, scheduled

    def _apply_event(self, event_type: str, job: dict[str, Any]) -> None:
        name = (job.get("metadata") or {}).get("name")
        key = self._key_of(job)

        if event_type == "DELETED":
            old_key = self._by_name.pop(name, None) if name else None
            if old_key is not None:
                self._entries.pop(old_key, None)
            elif key is not None:
                self._entries.pop(key, None)
            self._rv = _resource_version(job) or self._rv
            return

        if key is None:
            # 不带 UID/计划时刻注解的 Job 无法按约定命中，踢出索引
            if name:
                old_key = self._by_name.pop(name, None)
                if old_key is not None:
                    self._entries.pop(old_key, None)
            self._rv = _resource_version(job) or self._rv
            return

        if name:
            old_key = self._by_name.get(name)
            if old_key is not None and old_key != key:
                self._entries.pop(old_key, None)
            self._by_name[name] = key
        self._entries[key] = job
        self._rv = _resource_version(job) or self._rv

    async def _run(self) -> None:
        backoff = self.retry_min
        while not self._closed:
            try:
                data = await self.kube.list_jobs_page(self.namespace)
                items = data.get("items", []) or []
                rv = (data.get("metadata") or {}).get("resourceVersion")
                if rv is None:
                    raise KubeError("Job LIST 未返回 metadata.resourceVersion，无法建立 WATCH")
                rv = str(rv)
                self._install_snapshot(items, rv)
                await self._consume_watch(rv)
                # WATCH 正常结束（服务端超时/主动关闭）：从结束到下一次 LIST 之间
                # 可能有事件遗漏，先标记过期，重连并重新 LIST 后再回到 ready
                self._set_state(
                    STATE_STALE,
                    "WATCH 连接结束，索引已过期，正在重新 LIST 重建",
                )
                backoff = self.retry_min
                continue
            except asyncio.CancelledError:
                raise
            except KubeGone as exc:
                self._set_state(
                    STATE_STALE,
                    f"WATCH resourceVersion 已过期(410)，将重新 LIST 重建索引: {exc}",
                )
            except KubeError as exc:
                error = f"索引维护失败: {exc}"
                # 从未成功 LIST：保持 initializing（不可用），否则保留旧清单但标记过期
                state = STATE_STALE if self._ever_ready else STATE_INITIALIZING
                self._set_state(state, error)
            except Exception as exc:  # noqa: BLE001 - 后台任务不能因单次异常退出
                log.exception("Job 索引后台任务异常: namespace=%s", self.namespace)
                state = STATE_STALE if self._ever_ready else STATE_INITIALIZING
                self._set_state(state, f"索引维护异常: {exc}")

            if self._closed:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self.retry_max)

    async def _consume_watch(self, rv: str) -> None:
        opened = False

        def on_open() -> None:
            nonlocal opened
            opened = True
            self._set_state(STATE_READY)

        async for event in self.kube.watch_jobs(
            self.namespace, rv, on_open=on_open
        ):
            event_type = event.get("type")
            obj = event.get("object") or {}
            if event_type == "ERROR":
                code = obj.get("code")
                if code == 410:
                    raise KubeGone(
                        f"WATCH {self.namespace} 返回 ERROR 410: "
                        f"{obj.get('message') or ''}".strip()
                    )
                raise KubeError(
                    f"WATCH {self.namespace} 返回错误事件: {obj}"
                )
            if event_type == "BOOKMARK":
                self._rv = _resource_version(obj) or self._rv
                continue
            if event_type in ("ADDED", "MODIFIED", "DELETED"):
                self._apply_event(event_type, obj)
        if not opened:
            raise KubeError("WATCH 未能建立连接")


class JobIndexManager:
    """按命名空间复用索引实例；随应用生命周期关闭。"""

    def __init__(self, kube: KubeClient, *, initial_wait: float = 2.0):
        self.kube = kube
        self.initial_wait = initial_wait
        self._indexes: dict[str, NamespaceJobIndex] = {}

    def get(self, namespace: str) -> NamespaceJobIndex:
        index = self._indexes.get(namespace)
        if index is None:
            index = NamespaceJobIndex(
                namespace, self.kube, initial_wait=self.initial_wait
            )
            self._indexes[namespace] = index
            index.start()
        return index

    async def restart(self, namespace: str) -> NamespaceJobIndex:
        """丢弃现有索引并重新 LIST（演练用：模拟进程首次启动/索引重建）。"""
        old = self._indexes.pop(namespace, None)
        if old is not None:
            await old.close()
        index = NamespaceJobIndex(namespace, self.kube, initial_wait=self.initial_wait)
        self._indexes[namespace] = index
        index.start()
        return index

    def statuses(self) -> list[dict[str, Any]]:
        return [idx.status().as_dict() for idx in self._indexes.values()]

    async def close(self) -> None:
        await asyncio.gather(
            *(idx.close() for idx in self._indexes.values()),
            return_exceptions=True,
        )
        self._indexes.clear()
