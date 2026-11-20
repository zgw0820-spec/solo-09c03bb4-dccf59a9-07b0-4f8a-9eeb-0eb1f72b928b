"""补跑台账（SQLite）与同轮并发控制。

- 唯一约束 (cronjob_uid, scheduled_at) 保证同轮只可能落一条补跑记录。
- 内存 asyncio.Lock 按 (namespace, cronjob, scheduled_at) 加锁，挡同进程并发。
- 状态机：creating -> created / pending_verification / failed。
  重启后 creating 记录视为 pending_verification，可继续回查。
"""
from __future__ import annotations

import asyncio
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STATE_CREATING = "creating"
STATE_CREATED = "created"
STATE_PENDING = "pending_verification"
STATE_FAILED = "failed"

SOURCE_CATCHUP = "catchup"
SOURCE_SCHEDULER = "scheduler"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class CatchupStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()
        self._async_locks: dict[str, asyncio.Lock] = {}

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catchup_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    namespace TEXT NOT NULL,
                    cronjob_name TEXT NOT NULL,
                    cronjob_uid TEXT NOT NULL,
                    scheduled_at TEXT NOT NULL,
                    job_name TEXT,
                    job_uid TEXT,
                    source TEXT NOT NULL DEFAULT 'catchup',
                    state TEXT NOT NULL,
                    detail TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (cronjob_uid, scheduled_at)
                )
                """
            )
            self._conn.commit()

    @asynccontextmanager
    async def async_lock_for(self, key: str):
        """同轮并发锁：同一 (CronJob, 计划时刻) 串行化。

        退出且锁已无人持有时回收字典条目，避免长期运行无限增长。
        """
        if key not in self._async_locks:
            self._async_locks[key] = asyncio.Lock()
        lock = self._async_locks[key]
        try:
            async with lock:
                yield
        finally:
            # 走到这里本协程已释放；仍 locked 说明还有其他协程持有，保留条目
            if not lock.locked():
                self._async_locks.pop(key, None)

    def get_by_round(self, cronjob_uid: str, scheduled_at: str) -> dict[str, Any] | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM catchup_records WHERE cronjob_uid=? AND scheduled_at=?",
                (cronjob_uid, scheduled_at),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def insert_creating(
        self,
        namespace: str,
        cronjob_name: str,
        cronjob_uid: str,
        scheduled_at: str,
        job_name: str,
    ) -> dict[str, Any]:
        now = _utcnow_iso()
        with self._lock:
            # 已有同轮记录（并发/重试/重启）：直接返回，交由上层查回
            existing = self.get_by_round(cronjob_uid, scheduled_at)
            if existing is not None:
                return existing
            cur = self._conn.execute(
                """
                INSERT INTO catchup_records
                  (namespace, cronjob_name, cronjob_uid, scheduled_at,
                   job_name, source, state, detail, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (namespace, cronjob_name, cronjob_uid, scheduled_at,
                 job_name, SOURCE_CATCHUP, STATE_CREATING, None, now, now),
            )
            self._conn.commit()
            return self.get_by_round(cronjob_uid, scheduled_at)  # type: ignore[return-value]

    def update_state(
        self, record_id: int, state: str, detail: str | None = None,
        job_name: str | None = None, job_uid: str | None = None,
    ) -> None:
        now = _utcnow_iso()
        with self._lock:
            self._conn.execute(
                """
                UPDATE catchup_records
                SET state=?, detail=COALESCE(?, detail),
                    job_name=COALESCE(?, job_name),
                    job_uid=COALESCE(?, job_uid),
                    updated_at=?
                WHERE id=?
                """,
                (state, detail, job_name, job_uid, now, record_id),
            )
            self._conn.commit()

    def list_records(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM catchup_records ORDER BY id DESC LIMIT ?", (limit,)
            )
            return [dict(r) for r in cur.fetchall()]

    def recover_creating(self) -> list[dict[str, Any]]:
        """重启恢复：把 creating 标记为 pending_verification。"""
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM catchup_records WHERE state=?", (STATE_CREATING,)
            )
            rows = [dict(r) for r in cur.fetchall()]
            if rows:
                self._conn.execute(
                    "UPDATE catchup_records SET state=?, detail=COALESCE(detail, ?), "
                    "updated_at=? WHERE state=?",
                    (STATE_PENDING, "进程重启，创建结果待核实", _utcnow_iso(),
                     STATE_CREATING),
                )
                self._conn.commit()
                ids = [r["id"] for r in rows]
                placeholders = ",".join("?" for _ in ids)
                cur = self._conn.execute(
                    f"SELECT * FROM catchup_records WHERE id IN ({placeholders})",
                    ids,
                )
                rows = [dict(r) for r in cur.fetchall()]
        return rows

    def close(self) -> None:
        with self._lock:
            self._conn.close()
