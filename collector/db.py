"""采集任务的 SQLite 持久化。

所有会改变任务关键状态的写操作都带 generation 条件：
暂停/重启会把 generation +1，旧 worker 的迟到写入因此全部失效，
避免“暂停后迟到的响应”或重复点击改写续传后的字节与进度。
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  station     TEXT NOT NULL,
  date        TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'pending',
  etag        TEXT,
  size        INTEGER,
  sha256      TEXT,
  downloaded  INTEGER NOT NULL DEFAULT 0,
  generation  INTEGER NOT NULL DEFAULT 0,
  error       TEXT,
  final_path  TEXT,
  created_at  REAL NOT NULL,
  updated_at  REAL NOT NULL,
  UNIQUE(station, date)
);
"""

# pending -> downloading -> (paused | failed | completed)
# paused/failed -> downloading （续传）, completed 为终态
STATUSES = ("pending", "downloading", "paused", "completed", "failed")


class Database:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            cur = self._conn.execute(sql, params)
            self._conn.commit()
            return cur

    # ---------- 查询 ----------

    def get(self, job_id: int) -> dict | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def list_jobs(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM jobs ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]

    # ---------- 创建（幂等：同一 站号+日期 只有一个任务） ----------

    def create_job(self, station: str, date: str) -> dict:
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO jobs(station, date, status, created_at, updated_at)"
                " VALUES(?, ?, 'pending', ?, ?)",
                (station, date, now, now),
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE station=? AND date=?", (station, date)
            ).fetchone()
        return dict(row)

    # ---------- 状态迁移（原子条件更新，重复点击安全） ----------

    def transition_start(self, job_id: int) -> int | None:
        """pending/paused/failed -> downloading。成功返回新 generation，否则 None。"""
        cur = self._execute(
            "UPDATE jobs SET status='downloading', generation=generation+1,"
            " error=NULL, updated_at=?"
            " WHERE id=? AND status IN ('pending','paused','failed')",
            (time.time(), job_id),
        )
        if cur.rowcount == 0:
            return None
        return self.get(job_id)["generation"]

    def transition_pause(self, job_id: int) -> bool:
        """downloading -> paused，并作废当前 generation。"""
        cur = self._execute(
            "UPDATE jobs SET status='paused', generation=generation+1, updated_at=?"
            " WHERE id=? AND status='downloading'",
            (time.time(), job_id),
        )
        return cur.rowcount > 0

    def mark_interrupted(self) -> list[int]:
        """服务重启时把残留的 downloading 标记为中断（随后可自动续传）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM jobs WHERE status='downloading'"
            ).fetchall()
            self._conn.execute(
                "UPDATE jobs SET status='paused', generation=generation+1,"
                " error='服务重启前中断', updated_at=? WHERE status='downloading'",
                (time.time(),),
            )
            self._conn.commit()
        return [r["id"] for r in rows]

    # ---------- 下载过程写（全部按 generation 守卫） ----------

    def reset_meta(self, job_id: int, gen: int, etag: str, size: int, sha256: str) -> bool:
        """修订变化：记录新元数据并清零进度（调用方负责清空分片文件）。"""
        cur = self._execute(
            "UPDATE jobs SET etag=?, size=?, sha256=?, downloaded=0, updated_at=?"
            " WHERE id=? AND generation=? AND status='downloading'",
            (etag, size, sha256, time.time(), job_id, gen),
        )
        return cur.rowcount > 0

    def set_meta(self, job_id: int, gen: int, size: int, sha256: str) -> bool:
        cur = self._execute(
            "UPDATE jobs SET size=?, sha256=?, updated_at=?"
            " WHERE id=? AND generation=? AND status='downloading'",
            (size, sha256, time.time(), job_id, gen),
        )
        return cur.rowcount > 0

    def set_downloaded(self, job_id: int, gen: int, downloaded: int) -> bool:
        cur = self._execute(
            "UPDATE jobs SET downloaded=?, updated_at=?"
            " WHERE id=? AND generation=? AND status='downloading'",
            (downloaded, time.time(), job_id, gen),
        )
        return cur.rowcount > 0

    def complete(self, job_id: int, gen: int, size: int, final_path: str) -> bool:
        cur = self._execute(
            "UPDATE jobs SET status='completed', downloaded=?, final_path=?,"
            " error=NULL, updated_at=?"
            " WHERE id=? AND generation=? AND status='downloading'",
            (size, final_path, time.time(), job_id, gen),
        )
        return cur.rowcount > 0

    def fail(self, job_id: int, gen: int, error: str, reset: bool = False) -> bool:
        """失败但保留可安全续传的进度；reset=True 时丢弃不可信的已下载字节。"""
        downloaded_clause = ", downloaded=0" if reset else ""
        cur = self._execute(
            f"UPDATE jobs SET status='failed', error=?{downloaded_clause}, updated_at=?"
            " WHERE id=? AND generation=? AND status='downloading'",
            (error, time.time(), job_id, gen),
        )
        return cur.rowcount > 0

    # ---------- 删除 ----------

    def delete(self, job_id: int) -> dict | None:
        job = self.get(job_id)
        if job is None:
            return None
        self._execute("DELETE FROM jobs WHERE id=?", (job_id,))
        return job

    def close(self):
        with self._lock:
            self._conn.close()
