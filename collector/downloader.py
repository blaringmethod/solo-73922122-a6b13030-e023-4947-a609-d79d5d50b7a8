"""下载执行器：断点续传、修订校验、故障重试、完成校验。

安全不变量：
  1. 同一任务同一时刻只有一个 worker（DB 原子迁移 + 内存任务表双重保证）。
  2. worker 的每一次 DB 写入都带 generation 条件；暂停/重启会使旧
     generation 作废，迟到的写入（暂停后在途的响应）不会落地。
  3. 分片文件始终是“当前 ETag 内容”的前缀：修订变化先截断再写，
     源忽略 Range 返回 200 全量时从头重写，绝不按分段追加。
  4. 只有 长度+SHA-256 均匹配才把分片改名为正式 ZIP 并开放下载。
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import re
import time

import httpx

from db import Database

MAX_ARCHIVE_BYTES = int(os.environ.get("MAX_ARCHIVE_BYTES", str(8 * 1024 * 1024)))
CHUNK = 64 * 1024
SYNC_EVERY = 256 * 1024      # 每下载这么多字节落一次进度
SYNC_INTERVAL = 0.5          # 或至少隔这么多秒落一次
MAX_ATTEMPTS = 8             # 连续无进展的最大重试次数

CONTENT_RANGE_RE = re.compile(r"bytes (\d+)-(\d+)/(\d+|\*)")


class _Stopped(Exception):
    """任务被暂停/取代/删除，worker 立即退出。"""


class _Retryable(Exception):
    """本次尝试失败但可重试（网络错误、非预期状态码等）。"""


class _RestartFromZero(Exception):
    """源状态与本地分片不一致（如 416），放弃分片从头采集。"""


def _backoff(attempts: int) -> float:
    return min(0.3 * 2 ** max(attempts - 1, 0), 5.0)


class DownloadManager:
    def __init__(self, db: Database, data_dir: str, source_base: str):
        self.db = db
        self.parts_dir = os.path.join(data_dir, "parts")
        self.final_dir = os.path.join(data_dir, "final")
        os.makedirs(self.parts_dir, exist_ok=True)
        os.makedirs(self.final_dir, exist_ok=True)
        self.source_base = source_base.rstrip("/")
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=5.0),
            follow_redirects=False,
        )
        self.tasks: dict[int, asyncio.Task] = {}
        self.stop_events: dict[int, asyncio.Event] = {}

    # ------------------------------------------------------------------ 对外操作

    async def start(self, job_id: int) -> bool:
        """开始/续传。重复点击（已在下载或已完成）安全返回 False。

        DB 原子迁移是唯一守卫：迁移成功就必须起新 worker。
        先等旧 worker 彻底退出再启动，避免两个 worker 同时写分片。
        """
        gen = self.db.transition_start(job_id)
        if gen is None:
            return False
        await self._stop_worker(job_id)
        stop_ev = asyncio.Event()
        self.stop_events[job_id] = stop_ev
        self.tasks[job_id] = asyncio.create_task(self._run(job_id, gen, stop_ev))
        return True

    async def pause(self, job_id: int) -> bool:
        """暂停。先作废 generation 再取消 worker，迟到的响应无法落盘。"""
        ok = self.db.transition_pause(job_id)
        await self._stop_worker(job_id)
        return ok

    async def resume_interrupted(self, job_ids: list[int]):
        for job_id in job_ids:
            await self.start(job_id)

    async def delete(self, job_id: int) -> dict | None:
        await self._stop_worker(job_id)
        job = self.db.delete(job_id)
        if job is not None:
            self._remove_file(self._part_path(job_id))
            if job.get("final_path"):
                self._remove_file(job["final_path"])
        return job

    async def shutdown(self):
        for job_id in list(self.tasks):
            await self._stop_worker(job_id)
        await self.client.aclose()

    async def _stop_worker(self, job_id: int):
        ev = self.stop_events.get(job_id)
        if ev is not None:
            ev.set()
        task = self.tasks.get(job_id)
        if task is not None and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=5)
            except (Exception, asyncio.CancelledError):
                pass

    # ------------------------------------------------------------------ worker

    async def _run(self, job_id: int, gen: int, stop_ev: asyncio.Event):
        try:
            await self._download_loop(job_id, gen, stop_ev)
        except asyncio.CancelledError:
            raise  # 暂停/删除/关停：DB 状态已由对方写好
        except _Stopped:
            pass
        except Exception as exc:  # 未预料错误：标失败但保留进度
            try:
                self.db.fail(job_id, gen, f"内部错误（可续传）: {exc!r}")
            except Exception:
                pass
        finally:
            # 只清理属于本 worker 的表项，避免误删继任者的登记
            if self.tasks.get(job_id) is asyncio.current_task():
                self.tasks.pop(job_id, None)
            if self.stop_events.get(job_id) is stop_ev:
                self.stop_events.pop(job_id, None)

    async def _download_loop(self, job_id: int, gen: int, stop_ev: asyncio.Event):
        attempts = 0
        while True:
            self._check_stop(stop_ev)
            job = self.db.get(job_id)
            if job is None or job["status"] != "downloading":
                raise _Stopped()
            url = f"{self.source_base}/archives/{job['station']}/{job['date']}.zip"

            # ---- 1. HEAD：拿当前 修订/长度/摘要 ----
            try:
                head = await self.client.head(url)
            except (httpx.HTTPError, OSError) as exc:
                attempts = await self._retry_or_fail(
                    job_id, gen, attempts, stop_ev, f"无法连接数据源: {exc!r}"
                )
                if attempts is None:
                    return
                continue
            if head.status_code == 404:
                self.db.fail(job_id, gen, "数据源上不存在该归档 (404)")
                return
            if head.status_code >= 400:
                attempts = await self._retry_or_fail(
                    job_id, gen, attempts, stop_ev, f"数据源响应 HTTP {head.status_code}"
                )
                if attempts is None:
                    return
                continue

            etag = head.headers.get("etag")
            size_s = head.headers.get("content-length")
            sha256 = head.headers.get("x-checksum-sha256")
            if not etag or not size_s or not sha256:
                self.db.fail(job_id, gen, "源未提供 ETag/长度/SHA-256，无法安全采集")
                return
            size = int(size_s)
            if size > MAX_ARCHIVE_BYTES:
                self.db.fail(
                    job_id, gen,
                    f"归档 {size} 字节超过 8 MiB 上限，已拒绝（可换站号重试）",
                )
                return

            # ---- 2. 修订检查：ETag 变了 → 旧分片作废，重新采集 ----
            if job["etag"] != etag:
                self._truncate(job_id)
                if not self.db.reset_meta(job_id, gen, etag, size, sha256):
                    raise _Stopped()
            else:
                if not self.db.set_meta(job_id, gen, size, sha256):
                    raise _Stopped()

            # ---- 3. 以磁盘分片为准对齐进度（崩溃/暂停后的真实字节数） ----
            offset = self._file_size(job_id)
            if offset > size:
                self._truncate(job_id)
                offset = 0
            if not self.db.set_downloaded(job_id, gen, offset):
                raise _Stopped()
            if offset == size:
                self._verify_and_finish(job_id, gen, size, sha256)
                return

            # ---- 4. GET：分段续传；源若返回全量则从头重写 ----
            headers = {}
            if offset > 0:
                headers["Range"] = f"bytes={offset}-"
                headers["If-Range"] = etag
            got_any = False
            try:
                async with self.client.stream("GET", url, headers=headers) as resp:
                    if resp.status_code == 416:
                        raise _RestartFromZero()
                    if resp.status_code not in (200, 206):
                        raise _Retryable(f"数据源响应 HTTP {resp.status_code}")

                    r_etag = resp.headers.get("etag", etag)
                    r_sha = resp.headers.get("x-checksum-sha256", sha256)

                    if resp.status_code == 200:
                        # 全量响应（源忽略 Range 或 If-Range 不匹配）：
                        # 绝不按分段追加，截断后从头重写
                        if r_etag != etag:
                            self._truncate(job_id)
                            new_size = int(resp.headers.get("content-length", size))
                            if new_size > MAX_ARCHIVE_BYTES:
                                raise _Retryable("新修订超过 8 MiB 上限")
                            if not self.db.reset_meta(job_id, gen, r_etag, new_size, r_sha):
                                raise _Stopped()
                            size, sha256 = new_size, r_sha
                        else:
                            self._truncate(job_id)
                        offset = 0
                        if not self.db.set_downloaded(job_id, gen, 0):
                            raise _Stopped()
                    else:
                        # 206：校验分段确实接在本地偏移之后
                        cr = CONTENT_RANGE_RE.fullmatch(
                            resp.headers.get("content-range", "")
                        )
                        if cr is None or int(cr.group(1)) != offset:
                            raise _RestartFromZero()
                        if r_etag != etag:
                            # 分段响应却带着别的修订：不一致，重来
                            raise _RestartFromZero()

                    part = self._part_path(job_id)
                    last_sync_offset, last_sync_time = offset, time.monotonic()
                    with open(part, "r+b" if os.path.exists(part) else "w+b") as fp:
                        fp.seek(offset)
                        async for chunk in resp.aiter_bytes(CHUNK):
                            self._check_stop(stop_ev)
                            fp.write(chunk)
                            offset += len(chunk)
                            got_any = True
                            if offset > size or offset > MAX_ARCHIVE_BYTES:
                                raise _Retryable("源发送的字节超过声明长度")
                            now = time.monotonic()
                            if (
                                offset - last_sync_offset >= SYNC_EVERY
                                or now - last_sync_time >= SYNC_INTERVAL
                            ):
                                fp.flush()
                                if not self.db.set_downloaded(job_id, gen, offset):
                                    raise _Stopped()
                                last_sync_offset, last_sync_time = offset, now
                        fp.flush()
                        os.fsync(fp.fileno())
                    if not self.db.set_downloaded(job_id, gen, offset):
                        raise _Stopped()

            except _RestartFromZero:
                self._truncate(job_id)
                if not self.db.set_downloaded(job_id, gen, 0):
                    raise _Stopped()
                continue
            except _Retryable as exc:
                attempts = await self._retry_or_fail(
                    job_id, gen, attempts, stop_ev, str(exc)
                )
                if attempts is None:
                    return
                continue
            except (httpx.HTTPError, OSError, asyncio.TimeoutError) as exc:
                # 断流/超时等传输错误：有进展则不计失败次数
                attempts = 0 if got_any else attempts + 1
                if attempts > MAX_ATTEMPTS:
                    self.db.fail(job_id, gen, f"网络错误（进度已保留，可续传）: {exc!r}")
                    return
                await self._sleep(_backoff(attempts), stop_ev)
                continue

            # ---- 5. 一次响应正常结束 ----
            if got_any:
                attempts = 0
            if offset >= size:
                self._verify_and_finish(job_id, gen, size, sha256)
                return
            # 源提前结束（未抛错的短响应）：接着续传
            attempts += 1
            if attempts > MAX_ATTEMPTS:
                self.db.fail(job_id, gen, "源反复提前结束响应（进度已保留，可续传）")
                return
            await self._sleep(_backoff(attempts), stop_ev)

    # ------------------------------------------------------------------ 校验收尾

    def _verify_and_finish(self, job_id: int, gen: int, size: int, sha256: str):
        """只有 长度+摘要 均匹配才开放完整 ZIP；否则丢弃数据并标失败。"""
        part = self._part_path(job_id)
        actual_size = os.path.getsize(part) if os.path.exists(part) else -1
        digest = hashlib.sha256()
        if actual_size >= 0:
            with open(part, "rb") as fp:
                for block in iter(lambda: fp.read(1 << 20), b""):
                    digest.update(block)
        if actual_size == size and digest.hexdigest() == sha256:
            job = self.db.get(job_id)
            final_path = os.path.join(
                self.final_dir, f"{job['station']}_{job['date']}.zip"
            )
            os.replace(part, final_path)
            self.db.complete(job_id, gen, size, final_path)
        else:
            # 字节不可信：丢弃分片，避免把坏数据续进下一次采集
            self._truncate(job_id)
            self.db.fail(
                job_id, gen,
                f"校验失败（长度 {actual_size}/{size} 或摘要不符），已丢弃数据，可重新采集",
                reset=True,
            )

    # ------------------------------------------------------------------ 小工具

    async def _retry_or_fail(
        self, job_id: int, gen: int, attempts: int, stop_ev: asyncio.Event, message: str
    ) -> int | None:
        attempts += 1
        if attempts > MAX_ATTEMPTS:
            self.db.fail(job_id, gen, f"{message}（进度已保留，可续传）")
            return None
        await self._sleep(_backoff(attempts), stop_ev)
        return attempts

    async def _sleep(self, seconds: float, stop_ev: asyncio.Event):
        try:
            await asyncio.wait_for(stop_ev.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
        self._check_stop(stop_ev)

    @staticmethod
    def _check_stop(stop_ev: asyncio.Event):
        if stop_ev.is_set():
            raise _Stopped()

    def _part_path(self, job_id: int) -> str:
        return os.path.join(self.parts_dir, f"{job_id}.part")

    def _file_size(self, job_id: int) -> int:
        path = self._part_path(job_id)
        return os.path.getsize(path) if os.path.exists(path) else 0

    def _truncate(self, job_id: int):
        with open(self._part_path(job_id), "wb"):
            pass

    @staticmethod
    def _remove_file(path: str | None):
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass
