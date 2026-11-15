"""探空报文采集服务：网页 + REST API。

- 按 站号+日期 从演示源采集归档（≤ 8 MiB），支持开始/暂停/续传
- SQLite 持久化进度，服务重启后自动续传
- 只有 长度+SHA-256 均校验通过才开放完整 ZIP 下载
- 演示源的故障开关经本服务代理，浏览器单页即可操作
"""
from __future__ import annotations

import os
import re
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field

from db import Database
from downloader import DownloadManager

DATA_DIR = os.environ.get("DATA_DIR", "./data")
SOURCE_BASE_URL = os.environ.get("SOURCE_BASE_URL", "http://127.0.0.1:8001").rstrip("/")

STATION_RE = re.compile(r"^[A-Za-z0-9_-]{1,16}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
STATIC_DIR = Path(__file__).parent / "static"

db: Database
manager: DownloadManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db, manager
    db = Database(os.path.join(DATA_DIR, "collector.db"))
    manager = DownloadManager(db, DATA_DIR, SOURCE_BASE_URL)
    # 服务重启：把上次中断的下载自动续传
    interrupted = db.mark_interrupted()
    await manager.resume_interrupted(interrupted)
    yield
    await manager.shutdown()
    db.close()


app = FastAPI(title="探空报文采集服务", lifespan=lifespan)


# ---------------------------------------------------------------------- 页面

@app.get("/", response_class=HTMLResponse)
def index():
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/api/health")
def health():
    return {"ok": True}


# ---------------------------------------------------------------------- 任务

class NewJob(BaseModel):
    station: str = Field(min_length=1, max_length=16)
    date: str
    autostart: bool = True


def job_view(job: dict) -> dict:
    return {
        "id": job["id"],
        "station": job["station"],
        "date": job["date"],
        "status": job["status"],
        "etag": job["etag"],
        "size": job["size"],
        "sha256": job["sha256"],
        "downloaded": job["downloaded"],
        "error": job["error"],
        "can_download": job["status"] == "completed"
        and bool(job["final_path"])
        and os.path.exists(job["final_path"]),
    }


@app.get("/api/jobs")
def list_jobs():
    return [job_view(j) for j in db.list_jobs()]


@app.post("/api/jobs", status_code=201)
async def create_job(payload: NewJob):
    if not STATION_RE.match(payload.station):
        raise HTTPException(400, "站号只能包含字母、数字、-、_（≤16 字符）")
    if not DATE_RE.match(payload.date):
        raise HTTPException(400, "日期格式应为 YYYY-MM-DD")
    job = db.create_job(payload.station, payload.date)
    if payload.autostart:
        await manager.start(job["id"])  # 幂等：重复点击不会起第二个 worker
        job = db.get(job["id"])
    return job_view(job)


def get_job_or_404(job_id: int) -> dict:
    job = db.get(job_id)
    if job is None:
        raise HTTPException(404, "任务不存在")
    return job


@app.get("/api/jobs/{job_id}")
def get_job(job_id: int):
    return job_view(get_job_or_404(job_id))


@app.post("/api/jobs/{job_id}/start")
async def start_job(job_id: int):
    get_job_or_404(job_id)
    started = await manager.start(job_id)
    job = db.get(job_id)
    return {**job_view(job), "started": started}


@app.post("/api/jobs/{job_id}/pause")
async def pause_job(job_id: int):
    get_job_or_404(job_id)
    paused = await manager.pause(job_id)
    job = db.get(job_id)
    return {**job_view(job), "paused": paused}


@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: int):
    job = await manager.delete(job_id)
    if job is None:
        raise HTTPException(404, "任务不存在")
    return {"deleted": job_id}


@app.get("/api/jobs/{job_id}/download")
def download_job(job_id: int):
    job = get_job_or_404(job_id)
    # 半成品一律不提供：只有校验完成、文件在位的归档才能下载
    if (
        job["status"] != "completed"
        or not job["final_path"]
        or not os.path.exists(job["final_path"])
    ):
        raise HTTPException(409, "归档尚未完成校验，不提供下载")
    return FileResponse(
        job["final_path"],
        filename=f"{job['station']}_{job['date']}.zip",
        media_type="application/zip",
    )


# ---------------------------------------------------------------------- 演示源代理

class SourceModeUpdate(BaseModel):
    mode: str | None = None
    throttle_kbps: int | None = Field(default=None, ge=0)


async def proxy_to_source(method: str, path: str, **kwargs):
    try:
        resp = await manager.client.request(method, f"{SOURCE_BASE_URL}{path}", timeout=5, **kwargs)
    except (httpx.HTTPError, OSError) as exc:
        raise HTTPException(502, f"演示源不可达: {exc!r}")
    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, resp.text)
    return resp.json()


@app.get("/api/source/status")
async def source_status():
    return await proxy_to_source("GET", "/api/control")


@app.post("/api/source/mode")
async def source_mode(update: SourceModeUpdate):
    body = {k: v for k, v in update.model_dump().items() if v is not None}
    return await proxy_to_source("POST", "/api/control", json=body)


@app.post("/api/source/revision")
async def source_revision():
    return await proxy_to_source("POST", "/api/control/revision")
