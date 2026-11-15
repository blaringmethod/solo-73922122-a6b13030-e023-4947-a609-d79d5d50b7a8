"""演示用探空报文归档数据源。

按 站号+日期 提供确定性的 ZIP 归档（同一修订下内容可复现）：
  GET/HEAD /archives/{station}/{date}.zip
    - 强 ETag（内容变化即变化）
    - Content-Length
    - X-Checksum-SHA256（整档摘要）
    - 支持 Range / If-Range（206 / 200）

故障开关（浏览器或 API 可切换）：
  - normal        正常支持分段
  - ignore-range  无视 Range/If-Range，总是 200 全量返回
  - drop          响应流到一半直接断开连接
  - 更换修订       revision+1，内容与 ETag 全部改变
  - 限速           限制每秒字节数，便于观察进度

控制接口：
  GET  /api/control                 查看状态
  POST /api/control                 {"mode": ..., "throttle_kbps": ...}
  POST /api/control/revision        更换修订
  GET  /                            控制页面
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import random
import re
import zipfile

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel

MIB = 1024 * 1024
STATION_RE = re.compile(r"^[A-Za-z0-9_-]{1,16}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MODES = ("normal", "ignore-range", "drop")

app = FastAPI(title="探空报文演示数据源")

STATE = {
    "mode": "normal",        # normal | ignore-range | drop
    "throttle_kbps": 0,      # 0 = 不限速
    "revision": 1,           # 修订号，更换后内容与 ETag 全变
}

_archive_cache: dict[tuple[str, str, int], dict] = {}


def build_archive(station: str, date: str, revision: int) -> dict:
    """按 (站号, 日期, 修订) 确定性生成一个真实 ZIP 归档。"""
    key = (station, date, revision)
    cached = _archive_cache.get(key)
    if cached is not None:
        return cached

    seed = hashlib.sha256(f"{station}|{date}|rev{revision}".encode()).digest()
    if station.lower().startswith("big"):
        target = 9 * MIB  # 故意超过采集端 8 MiB 上限，用于演示拒绝
    else:
        target = 1 * MIB + (int.from_bytes(seed[4:8], "big") % (5 * MIB))

    rng = random.Random(seed)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        i = 0
        while buf.tell() < target:
            info = zipfile.ZipInfo(f"raw_msg_{i:04d}.bin", date_time=(2020, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_STORED
            zf.writestr(info, rng.randbytes(256 * 1024))
            i += 1
    content = buf.getvalue()
    sha = hashlib.sha256(content).hexdigest()
    meta = {
        "content": content,
        "size": len(content),
        "sha256": sha,
        "etag": f'"r{revision}-{sha[:16]}"',
    }
    if len(_archive_cache) >= 32:  # 简单 LRU：清掉最早的
        _archive_cache.pop(next(iter(_archive_cache)))
    _archive_cache[key] = meta
    return meta


def parse_range(header: str, size: int):
    """解析单个字节区间。返回 (start, end) 或错误标记。"""
    m = re.fullmatch(r"bytes=(\d+)-(\d*)", header.strip())
    if not m:
        return "invalid"
    start = int(m.group(1))
    end = int(m.group(2)) if m.group(2) else size - 1
    if start >= size:
        return "unsatisfiable"
    return start, min(end, size - 1)


@app.api_route("/archives/{station}/{date}.zip", methods=["GET", "HEAD"])
async def serve_archive(station: str, date: str, request: Request):
    if not STATION_RE.match(station) or not DATE_RE.match(date):
        return JSONResponse({"detail": "站号或日期格式非法"}, status_code=400)

    meta = build_archive(station, date, STATE["revision"])
    size = meta["size"]
    headers = {
        "ETag": meta["etag"],
        "X-Checksum-SHA256": meta["sha256"],
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store",
    }

    # Range 处理：ignore-range 模式或 If-Range 不匹配时按全量 200 返回
    range_header = request.headers.get("range")
    if_range = request.headers.get("if-range")
    honor_range = (
        range_header
        and STATE["mode"] != "ignore-range"
        and not (if_range and if_range != meta["etag"])
    )

    status_code = 200
    start, end = 0, size - 1
    if honor_range:
        parsed = parse_range(range_header, size)
        if parsed == "invalid":
            return JSONResponse({"detail": "Range 语法非法"}, status_code=400)
        if parsed == "unsatisfiable":
            return Response(
                status_code=416,
                headers={**headers, "Content-Range": f"bytes */{size}"},
            )
        start, end = parsed
        status_code = 206
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    length = end - start + 1
    headers["Content-Length"] = str(length)

    if request.method == "HEAD":
        return Response(status_code=status_code, headers=headers)

    content = meta["content"]
    mode = STATE["mode"]
    throttle_bps = STATE["throttle_kbps"] * 1024

    # 断流故障：响应发出一部分后强行断开（短响应不再断，保证最终能完成）
    drop_after = None
    if mode == "drop" and length > 256 * 1024:
        rnd = int.from_bytes(hashlib.sha256(content[:64]).digest()[:4], "big")
        drop_after = 384 * 1024 + rnd % (256 * 1024)

    async def body():
        sent = 0
        pos = start
        chunk = 64 * 1024
        while pos <= end:
            if drop_after is not None and sent >= drop_after:
                # 演示故障：TCP 连接中断（uvicorn 日志里的堆栈即注入的故障）
                raise ConnectionError("演示故障：连接被演示源主动断开")
            piece = content[pos : pos + chunk]
            if drop_after is not None and sent + len(piece) > drop_after:
                piece = piece[: drop_after - sent]
            pos += len(piece)
            sent += len(piece)
            yield piece
            if throttle_bps > 0:
                await asyncio.sleep(len(piece) / throttle_bps)
            else:
                await asyncio.sleep(0)

    return StreamingResponse(
        body(), status_code=status_code, headers=headers, media_type="application/zip"
    )


class ControlUpdate(BaseModel):
    mode: str | None = None
    throttle_kbps: int | None = None


def control_state() -> dict:
    return {**STATE, "modes": list(MODES)}


@app.get("/api/control")
def get_control():
    return control_state()


@app.post("/api/control")
def set_control(update: ControlUpdate):
    if update.mode is not None:
        if update.mode not in MODES:
            return JSONResponse({"detail": f"未知模式，可选：{MODES}"}, status_code=400)
        STATE["mode"] = update.mode
    if update.throttle_kbps is not None:
        STATE["throttle_kbps"] = max(0, min(int(update.throttle_kbps), 1024 * 1024))
    return control_state()


@app.post("/api/control/revision")
def bump_revision():
    STATE["revision"] += 1
    return control_state()


CONTROL_PAGE = """<!doctype html>
<html lang="zh"><head><meta charset="utf-8"><title>演示数据源控制</title>
<style>
 body{font-family:system-ui,sans-serif;max-width:720px;margin:2rem auto;padding:0 1rem}
 button{margin:.2rem;padding:.4rem .9rem;cursor:pointer}
 #state{background:#f4f4f4;padding:.8rem;border-radius:6px;white-space:pre;font-family:monospace}
</style></head><body>
<h1>演示数据源控制</h1>
<div id="state">加载中…</div>
<p>
 <button onclick="setMode('normal')">正常</button>
 <button onclick="setMode('ignore-range')">忽略 Range</button>
 <button onclick="setMode('drop')">断流</button>
 <button onclick="bump()">更换修订（内容变更）</button>
</p>
<p>限速(KiB/s，0=不限)：<input id="thr" type="number" value="0" min="0" style="width:7rem">
 <button onclick="setThrottle()">应用</button></p>
<p>归档地址格式：<code>/archives/{站号}/{YYYY-MM-DD}.zip</code>（站号以 big 开头时生成 9 MiB 归档）</p>
<script>
async function refresh(){
  const s = await (await fetch('/api/control')).json();
  document.getElementById('state').textContent =
    `模式: ${s.mode}\\n修订: r${s.revision}\\n限速: ${s.throttle_kbps} KiB/s`;
  document.getElementById('thr').value = s.throttle_kbps;
}
async function setMode(m){ await fetch('/api/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode:m})}); refresh(); }
async function setThrottle(){ const v=+document.getElementById('thr').value||0; await fetch('/api/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({throttle_kbps:v})}); refresh(); }
async function bump(){ await fetch('/api/control/revision',{method:'POST'}); refresh(); }
refresh(); setInterval(refresh, 2000);
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return CONTROL_PAGE
