# 探空报文采集平台

观测站每日原始报文 ZIP 的采集网页：按 **站号 + 日期** 从本地 HTTP 演示源拉取归档（≤ 8 MiB），
支持 **开始 / 暂停 / 续传 / 进度查看**，网络中断、服务重启后均可安全续传；
只有 **长度 + SHA-256 均校验通过** 的完整归档才开放下载，交给后续解码作业。

## 组成

| 服务 | 端口 | 说明 |
|---|---|---|
| `collector` | 8000 | 采集网页 + REST API（FastAPI + SQLite + httpx） |
| `source` | 8001 | 演示数据源（FastAPI），带故障注入开关 |

```
浏览器 ──► collector:8000 ──► source:8001
              │  SQLite（任务/进度/generation）
              └─ /data/parts/*.part（续传分片）  /data/final/*.zip（校验通过的成品）
```

## 启动

```bash
docker compose up --build
# 采集台  http://localhost:8000   （采集操作 + 演示源故障开关都在此页）
# 演示源  http://localhost:8001   （源自己的控制页，可选）
```

采集端数据（SQLite、分片、成品）持久化在 `./data/`，重启容器后自动续传中断的任务。

## 演示脚本

1. **正常采集**：输入站号（如 `54511`）、日期，点“开始采集”，进度条走到 100% 后出现“下载 ZIP”。
2. **暂停 / 续传**：把限速设为 `256` KiB/s 再新建任务，下载中点“暂停”，进度停住；点“续传”继续。
   暂停后在途的响应不会改写进度（worker 已取消，且所有进度写入带 generation 守卫）。
3. **断流**：故障开关切到“断流”。源每次响应流到一半就断开，采集端自动带 Range 续传，
   进度逐段推进直到完成（源日志里的堆栈即注入的故障）。
4. **忽略 Range**：切到“忽略 Range”。续传请求也返回 200 全量，采集端**从头重写**而不是
   按分段追加（进度会回退到 0 再涨），最终校验通过。
5. **更换修订**：下载中或暂停时点“更换修订”，源内容与 ETag 全变。采集端在下一轮 HEAD
   发现 ETag 变化，**丢弃旧分片重新采集**，绝不拼接不同修订。
6. **服务重启**：下载中 `docker compose restart collector`，任务自动续传至完成。
7. **超限拒绝**：站号用 `big` 开头（如 `big001`），源生成 9 MiB 归档，采集端拒绝并标记失败。
8. **半成品不开放**：未完成/失败任务的下载接口一律返回 409，只有校验通过的成品可下载。

## 安全设计要点

- **单一 writer**：`(station, date)` 唯一约束 + 状态原子迁移（`UPDATE … WHERE status=…`），
  重复点击、重复创建都是幂等 no-op；同一任务同一时刻只有一个下载 worker。
- **generation 守卫**：每次开始/暂停/重启都会 `generation+1`，worker 的每一次进度写入都带
  `WHERE generation=?`，暂停后迟到的响应、过期的 worker 一律写不进库。
- **分片即前缀**：分片文件始终是“当前 ETag 内容”的前缀。修订变化先截断再写；
  源返回 200 全量（忽略 Range / If-Range 不匹配）时从头重写；续传偏移以磁盘分片实际
  大小为准（崩溃后 DB 与文件自动对齐）。
- **校验收尾**：下载到声明长度后计算 SHA-256，**长度和摘要均匹配**才 `rename` 为正式 ZIP
  并置为完成；摘要不符丢弃数据（不可信字节不留进下一次采集），网络类失败保留进度可续传。
- **8 MiB 上限**：HEAD 声明超限直接拒绝；传输中超过声明长度立即中止。

## REST API 摘要

采集端（8000）：

```
POST   /api/jobs                    {"station","date","autostart"}  幂等创建并开始
GET    /api/jobs                    任务列表（含进度）
POST   /api/jobs/{id}/start         开始/续传（幂等）
POST   /api/jobs/{id}/pause         暂停
DELETE /api/jobs/{id}               删除任务及文件
GET    /api/jobs/{id}/download      仅校验完成后可下载，否则 409
GET    /api/source/status           演示源状态（代理）
POST   /api/source/mode             {"mode","throttle_kbps"}（代理）
POST   /api/source/revision         更换修订（代理）
```

演示源（8001）：

```
GET/HEAD /archives/{station}/{date}.zip   支持 Range/If-Range，带 ETag、Content-Length、
                                          X-Checksum-SHA256
GET/POST /api/control                     故障开关：normal | ignore-range | drop，限速
POST     /api/control/revision            修订 +1（内容与 ETag 全变）
GET      /                                源控制页
```

## 本地开发（不用 Docker）

```bash
pip install -r source/requirements.txt -r collector/requirements.txt
(cd source && uvicorn app:app --port 8001) &
(cd collector && DATA_DIR=./data SOURCE_BASE_URL=http://127.0.0.1:8001 \
  uvicorn app:app --port 8000) &
```

说明：演示源的修订号保存在内存中，源服务重启后回到 r1（采集端会检测到 ETag 变化并重新采集）。
采集端以单 worker 进程运行（任务表为进程内状态），不要以 `--workers >1` 启动。
