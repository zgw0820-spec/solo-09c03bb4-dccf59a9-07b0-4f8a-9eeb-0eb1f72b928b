# CronJob 巡检补跑服务（FastAPI + Kubernetes 1.32+）

检修期间 CronJob 被暂停（`spec.suspend: true`），值班员需要核实“最近一轮到点的巡检”有没有漏跑，
漏了就按原模板补一个 Job。本服务把这套流程做成 HTTP API，并附带一个本地 Kubernetes API 模拟服务，
可复现 **Job 已创建但创建回包超时** 的场景。

## 功能范围

1. **查最近一次计划**
   - 只接受显式设置 `spec.timeZone`、且为**每天固定时刻**（日/月/星期均为通配）的 CronJob。
   - 自后向前扫描最近一次应到点的计划时刻（UTC 返回）。
   - 夏令时处理：
     - 回拨产生的**重复时刻取第一次**（`fold=0`）；
     - 春进产生的**不存在时刻直接跳过**，不取平移后的时间。
2. **Job 盘点：按命名空间复用的实时索引**
   - 每个命名空间只做一次初始 `LIST`，之后用 `WATCH` 增量维护（ADDED/MODIFIED/DELETED）；
   - 索引键为 `(CronJob UID, 计划时刻)`，查询 O(1) 命中，**不再每次扫描命名空间全部 Job**；
   - 兼容 Kubernetes 1.32 的新注解 `batch.kubernetes.io/cronjob-uid` / `job-name`，
     也兼容旧的 `kubernetes.io/controller-uid` / `scheduled-at`；原调度器 Job 与补跑 Job 一视同仁；
   - 三种索引状态：
     - `ready`：WATCH 已建立，索引实时；
     - `stale`：WATCH 断开或 `resourceVersion` 过期（410）。保留**上一次完整索引**并带过期标记，
       查询结果可用于参考（已命中的 Job 仍返回），但“查不到”不能当作“没有 Job”；
     - `initializing`：首次 `LIST` 未完成，索引不可用。
   - 重新连上 WATCH 前先重新 `LIST`，用最新清单原子替换索引，再继续增量维护，之后回到 `ready`。
3. **补跑**
   - 已有 Job：原样返回，不重复创建；
   - **索引不实时（`stale` / `initializing`）时禁止补建**：空结果不当成“无 Job”，
     直接返回 `pending_verification` 并说明索引状态，等索引恢复后再补；
   - 仅当 **CronJob 仍处于暂停** 且 **到点后 2 小时内（按 UTC）** 才按 `jobTemplate` 补建 Job；
   - 同轮（同 CronJob + 同计划时刻）并发只允许建一个（进程内异步锁 + SQLite 唯一约束双保险）；
   - 补建 Job 带 `inspection.example.com/catchup=true` 标签和 UID/计划时刻注解。
4. **超时与可靠性**
   - 创建请求回包超时：先按 Job 名回查，再在索引里按 (UID, 计划时刻) 命中；
   - 回查能确认 → 正常返回创建结果；查不清 → 返回 `pending_verification`（HTTP 200），提示人工核实；
   - 补跑台账持久化在 SQLite，**进程重启后**创建到一半的记录自动转为待核实，可通过 `/reverify` 查回；
   - 超过 2 小时补跑期限后，计划与历史 Job/台账仍可查询，只是不能再新建。
5. **演练支持**
   - 所有业务接口支持 `at` 查询参数（ISO 8601，可带时区），在 Swagger 上即可指定演练时间；
   - `POST /rehearsal/job-index/{ns}/disconnect|expire|recover|not-ready` 一键演示断线、410、
     恢复与首次索引未就绪，返回值即索引状态；
   - `GET /job-indexes`、`GET /health`、`GET /rehearsal/mock-stats` 可核对索引状态与 LIST/WATCH 计数。

## 目录结构

```
app/
  schedule.py   # cron 解析、时区/DST 计算
  kube.py       # Kubernetes batch/v1 客户端（LIST/WATCH/创建）、Job 解析与清单构造
  jobindex.py   # 按命名空间复用的 Job 实时索引（LIST + WATCH，ready/stale/initializing）
  store.py      # SQLite 台账 + 同轮并发锁
  service.py    # 补跑编排（校验、索引命中、创建、超时回查、状态机）
  models.py     # Swagger 响应模型
  main.py       # FastAPI 入口（含演练控制接口）
mock_k8s/
  server.py     # 本地 Kubernetes API 模拟（WATCH、resourceVersion、故障/断线注入）
tests/          # pytest：调度/DST、接口、并发、超时重试、重启恢复、索引状态
docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
```

- 模拟 K8s API：<http://localhost:8001>（文档内路径 `/docs`）
- 巡检补跑 API：<http://localhost:8080/docs>
- Compose 默认注入 `KUBE_MOCK_FAULT=create-timeout` 且客户端创建超时 3s、模拟端滞留 8s，
  因此每次补跑都会稳定复现“**创建成功后回包超时 → 回查确认**”。
  置空该环境变量即恢复正常路径。
- 索引相关环境变量：`JOB_INDEX_INITIAL_WAIT`（首次 LIST 等待上限，默认 5s）、
  `KUBE_WATCH_TIMEOUT`（单次 WATCH 超时，默认 300s）。

### 模拟服务故障注入（请求头 `X-Mock-Fault`）

| 值 | 行为 |
|---|---|
| `create-timeout` | Job 先落库，再滞留到客户端超时后返回 504 |
| `create-timeout-lose` | Job 先落库，然后直接断开连接（无任何回包） |
| `create-conflict-once` | 首次创建返回 409，第二次正常 |

## 不用容器的本地运行方式

```bash
pip install -r requirements.txt
# 终端 1：模拟 K8s
uvicorn mock_k8s.server:app --port 8001
# 终端 2：巡检服务
KUBE_API_URL=http://127.0.0.1:8001 CATCHUP_DB=./data/catchup.db \
  uvicorn app.main:app --port 8080
```

## 典型调用（Swagger 也可点选）

种子 CronJob `inspection-daily`：`0 9 * * *`，时区 `Asia/Shanghai`，暂停；
模拟端已预置一轮原调度器 Job（计划时刻 `2026-11-20T01:00:00Z`）。

```bash
# 查最近一轮计划与已有 Job（响应含 index_state/index_stale/index_status）
curl "http://localhost:8080/namespaces/default/cronjobs/inspection-daily/latest-schedule?at=2026-11-20T01:30:00Z"

# 补跑（有 Job 则返回原 Job；索引过期/未就绪则返回待核实）
curl -X POST "http://localhost:8080/namespaces/default/cronjobs/inspection-ny/catchup?at=2026-11-20T07:00:00Z"

# 对待核实记录重新回查
curl -X POST "http://localhost:8080/namespaces/default/cronjobs/inspection-ny/reverify?at=2026-11-20T07:00:00Z"

# 台账 / 索引状态
curl "http://localhost:8080/records"
curl "http://localhost:8080/job-indexes"
```

## 演练：断线 / 410 / 恢复 / 补跑（Swagger 可点选）

打开 <http://localhost:8080/docs>，按顺序点：

1. `POST /rehearsal/job-index/default/disconnect` —— 模拟 WATCH 断线；
2. `GET /namespaces/default/cronjobs/inspection-daily/latest-schedule?at=2026-11-20T01:30:00Z`
   —— `index_state=stale`、`index_status.synced_at` 为上次完整同步时间，已命中的 Job 仍返回；
   但若该轮没有 Job，`existing_job=null` **不能理解为“没有 Job”**；
3. `POST /namespaces/default/cronjobs/inspection-ny/catchup?at=2026-11-20T07:00:00Z`
   —— 返回 `pending_verification`，`index_stale=true`，不创建、不落台账；
4. `POST /rehearsal/job-index/default/expire` —— 模拟 `resourceVersion` 过期（410），索引再次 `stale`；
5. `POST /rehearsal/job-index/default/recover` —— 恢复 WATCH，重新 `LIST` 重建索引并回到 `ready`；
6. 再次 `POST .../inspection-ny/catchup?at=2026-11-20T07:00:00Z` —— 返回 `created`
   （Compose 默认的创建回包超时场景下，理由为“回查集群确认 Job 已存在”）；
7. 再点一次同一补跑 —— 返回 `returned_existing`，不重复创建；
8. `GET /rehearsal/mock-stats` —— 核对 `job_list` 只在初始/重建时增长，日常查询由 `job_watch` 增量维护；
9. `POST /rehearsal/job-index/default/not-ready?list_delay=15` —— 模拟首次索引未就绪；
   `latest-schedule` 返回 **503**（响应头 `X-Job-Index-State: initializing`），
   `catchup` 返回 `pending_verification`（`index_state=initializing`），都不把空结果当成无 Job；
   等待 LIST 完成或把 `list_delay` 调回 0 后自动恢复 `ready`。

等价 curl：

```bash
B=http://localhost:8080
curl "$B/namespaces/default/cronjobs/inspection-daily/latest-schedule?at=2026-11-20T01:30:00Z"
curl -X POST "$B/rehearsal/job-index/default/disconnect"
curl -X POST "$B/namespaces/default/cronjobs/inspection-ny/catchup?at=2026-11-20T07:00:00Z"
curl -X POST "$B/rehearsal/job-index/default/expire"
curl -X POST "$B/rehearsal/job-index/default/recover"
curl -X POST "$B/namespaces/default/cronjobs/inspection-ny/catchup?at=2026-11-20T07:00:00Z"
curl "$B/rehearsal/mock-stats"
```

### 模拟服务控制面（演练用）

| 接口 | 作用 |
|---|---|
| `POST /_mock/watch-disconnect` | 断开 WATCH 且暂不接受重连（客户端保留上次完整清单并标记过期） |
| `POST /_mock/expire-resource-version` | 让旧 `resourceVersion` 的 WATCH 返回 410（持续到 recover） |
| `POST /_mock/watch-recover` | 清除 410 / 恢复 WATCH，客户端重连后重新 LIST |
| `POST /_mock/list-delay?seconds=N` | 让 Job LIST 延迟 N 秒（演示首次索引未就绪） |
| `GET /_mock/stats` | LIST/WATCH/事件/410/断线计数与当前 rv |

## 测试

```bash
pytest
```

覆盖：cron 解析与 DST（纽约/柏林/上海/东京）、已有 Job 直接返回、补建成功、
暂停/窗口/时区/非每日表达式拦截、5 路并发只建一个、创建超时回查、查不清待核实、重启恢复查回；
以及 Job 实时索引：初始 LIST 后连续查询不再扫描、WATCH 增量命中其它轮次、
断线/410 期间带上一次完整索引与过期标记、过期期间补跑返回待核实、恢复后重建并继续补跑、
首次索引未就绪时 503 / 待核实。

## 已知边界 / 未完成项

- 鉴权仅支持 Bearer Token（`KUBE_TOKEN`），未做 mTLS 与 in-cluster ServiceAccount 自动挂载；
- 索引在进程内存中：多副本部署时每个副本各自 LIST/WATCH，互不共享；
- 并发锁是单进程内的 asyncio 锁，多副本部署时只有 SQLite 唯一约束兜底
  （多副本需换共享数据库或带 CAS 的外部锁）；
- 模拟服务只实现了本项目用到的 batch/v1 接口子集（WATCH 为轮询事件日志模拟，
  不是完整 apiserver 的 watch 缓存语义）；
- 未实现真实集群的集成测试（环境内无 Docker/集群），集成路径以模拟服务 + 真实 uvicorn 双进程验证。
