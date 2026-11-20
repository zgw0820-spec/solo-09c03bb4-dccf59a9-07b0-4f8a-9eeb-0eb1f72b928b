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
2. **按 (CronJob UID, 计划时刻) 匹配已有 Job**
   - 兼容 Kubernetes 1.32 的新注解 `batch.kubernetes.io/cronjob-uid` / `job-name`，
     也兼容旧的 `kubernetes.io/controller-uid` / `scheduled-at`；
   - 原调度器创建的 Job 与本服务补建的 Job 一视同仁。
3. **补跑**
   - 已有 Job：原样返回，不重复创建；
   - 仅当 **CronJob 仍处于暂停** 且 **到点后 2 小时内（按 UTC）** 才按 `jobTemplate` 补建 Job；
   - 同轮（同 CronJob + 同计划时刻）并发只允许建一个（进程内异步锁 + SQLite 唯一约束双保险）；
   - 补建 Job 带 `inspection.example.com/catchup=true` 标签和 UID/计划时刻注解。
4. **超时与可靠性**
   - 创建请求回包超时：先按 Job 名回查，再按 (UID, 计划时刻) 列查；
   - 回查能确认 → 正常返回创建结果；查不清 → 返回 `pending_verification`（HTTP 200），提示人工核实；
   - 补跑台账持久化在 SQLite，**进程重启后**创建到一半的记录自动转为待核实，可通过 `/reverify` 查回；
   - 超过 2 小时补跑期限后，计划与历史 Job/台账仍可查询，只是不能再新建。
5. **演练支持**：所有接口支持 `at` 查询参数（ISO 8601，可带时区），在 Swagger 上即可指定演练时间。

## 目录结构

```
app/
  schedule.py   # cron 解析、时区/DST 计算
  kube.py       # Kubernetes batch/v1 客户端、Job 匹配与清单构造
  store.py      # SQLite 台账 + 同轮并发锁
  service.py    # 补跑编排（校验、创建、超时回查、状态机）
  models.py     # Swagger 响应模型
  main.py       # FastAPI 入口
mock_k8s/
  server.py     # 本地 Kubernetes API 模拟（含故障注入）
tests/          # pytest：调度/DST、接口、并发、超时重试、重启恢复
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
# 查最近一轮计划与已有 Job
curl "http://localhost:8080/namespaces/default/cronjobs/inspection-daily/latest-schedule?at=2026-11-20T01:30:00Z"

# 补跑（有 Job 则返回原 Job）
curl -X POST "http://localhost:8080/namespaces/default/cronjobs/inspection-ny/catchup?at=2026-11-20T06:00:00Z"

# 对待核实记录重新回查
curl -X POST "http://localhost:8080/namespaces/default/cronjobs/inspection-ny/reverify?at=2026-11-20T06:00:00Z"

# 台账
curl "http://localhost:8080/records"
```

## 测试

```bash
pytest
```

覆盖：cron 解析与 DST（纽约/柏林/上海/东京）、已有 Job 直接返回、补建成功、
暂停/窗口/时区/非每日表达式拦截、5 路并发只建一个、创建超时回查、查不清待核实、重启恢复查回。

## 已知边界 / 未完成项

- 鉴权仅支持 Bearer Token（`KUBE_TOKEN`），未做 mTLS 与 in-cluster ServiceAccount 自动挂载；
- 并发锁是单进程内的 asyncio 锁，多副本部署时只有 SQLite 唯一约束兜底
  （多副本需换共享数据库或带 CAS 的外部锁）；
- 模拟服务只实现了本项目用到的 batch/v1 接口子集，不是完整 apiserver；
- 未实现真实集群的集成测试（环境内无 Docker/集群），集成路径以模拟服务验证。
