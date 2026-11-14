# 宽表 → 指标长表在线迁移（FastAPI + PostgreSQL）

把 `samples(id, temp, ph)` 宽表在线迁移为
`measurements(sample_id, metric, value)` 长表（每样本恰有 `temp`/`ph` 两项），
迁移全程不中断旧采集程序，最终切换后旧表名与旧列名仍然可用。

采用经典 **Expand → Migrate → Contract（扩展-迁移-收缩）** 模式，
新旧两个入口始终读写**同一份数据**，不使用任何定时同步。

---

## 1. 需求与实现对照

| 需求 | 实现方式 |
| --- | --- |
| 旧采集程序不中断 | 切换前 `samples` 一直是原表，直接 SQL 的 INSERT/UPDATE/DELETE 全程有效；行级双写触发器实时维护长表 |
| 回填时直接 SQL 修改不被旧扫描覆盖 | 回填按 id 水位推进，UPSERT 使用 `DO UPDATE SET value=EXCLUDED.value`；最新提交值永远胜出 |
| 删除不被复活、删除后复用 ID 不串数据 | 回填只扫描当前存在的行；删除经触发器级联清长表；复用的 id 作为新行按当前值写入 |
| 一次修改两项一起生效 | 所有写操作均在单事务内完成；触发器对一个样本整组写两行，约束失败整体回滚 |
| 只在最终切换时短暂阻塞写入 | 切换是唯一一次 `LOCK TABLE ... ACCESS EXCLUSIVE`，在单事务内完成校验与表→视图替换 |
| 切换失败继续提供旧接口 | 切换任一步失败/进程退出，事务回滚，`samples` 仍是原表、双写触发器完好，状态回到 `ready` |
| 进程中断重启可续办 | 回填水位 `last_id` 随批次事务提交；启动时自动把 `backfilling` 续跑、把残留 `switching` 回退为 `ready` |
| 无半迁移、不丢已提交修改 | DDL 引导与最终切换都是单事务；已提交数据在长表持久存在，崩溃只回滚未提交事务 |
| 切换后旧名称/列仍支持 SELECT 与增改删 | `samples` 变为同名视图（列仍为 `id,temp,ph`）+ `INSTEAD OF` 触发器把写下沉到长表 |
| 新旧入口读写同一份数据 | 切换前：表+触发器；切换后：视图+触发器。两个 HTTP 入口都经 `samples` 这一个写入面 |
| **作废样本在切换前移出在线库并留存原值** | 仅 `ready` 阶段受理作废单；单事务内 `FOR UPDATE` 锁定并读取已提交原值→写入归档表→删除（触发器级联清长表），在线两表同时消失、归档与在线同生共死 |
| **任一编号不存在则整单失败** | 事务内一次性 `SELECT ... FOR UPDATE` 锁定全部目标，缺任一 id 即抛错回滚，在线数据与归档表都不留任何痕迹 |
| **同单号重试返回原结果、不误伤复用 id 的新样本** | `pg_advisory_xact_lock(单号)` 串行化同号请求；单号唯一，已存在则原样回读首次留存（`reused=true`），不再执行任何删除 |
| **留存值对应真正删除的已提交版本** | 归档事务对样本行加 `FOR UPDATE`，旧采集程序并发的 UPDATE/DELETE 被阻塞到提交，归档读到的 temp/ph 与被删行是同一已提交版本 |
| 可启动迁移、查进度的网页 | 内置控制台 `/`，含阶段徽章、水位、一致性校验、增改删、崩溃演示与作废归档（提交/查询/列表）按钮 |
| Docker Compose 可实际启动 | `docker compose up` 一键拉起 Postgres 16 + API，自带 12 行虚构数据 |

---

## 2. 三阶段架构

```
Expand（应用启动时，单事务幂等）
  samples(表) ──AFTER ROW 触发器（双写）──▶ measurements(长表)
  + migration_state(id=1, state, last_id)

Migrate（后台批次回填，可暂停/续办/重启续跑）
  按 last_id 水位分批把宽表行展开为 (sample_id,'temp',..)、(sample_id,'ph',..)
  期间旧程序的直接 SQL 由触发器实时进长表，回填只补缺、绝不覆盖新值

Contract（最终切换，全程唯一写阻塞窗口，单事务）
  LOCK samples → 最终一致性校验 → DROP 表 → 建同名视图 + INSTEAD OF 触发器
  提交后：samples 是 measurements 的视图，两入口同写长表
```

状态机：
`init → backfilling ⇄ paused → ready → switching → switched`
（`switching` 失败/崩溃 → 回退 `ready`，可重试）

---

## 3. 目录结构

```
.
├── docker-compose.yml          # 主交付：db + api
├── docker-compose.demo.yml     # 演示覆盖（小批次、开启崩溃端点）
├── Dockerfile
├── requirements.txt
├── db/
│   ├── init/01_baseline.sql    # 旧世界：samples 表 + 12 行虚构数据（容器首次启动自动执行）
│   └── migrations/
│       ├── 02_expand.sql       # 长表、状态表、双写触发器（应用引导时执行）
│       ├── 03_contract.sql     # 同名视图 + INSTEAD OF 触发器（切换时执行）
│       └── 04_archive.sql      # 作废单头/留存明细表（应用引导时幂等执行）
├── app/
│   ├── main.py                 # FastAPI 路由（迁移控制 + 新旧两套入口 + 作废归档 + 网页）
│   ├── migration.py            # 回填引擎、状态机、一致性校验、原子切换
│   ├── archive.py              # 作废单归档：咨询锁串行化、行锁留存、整单事务
│   ├── crud.py / db.py / config.py / schemas.py
│   └── static/index.html       # 迁移控制台（原生 JS，无构建步骤）
├── demo/run_demo.sh            # 端到端演示脚本
└── tests/                      # pytest：迁移全生命周期 + 作废归档（共 18 用例）
```

---

## 4. 快速启动（Docker Compose）

```bash
docker compose up -d --build
# 打开控制台
open http://localhost:8000          # 或手动访问 http://localhost:8000
# 健康检查与进度
curl -s localhost:8000/health
curl -s localhost:8000/migration/progress
```

数据库连接：`postgresql://postgres:postgres@localhost:5432/warehouse`，
旧采集程序可直接连库对 `samples` 执行任意 SQL。

---

## 5. 网页与 API

### 迁移控制

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/migration/start` | 启动（或继续）后台回填 |
| POST | `/migration/pause` | 暂停回填，已提交批次保留 |
| POST | `/migration/resume` | 续办 |
| GET  | `/migration/progress` | 阶段、水位、行数、一致性校验 |
| POST | `/migration/switch` | 最终切换（唯一写阻塞窗口） |
| POST | `/migration/switch?hold_seconds=5` | 演示：持锁 5s 后回滚，模拟切换中崩溃 |

### 旧入口（宽表语义，切换前后均可写）

`GET/POST /samples`，`GET/PUT/DELETE /samples/{id}`，请求体 `{"temp": 22.5, "ph": 7.1}`

### 新入口（长表语义，按样本整组操作）

`GET /measurements`、`GET/DELETE /measurements/{sample_id}`、
`POST /measurements`、`PUT /measurements/{sample_id}`（整组替换两项）

### 作废样本归档（仅迁移阶段 `ready` 受理，最终切换前后均可查询）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/archive/orders` | 提交作废单 `{"order_no":"VOID-001","sample_ids":[2,5,9]}`；整单原子成功或一起回滚，返回留存的 temp/ph 原值 |
| GET | `/archive/orders` | 列出全部作废单（单号、状态、样本数、说明、时间） |
| GET | `/archive/orders/{order_no}` | 按单号查看每个被移走样本的留存原值 |

语义保证：

- **阶段闸门**：非 `ready` 返回 409，在线数据零变化；切换完成（`switched`）后同样拒绝新归档。
- **整单失败**：任一 `sample_id` 当前不存在（从未有过、已删除或已被其它单归档）返回 404，
  事务整体回滚，`samples`/`measurements`/归档表都不发生变化，失败单号不被占用、可修正后重试。
- **原值留存**：归档事务先 `SELECT ... FOR UPDATE` 锁定目标行并读取当前已提交 temp/ph，
  再写 `archive_samples` 并删除；旧采集程序的并发写被行锁阻塞，留存值即真正删除的那个版本。
- **两表同步消失**：删除走 `samples`（表+双写触发器，或切换后视图+INSTEAD OF），
  `measurements` 由触发器级联清理，在线库两个入口都查不到这些样本。
- **同单号幂等**：同单号并发/重复提交由事务级咨询锁串行化，首个事务提交后，
  后续请求直接回读首次结果（响应含 `"reused": true`），不再删除任何行；
  因此此后复用该 id 写入的新样本绝不会被误删。

### 崩溃演示

`POST /debug/crash`（仅 `ALLOW_CRASH_DEMO=1` 开启）：进程硬退出，
数据库侧等价 `kill -9`——未提交事务由 PostgreSQL 回滚，重启后续办。

---

## 6. 端到端演示

### Docker 环境

```bash
docker compose -f docker-compose.yml -f docker-compose.demo.yml up -d --build
# 进入 api 容器或在宿主机用 psql/HTTP 复现；网页按钮即可操作
```

### 脚本演示（需本机有 psql 与一个可连的 PostgreSQL）

`demo/run_demo.sh` 自动覆盖全部验收场景：

- A 启动回填 → 中途暂停（确认停在 `paused`）
- B 暂停期间旧程序**直接 SQL**：修改、删除、删除后复用 ID
- C 续办完成，验证不覆盖、不复活、复用 ID 恰好两项
- D 两项修改的原子性（NOT NULL 失败整体回滚）
- E 切换中退出：NOWAIT 探针证明排他锁存在 → 回滚 409 → 旧表完好可写
- E2 进程硬退出后重启，从已提交水位自动续办
- F 最终切换：`samples` 变视图，旧名旧列仍支持 SELECT/增改删
- F1 视图显式 id 写入后序列水位自动抬升
- F2 新旧 HTTP 入口读写同一份数据
- F3 全局不变量：每样本恰两项、逐值一致、零不匹配

```bash
BASE_URL=http://127.0.0.1:8000 \
PSQL="psql -h localhost -U postgres -d warehouse -v ON_ERROR_STOP=1 -tA" \
bash demo/run_demo.sh
```

无 Docker 的本机环境可用 `scripts/local_dev.sh`（见第 8 节）先拉起依赖再演示。

---

## 7. 关键正确性设计说明

**为什么回填不会覆盖旧程序的新修改？**
回填语句对冲突指标执行 `ON CONFLICT DO UPDATE SET value = EXCLUDED.value`，
而 `EXCLUDED.value` 来自扫描时刻宽表的当前值。若某行已被双写触发器更新为新值，
回填要么扫不到（已越过水位），要么用同样的当前值 UPSERT，旧值不可能回写。

**为什么删除不会复活？**
回填源是 `FROM samples ... WHERE id > 水位` 的实时查询，已删除行不在结果集中；
触发器的 DELETE 分支同时清理长表，没有任何机制会再把它插回。

**为什么复用 ID 不会串？**
复用的 id 对数据库就是一行新数据，触发器/回填只认当前 `(id, temp, ph)`，
整组重写该 id 的两项指标，与它的“前世”无任何关联。

**为什么不会半迁移？**
- 引导（建长表/触发器）与最终切换（锁表、校验、换视图）各自是**一个事务**；
- 回填按批提交，水位与本批数据同事务落库；
- 进程崩溃只回滚未提交事务，重启后从水位继续，或把残留 `switching` 退回 `ready`。

**切换后 id 如何分配？**
原 identity 序列随表删除，切换事务重建 `samples_id_seq` 并对齐到最大 id；
视图 INSERT 触发器在 `NEW.id IS NULL` 时取序列，显式 id 写入时抬升序列水位。

**作废归档如何保证“留存值=真正删除的已提交版本”？**
归档在一个事务里先对全部目标样本 `SELECT ... FOR UPDATE`：旧采集程序并发的
UPDATE/DELETE 会被这把行锁阻塞到归档事务结束，因此归档读到的 temp/ph 与随后
DELETE 掉的是同一行、同一个已提交版本，并发写不可能在留存与删除之间插入新版本。

**作废归档如何与最终切换互斥？**
归档事务对 `migration_state` 单行取 `FOR UPDATE` 并校验状态为 `ready`；最终切换
也要在同一事务里更新这行状态，二者经行锁互斥。归档进行中切换会等待，切换完成后
状态为 `switched`，新归档请求读到非 `ready` 直接 409 拒绝。

**同单号重试为何不会误删复用 id 的新样本？**
`archive_orders.order_no` 有唯一约束，且事务先取按单号派生的事务级咨询锁。
首个事务提交后，同号的后来者在同事务内复查到该单号已存在，立即回读首次留存明细
（`reused=true`）并返回，**不再执行任何 SELECT/DELETE**；之后即便某个 id 被删除并
重新写入新样本，也与该历史单号完全无关，不会被触碰。

**作废单中途失败会留下半成品吗？**
不会。咨询锁、阶段校验、行锁、归档写入、样本删除全部在一个数据库事务内；任何一步
抛错（缺 id、约束违反、进程被杀）都会整体回滚——归档表无残留，在线 `samples` 与
`measurements` 也原样保留，失败单号可在修正后重新提交。

---

## 8. 无 Docker 的本机运行（可选）

`scripts/local_dev.sh` 演示如何用本机 Python venv + 任意 PostgreSQL 运行：

```bash
# 1) 建库并加载基线
psql -d postgres -c "CREATE DATABASE warehouse;"
psql -d warehouse -f db/init/01_baseline.sql
# 2) 起 API
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL="postgresql://USER@/warehouse?host=/var/run/postgresql"
export BACKFILL_BATCH_SIZE=2 BACKFILL_IDLE_SECONDS=0.4 ALLOW_CRASH_DEMO=1
uvicorn app.main:app --port 8000
```

---

## 9. 自动化测试

```bash
# TEST_DATABASE 指向一个可创建的测试库；每用例自动 DROP/CREATE SCHEMA 重置
TEST_DATABASE=warehouse_pytest \
ADMIN_DATABASE_URL="postgresql:///postgres?host=%2Fvar%2Frun%2Fpostgresql&user=postgres" \
pytest tests/ -v
```

`tests/` 共 18 个用例。迁移 7 个：初始进度、回填完成且每组两项、暂停续办、
直接 SQL 改/删/复用 ID 的存活、两项原子更新、失败切换回滚保旧接口、
成功切换后视图与双入口同数据。作废归档 11 个：非 ready 拒绝（切换前后）、
非法请求体、缺 id 整单失败且零副作用、成功归档留存并两表消失、同号重试幂等且
保护复用 id 的新样本、FOR UPDATE 行锁挡住并发写、失败整体回滚、归档后切换仍成功、
单内去重、咨询锁同号互斥/异号并行。

> 注：并发用例采用 `FOR UPDATE NOWAIT` 与 `pg_try_advisory_xact_lock` 等
> **非阻塞探针**断言锁语义，避免依赖操作系统对阻塞等待后端的唤醒时序。
