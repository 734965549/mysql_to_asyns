# MySQL-to-Async 数据同步工具



## 项目简介



MySQL-to-Async 是一个高性能的 MySQL 数据同步工具，支持全量同步、增量同步以及全量+增量组合同步模式。该工具采用领域驱动设计（DDD）架构，提供 RESTful API 接口和 Web 管理界面，支持任务的创建、启动、暂停、监控等全生命周期管理。



## 核心能力



- **全量数据迁移**：支持千万级数据高效迁移

- **实时增量同步**：基于 Binlog 的实时数据捕获，延迟小于1秒

- **断点恢复**：全量中断后拒绝续传并要求重新准备目标端；增量阶段通过 Binlog 位点（Redis/内存）恢复

- **无主键表同步**：智能识别策略，全列匹配保障数据一致性

- **Web 管理界面**：可视化任务管理，实时监控同步进度

- **只读保护机制**：同步期间自动锁定目标库，防止数据冲突

- **多目标写入**：增量同步支持同时写入 MySQL、Kafka、HTTP Webhook



## 性能指标



| 指标 | 数值 | 说明 |

|------|------|------|

| 全量同步速度 | 50,000+ rows/s | 批量插入，4核8G配置 |

| 增量同步延迟 | < 1s | Binlog实时订阅 |

| 支持数据量 | 1亿+ | 已验证生产环境 |

| 并发任务数 | 10+ | 视服务器配置而定 |



## 功能特性



### 核心功能



- **多种同步模式**：支持全量同步(FULL)、增量同步(INCREMENTAL)、全量+增量(ALL)

- **断点恢复**：
  - **全量**：全量阶段暂停/失败后不再续传；同一旧任务需开启 DDL 前删除后重新全量，或人工清理目标端后创建/重置任务从头跑
  - **增量**：基于 Redis/内存 checkpoint 保存 Binlog 位点，服务重启后续订

- **实时监控**：提供任务进度、状态、指标等实时监控

- **Binlog 订阅**：基于 MySQL Binlog 的实时增量数据捕获

- **批量处理**：支持批量数据读写，提高同步效率

- **无主键表支持**：针对无主键表提供全列匹配策略

- **审计日志**：记录所有同步操作的审计日志

- **只读保护**：同步期间自动设置目标库只读模式

- **密码加密存储**：任务配置中的数据库密码使用 AES-256-GCM 加密后持久化，防止明文泄露
- **任务元数据字段对齐**：任务存储表 `sys_sync_tasks` 建议包含 `pk_id`、`created_at`、`updated_at` 等字段，并由 DBA 按升级脚本统一执行结构升级，兼容旧数据库结构



### 高级特性



- **智能表识别**：自动识别主键、唯一键，选择最优同步策略

- **数据一致性保障**：支持INSERT、UPDATE、DELETE事件同步

- **自定义数据库配置**：每个任务可配置独立的源和目标数据库

- **任务级 Runtime 隔离**：每个任务独立维护 source/target DB、analyzer、readOnlyManager，支持并发启动

- **库级别同步**：支持整个数据库的批量同步

- **表级别同步**：支持指定表的精确同步



## 快速开始



### 环境要求



- Go 1.24+

- MySQL 5.7.5+ / 8.0+ (需开启Binlog；V2 多 schema `GET_LOCK` 要求 ≥ 5.7.5)

- Redis 6.0+ (推荐)

- Node.js 18+ (前端开发，可选)



### MySQL配置要求



确保源MySQL已启用Binlog：



``sql

-- 检查Binlog是否启用

SHOW VARIABLES LIKE 'log_bin';



-- 检查Binlog格式（必须为ROW）

SHOW VARIABLES LIKE 'binlog_format';



-- 检查binlog_row_image（建议为FULL）

SHOW VARIABLES LIKE 'binlog_row_image';

``



MySQL配置文件 (my.cnf/my.ini)：



`ini

[mysqld]

log-bin=mysql-bin

binlog_format=ROW

binlog_row_image=FULL

server-id=1

expire_logs_days=7

`



### 数据库权限要求



``sql

GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'username'@'%';

GRANT SELECT ON *.* TO 'username'@'%';

# ALL/FULL V2 多连接对齐均需要 RELOAD（或 FLUSH_TABLES）；缺权限立即失败，不再自动降级

GRANT RELOAD ON *.* TO 'username'@'%';

FLUSH PRIVILEGES;

```



### 安装与配置



#### 1. 下载源码



``bash

git clone https://github.com/yourusername/mysql-to-sync.git

cd mysql-to-sync

``



#### 2. 配置文件



编辑 etc/application.toml：



``toml

[http]

  host = "0.0.0.0"

  port = 8081



[datasource]

  provider = "mysql"

  host = "192.168.1.100"

  port = 3306

  database = "source_db"

  username = "root"

  password = "password"

  debug = false



[target]

  host = "192.168.1.101"

  port = 3306

  database = "target_db"

  username = "root"

  password = "password"



[redis]

  host = "127.0.0.1"

  port = 6379

  password = ""

  db = 0



[security]
  # 任务密码加密密钥（AES-256），建议 32 字节；留空则不加密
  # 也可通过环境变量 MYSQL_TO_ASYNC_SECURITY_ENCRYPT_KEY 设置
  encrypt_key = "your-32-byte-secret-key-here!!!!"

[log]

  level = "info"

  [log.console]

    enable = true

  [log.file]

    enable = true

``



#### 3. 启动服务



**方式一：直接运行**



``bash

go run main.go

``



**方式二：编译后运行**



``bash

# Linux/macOS

go build -o mysql-to-sync

./mysql-to-sync



# Windows

go build -o mysql-to-sync.exe

.\mysql-to-sync.exe

``



#### 4. 启动Web界面（可选）



``bash

cd web

npm install

npm run dev

``



访问 http://localhost:5173 打开Web管理界面。



#### 5. Docker Compose 一键启动（前后端 + 依赖）



`Dockerfile` 已支持多阶段多目标构建：



- `backend`：Go 后端服务（监听 `8080`）；

- `frontend`：Nginx 托管前端 `npm run build` 产物，并反向代理 `/api` 到后端。



启动命令：



```bash

docker compose up -d --build

```



访问地址：



- 前端页面：`http://localhost`

- API 入口（通过 Nginx 代理）：`http://localhost/api`

- 后端容器内部端口：`app:8080`（compose 网络内）



## API 接口文档



服务启动后，默认监听端口 8080。API基础路径：http://localhost:8080/api



### 任务管理



#### 创建任务



```bash
POST /api/tasks
Content-Type: application/json
```

**请求示例：**

```json
{
  "name": "用户表同步",
  "mode": "ALL",
  "sync_level": "TABLE",
  "source_schema": "production",
  "target_schema": "production_backup",
  "tables": ["users", "orders"],
  "target_tables": ["users_archive", "orders_archive"],
  "batch_size": 1000,
  "worker_count": 4,
  "intra_table_worker_count": 8,
  "enable_limit_one": false,
  "optimize_index": true,
  "enable_read_only": true,
  "enable_drop_table_before_ddl": true,
  "enable_skip_binlog": false,
  "source_db": {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "production",
    "username": "root",
    "password": "root_password"
  },
  "target_db": {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "production_backup",
    "username": "root",
    "password": "root_password"
  },
  "sink_configs": [
    {
      "type": "MYSQL"
    }
  ]
}
```



**请求参数说明：**



| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name | string | 是 | 任务名称 |
| mode | string | 是 | 同步模式：FULL/INCREMENTAL/ALL |
| sync_level | string | 是 | 同步级别：DATABASE(库级别)/TABLE(表级别) |
| source_schema | string | 是 | 源数据库名 |
| target_schema | string | 是 | 目标数据库名 |
| tables | array | 表级别必填 | 要同步的表列表 |
| batch_size | int | 否 | 批量大小，默认1000 |
| worker_count | int | 否 | 工作线程数，默认4 |
| tx_commit_every_n_parallel | int | 否 | 并行 worker 每 N 批提交一次事务；0 表示使用默认值 5。减小可降低锁等待，增大可减少 fsync 频率提高吞吐 |
| enable_limit_one | bool | 否 | 无主键表LIMIT 1保护，默认false |
| enable_drop_table_before_ddl | bool | 否 | 开启后按同步级别删除目标：DATABASE 级别先 `DROP DATABASE IF EXISTS` + `CREATE DATABASE` 重建目标库；TABLE 级别在每张表建表前执行 `DROP TABLE IF EXISTS`。默认false |
| enable_skip_binlog | bool | 否 | 全量同步写入前在目标端临时关闭 sql_log_bin（`SET SESSION sql_log_bin=0`），写入后恢复；可加速全量导入并减少目标 binlog 体积。需目标库账号具备 SUPER 权限。默认false |
| index_restore_worker_count | int | 否 | 索引回放表级并发度，0=自动推导 min(worker_count,4)，默认0 |
| sink_configs | array | 否 | 增量同步目标 Sink 配置列表，不传默认 `[{type:MYSQL}]`，详见下方「Sink 配置说明」 |
| allow_nopk_all | bool | 否 | ALL 模式下含无 PK/UK 表时必须显式设为 `true` 以确认 best-effort 风险（不保证严格去重），否则任务被拒绝。默认 false |



**Binlog 位点说明：**

- **FULL 模式**不捕获 binlog 位点、不保存增量 checkpoint。FULL 只做一次无缝全表遍历，保证分片边界与读取流程不漏数据；同步期间发生的新增、更新和删除不进行追平。

- **ALL 模式**在全量扫描开始前，以无锁方式执行 `SHOW MASTER STATUS` 取全局 binlog 位点（P0），不再使用 `FLUSH TABLES WITH READ LOCK`。P0 捕获或持久化失败时任务立即终止。全量基线扫描完成后再捕获一次位点 P1，随后从 P0 回放 binlog 做有界追平（bounded catch-up）到 P1，追平后恢复非 identity 索引并进入持续同步。无 PK/UK 表的 ALL 需任务请求体显式确认 `allow_nopk_all=true`（best-effort，不保证严格去重）。

- **V1 全量（默认 `full_load_engine=v1`）**：全量读取为普通短查询，不长期持有源库 MDL。ALL + 无 PK/UK 表为 best-effort，增量接管阶段存在重复 INSERT 风险，需显式确认 `allow_nopk_all=true`。

- **V2 全量（`full_load_engine=v2`）**：全量读取同样为普通短查询，不持有表级 MDL，不再使用长生命周期 RR 快照事务；JSON/TEXT/BLOB 主键表默认使用按实际行字节数自适应的直接 keyset 查询，不再自动生成 `WHERE id IN (...)`，也不会关闭未消费完的 payload 结果后重复拉取。只有显式设置 `full_load_two_phase_read=true` 才启用两阶段读取。历史版本的表级 binlog HWM（`table_binlog_hwms`）已下线，仅保留用于读取旧任务存档，新一轮全量开始时清空。ALL + 无 PK/UK 表同样为 best-effort，依赖 P0..P1 有界追平收敛，不保证严格去重，需 `allow_nopk_all=true`。写入侧在每个目标 schema 自动创建 `__mts_fl_tx` 事务标记表（与业务 INSERT 同事务提交，含 `run_id`），用于 Commit 结果未知时的锁定探测，避免业务行存在性误判；启动前以 `GET_LOCK` 强制同 schema 单任务，并 fail-closed 校验目标业务表 InnoDB、marker 表结构（含完整唯一索引），拒绝业务表占用保留名 `__mts_fl_tx`；数据流水线成功后在仍持有 schema 互斥锁时执行 `DROP TABLE IF EXISTS __mts_fl_tx`；清理失败会让任务明确失败，失败/暂停路径保留 marker。目标账号需对 marker 表具备 `CREATE TABLE`/`INSERT`/`SELECT ... FOR UPDATE`/`DROP`，以及 `GET_LOCK`/`RELEASE_LOCK`。

- 历史版本曾提供 `enable_consistent_snapshot` 任务级字段，现已下线。如果客户端代码仍在传该字段，请直接删除，服务端会忽略。

**ALL 模式运行状态字段（任务详情接口返回）：**

| 字段 | 含义 |
|------|------|
| `full_sync_start_position` | 全量基线扫描开始前的 binlog 位点 P0（`file:pos`） |
| `full_sync_end_position` | 基线扫描完成后的 binlog 位点 P1（bounded catch-up 目标） |
| `full_sync_catchup_position` | catch-up 阶段当前已提交位点，逐步逼近 `full_sync_end_position` |
| `full_sync_subphase` | ALL 全量子阶段：`BASE_SCAN`（基线扫描）/ `CATCH_UP`（有界追平）/ `RESTORE_INDEX`（恢复非 identity 索引）/ `STREAMING`（持续增量） |
| `nopk_all_risk_acknowledged_at` | 用户确认 ALL 无 PK/UK best-effort 风险的时间戳（服务端写入，真正生效以此为准） |

**`enable_drop_table_before_ddl` 说明：**

- 该参数属于任务请求体字段（`POST /api/tasks`、`PUT /api/tasks/:id`）。JSON 字段名、默认值、存储结构不变，无迁移要求。

- 删除/重建行为按 `sync_level` 分支执行，且仅在全量阶段（`MarkFullSyncStarted` 之后、任何目标表 DDL/数据写入前）执行一次；增量阶段或已跳过全量阶段时不执行删除：

  - **DATABASE 级别**：对去重后的每个唯一目标库依次执行 `DROP DATABASE IF EXISTS \`目标库\`` 与 `CREATE DATABASE IF NOT EXISTS \`目标库\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`（使用用户配置的目标库名，`TargetDatabases`/`TargetSchema` 为空才回退源名）。任一步失败立即终止全量同步并标记 `FULL_FAILED`。库级重建完成后，建表阶段不再逐表执行 `DROP TABLE`（目标库已为空）。多个源库映射到同一目标库时，该目标库只重建一次。
  - **TABLE 级别**：保持原有行为，在每张表执行 `CREATE TABLE ... LIKE ...` 或回放 `SHOW CREATE TABLE` 之前，先执行 `DROP TABLE IF EXISTS \`目标库\`.\`目标表\``（使用用户配置的目标库/目标表名，`TargetTables` 为空才回退源表名）。

- 注意：开启后会删除目标库/目标表及其数据，请确认目标端允许覆盖。按用户选择，不新增"源目标同实例同名库"保护；配置如此映射时仍会执行删除。

- 写入与重启副作用：全量阶段统一使用普通 `INSERT`（无 `IGNORE`、无 upsert），并会使用批量导入会话优化。目标端必须由用户保证为空，或开启 `enable_drop_table_before_ddl=true` 由程序重建为空；目标端非空属于不支持场景，可能失败或污染目标数据。全量阶段暂停/失败后不再续传；若未开启 DDL 前删除，同一旧任务再次启动会被拒绝，需要开启该开关重建后重新全量，或人工清理目标端后创建/重置任务从头跑。增量阶段仍通过 checkpoint 恢复。



**Sink 配置说明（sink_configs）：**

`sink_configs` 用于配置增量同步阶段的数据写入目标。支持三种类型：`MYSQL`、`KAFKA`、`HTTP_WEBHOOK`。不传时默认 `[{type: "MYSQL"}]`。

**约束：**
- 仅 `mode=INCREMENTAL` 支持非 `MYSQL` 类型的 Sink。`FULL`/`ALL` 模式含非 MYSQL Sink 时任务拒绝启动。
- 多个 Sink 采用 At-Least-Once 语义：所有 Sink 写入成功后才推进 binlog checkpoint，任一 Sink 失败则任务标记 FAILED。
- 密码/密钥字段（SASL 密码、Webhook headers）持久化时自动使用 AES-256-GCM 加密存储。
- 任务创建、详情、列表和更新响应中的数据库密码及 Sink 密钥统一返回 `******`；更新时原样提交该占位符会保留已有密钥。

**KAFKA options：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| brokers | []string | 是 | - | Kafka broker 地址列表 |
| topic | string | 是 | - | Topic 名称（routing_mode=single_topic 时使用） |
| routing_mode | string | 否 | single_topic | Topic 路由模式：`single_topic`（固定 topic）/ `per_table`（按 `{prefix}.{schema}.{table}` 命名） |
| topic_prefix | string | 条件必填 | - | `routing_mode=per_table` 时必填的 topic 前缀 |
| key_mode | string | 否 | pk | 消息 key 生成方式：`pk`（主键值拼接）/ `none`（`schema.table:binlog_pos`） |
| batch_size | int | 否 | 1000 | 批次大小 |
| batch_timeout_ms | int | 否 | 500 | 批次超时（毫秒） |
| required_acks | int | 否 | 1 | 发送确认级别（0/1/-1=all） |
| security.sasl_mechanism | string | 否 | - | SASL 认证机制：`PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512` |
| security.sasl_username | string | 否 | - | SASL 用户名 |
| security.sasl_password | string | 否 | - | SASL 密码（持久化加密） |
| security.tls_enabled | bool | 否 | false | 是否启用 TLS |
| security.ca_cert_path | string | 否 | - | CA 证书文件路径 |
| security.client_cert_path | string | 否 | - | 客户端证书文件路径（mTLS） |
| security.client_key_path | string | 否 | - | 客户端密钥文件路径（mTLS） |
| security.insecure_skip_verify | bool | 否 | false | 是否跳过证书校验 |

**HTTP_WEBHOOK options：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| url | string | 是 | - | Webhook 目标 URL |
| method | string | 否 | POST | HTTP 请求方法 |
| timeout_ms | int | 否 | 3000 | 请求超时（毫秒） |
| headers | map[string]string | 否 | - | 自定义 HTTP 头（持久化加密） |
| retry_times | int | 否 | 3 | 失败重试次数（非 2xx 触发重试） |
| retry_backoff_ms | int | 否 | 500 | 重试退避间隔（毫秒） |

**消息体格式（统一 JSON）：**

```json
{
  "task_id": "task_001",
  "source_schema": "db1",
  "source_table": "orders",
  "event_type": "UPDATE",
  "event_time": "2026-07-14T10:00:00+08:00",
  "binlog_file": "mysql-bin.000001",
  "binlog_pos": 12345,
  "primary_keys": { "id": 42 },
  "before": { "id": 42, "status": 0 },
  "after": { "id": 42, "status": 1 }
}
```



#### 更新任务



```bash
PUT /api/tasks/:id
Content-Type: application/json
```

**请求示例：**

```json
{
  "name": "用户表同步-调整版",
  "mode": "ALL",
  "sync_level": "TABLE",
  "source_schema": "production",
  "target_schema": "production_backup",
  "tables": ["users", "orders"],
  "target_tables": ["users_archive", "orders_archive"],
  "batch_size": 2000,
  "worker_count": 8,
  "intra_table_worker_count": 12,
  "enable_limit_one": false,
  "optimize_index": true,
  "enable_read_only": true,
  "enable_drop_table_before_ddl": true,
  "enable_skip_binlog": false,
  "source_db": {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "production",
    "username": "root",
    "password": "root_password"
  },
  "target_db": {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "production_backup",
    "username": "root",
    "password": "root_password"
  }
}
```

**说明：**

- 更新任务时，请把需要保留的字段一并传入，后端按请求体覆盖对应配置。
- `enable_drop_table_before_ddl=true` 时，任务在同步前按 `sync_level` 删除目标：DATABASE 级别重建目标库（`DROP DATABASE` + `CREATE DATABASE`），TABLE 级别在每张表建表前执行 `DROP TABLE IF EXISTS`。
- 如果只想切换该开关，可以只传该字段和任务标识相关的必要字段，但建议前端保存完整配置，避免遗漏。



#### 获取所有任务



```bash

GET /api/tasks

```



#### 获取任务详情



```bash

GET /api/tasks/:id

```



#### 启动任务



```bash
POST /api/tasks/:id/start
```

**请求体说明：**

- 不传请求体：立即启动任务。
- 传入 `scheduled_at`：设置单次定时启动。
- 传入 `repeat_count` 和 `repeat_interval_sec`：设置重复定时启动。
- 传入 `schedule_mode: "cron"` 与 `cron_expression`：按 Cron 表达式周期启动（实际触发时间由后端计算）。

**请求示例 1：立即启动**

```bash
curl -X POST http://localhost:8080/api/tasks/任务ID/start
```

**请求示例 2：单次定时启动**

```json
{
  "scheduled_at": "2026-06-04T18:30:00+08:00"
}
```

**请求示例 3：重复定时启动**

```json
{
  "scheduled_at": "2026-06-04T18:30:00+08:00",
  "repeat_count": 3,
  "repeat_interval_sec": 60
}
```

**请求示例 4：Cron 定时启动**

```json
{
  "scheduled_at": "2026-06-12T20:00:00+08:00",
  "schedule_mode": "cron",
  "cron_expression": "0 1 * * 6",
  "cron_timezone": "Asia/Shanghai"
}
```

Cron 模式下 `scheduled_at` 仅作基准时间，**下次执行时刻由 `cron_expression` 计算**；不会因请求处理耗时触发「时间早于当前时间」校验失败。

**参数说明：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| scheduled_at | string | 定时启动时必填 | 首次/基准启动时间，RFC3339 格式 |
| schedule_mode | string | 否 | `cron` 表示 Cron 周期调度 |
| cron_expression | string | Cron 模式必填 | 标准五段 Cron，如 `0 1 * * 6` |
| cron_timezone | string | 否 | 时区，默认本地 |
| repeat_count | int | 否 | 重复启动总次数，包含首次执行；不传或传 0 表示仅执行一次 |
| repeat_interval_sec | int | 否 | 每次执行完成后到下一次启动的间隔秒数，默认 0 |

**说明：**

- 单次/重复定时模式下，`scheduled_at` 不能早于当前时间；Cron 模式不受此限制。
- `repeat_count` 必须大于等于 0。
- `repeat_interval_sec` 必须大于等于 0。
- 重复启动会在每次任务执行完成后，自动安排下一次启动，直到达到 `repeat_count`。

#### 暂停任务



```bash

POST /api/tasks/:id/pause

```

全量进行中暂停后 `sync_phase` 保持为 `FULL_STARTED`。再次调用启动接口不会续传全量；未开启「DDL 前 DROP TABLE」时会被拒绝。同一旧任务需开启该开关重建后重新全量，或人工清理目标端后创建/重置任务从头跑。若任务已完成全量并进入增量阶段，再启动会从增量 checkpoint 继续。

#### 结束任务（仅 ALL 模式增量阶段）

```bash

POST /api/tasks/:id/end

```

将一个持续运行的 `ALL` 任务在增量阶段手动结束，进入新的终态 `STOPPED`。仅在同时满足以下条件时允许：

- `status=RUNNING`
- `mode=ALL`
- `sync_phase=INCREMENTAL_STARTED`

结束为终态操作：原任务**不能**再次启动、编辑或设置定时调度；仍允许查看、行数对比、复制新建和删除。结束采用「立即优雅停止」语义——完成当前正在退出的处理和资源关闭，但不捕获新的结束位点，也不等待持续产生的源端写入全部追平。`FULL` 等一次性任务自然完成仍走 `COMPLETED`，不通过此接口。

返回：`200` 成功；`404` 任务不存在；`409` 模式/阶段/状态不允许结束。人工结束不计入 `mysql_sync_tasks_completed` 指标（该指标仍只统计 `COMPLETED`）。

#### 行数对比（手动触发）

```bash

POST /api/tasks/:id/row-count-comparison

```

在任务结束/完成后由用户手动触发，后台精确统计源端和目标端逐表 `COUNT(*)`，结果随任务存档持久化并在详情页展示。接口只负责校验并启动后台任务，成功返回 `202 Accepted`。允许条件：

- 任务状态为 `COMPLETED` 或 `STOPPED`
- 任务模式为 `FULL` 或 `ALL`
- `HasFullSyncEverCompleted()` 为真（`sync_phase` 为 `FULL_COMPLETED` 或 `INCREMENTAL_STARTED`）
- 目标端为 MySQL

同一任务已存在 `CHECKING` 核对时再次请求返回 `409`。核对结束后允许重新执行，新结果覆盖旧结果，不保存历史版本。**行数对比不会由同步完成流程自动触发，也不会因不一致而修改原任务的 `COMPLETED` 或 `STOPPED` 状态**。

汇总状态：`CHECKING`（进行中）/ `MATCHED`（全部一致）/ `MISMATCHED`（至少一张表不一致）/ `PARTIAL`（部分表查询失败）/ `FAILED`（连接或表清单获取失败，或所有表均无法完成核对）。`difference` 统一定义为「目标端行数减源端行数」。服务重启时若存档仍为 `CHECKING`，会自动改为 `FAILED` 并注明服务重启导致核对中断。

> ⚠️ 结果为点击核对期间获得的行数快照。源端在任务结束后仍可能继续写入，因此这不是事务一致性证明，仅用于粗略核对。

#### 取消定时启动

```bash

POST /api/tasks/:id/cancel-schedule

```

仅当任务状态为 `SCHEDULED` 时可取消，恢复为设置定时前的状态。



#### 删除任务



```bash

DELETE /api/tasks/:id

```



#### 获取任务指标



```bash

GET /api/tasks/:id/metrics

GET /api/tasks/:id/events

GET /api/tasks/:id/event-executions

```

**任务事件 API**（`GET /api/tasks/:id/events`）支持查询参数：

- `execution_id` / `after_seq` / `before_seq` / `limit`
- `min_severity`（INFO/WARN/ERROR）/ `visibility`（KEY/DIAGNOSTIC）
- `category` / `code` / `source_schema` / `source_table`

任务详情页「日志与错误」页签默认展示当前 execution 的 KEY 事件（最新在前），运行中每 3 秒增量轮询。

**关键事件码索引**（`visibility=KEY`，不含 5 秒 progress 等 DIAGNOSTIC 事件）：

| 分类 | 事件码 | 说明 |
|------|--------|------|
| 生命周期 | `TASK_SCHEDULED` / `TASK_STARTED` / `TASK_RESUMED` / `TASK_PAUSED` / `TASK_STOPPED` / `TASK_COMPLETED` / `TASK_FAILED` | 任务调度与状态变更 |
| 配置 | `TASK_CONFIG_EFFECTIVE` / `FULL_LOAD_CONFIG_EFFECTIVE` | 每轮启动/V2 池 cap 后的生效配置 |
| 阶段 | `PHASE_DDL_PREP_*` / `PHASE_P0_CAPTURED` / `PHASE_BASE_SCAN_*` / `PHASE_P1_*` / `PHASE_CATCHUP_*` / `PHASE_INDEX_RESTORE_*` / `PHASE_INCREMENTAL_STARTED` | ALL/FULL 各阶段起止 |
| 表规划 | `TABLE_PLAN_CREATED` / `TABLE_ESTIMATE_FAILED` / `TABLE_CHUNK_PLAN_FALLBACK` / `TABLE_PARALLELISM_REDUCED` / `NOPK_SEQUENTIAL_FALLBACK` | 分片规划与降级 |
| 表进展 | `TABLE_NO_PROGRESS` / `TABLE_PROGRESS_RECOVERED` | 表长时间无读进展及恢复 |
| 宽表/拆批 | `WIDE_TABLE_ADAPTIVE_WINDOW_ENABLED` / `WIDE_TABLE_TWO_PHASE_ENABLED` / `ROW_EXCEEDS_BATCH_BYTES` | 宽表自适应 keyset、显式两阶段读、单行超 batch_bytes |
| 连接池 | `SOURCE_POOL_BUDGET_CAPPED` / `SOURCE_POOL_WAIT_HIGH` / `TARGET_POOL_BUDGET_CAPPED` | 池上限裁剪与等待压力 |
| 队列背压 | `QUEUE_BACKPRESSURE_HIGH` / `QUEUE_BACKPRESSURE_RECOVERED` | 写队列高水位与恢复 |
| 重试 | `WRITE_LOCK_RETRY` / `TABLE_READ_RETRY*` / `TX_COMMIT_*` / `TX_REPLAY_*` | 写锁、读重试、提交不确定与重放 |
| Staging | `STAGING_TABLE_CREATED` / `STAGING_TABLE_PUBLISHED` / `STAGING_TABLE_DROPPED` | V2 staging 表生命周期 |
| 其它 | `SLOW_SOURCE_QUERY` / `SCHEMA_LOCK_LOST` / `INDEX_RESTORE_FAILED` / `CHECKPOINT_PERSIST_FAILED` | 慢查询与任务级故障 |

同一 `(code, 表, details 指纹)` 在 60 秒内重复出现会聚合为 `*_REPEATED` 汇总事件（`repeat_count` / `first_at` / `last_at`）。删除任务会联动清理该任务全部事件。

**响应示例：**



```json

{

  "processed_rows": 12345,

  "total_rows": 50000,

  "estimated_total_rows": 50000,

  "progress_percent": 24.69,

  "status": "RUNNING",

  "current_position": "users:15000",

  "binlog_file": "mysql-bin.000003",

  "binlog_pos": 4567,

  "lag": 0

}

```



### 元数据接口



#### 获取数据库列表



```bash

GET /api/metadata/databases

```



#### 获取表列表



```bash

GET /api/metadata/tables?schema=database_name

```



#### 获取表标识信息



```bash

GET /api/metadata/identity?schema=database_name&table=table_name

```



#### 刷新元数据



```bash

POST /api/metadata/refresh

```



### 配置接口



#### 获取默认配置



```bash

GET /api/config/default

```



## 使用示例



### 示例1：全量同步



```bash

curl -X POST http://localhost:8080/api/tasks \

  -H "Content-Type: application/json" \

  -d '{

    "name": "全量迁移订单数据",

    "mode": "FULL",

    "sync_level": "TABLE",

    "source_schema": "production",

    "target_schema": "backup",

    "tables": ["orders", "order_items"],

    "batch_size": 2000,

    "worker_count": 16,

    "intra_table_worker_count": 16

  }'

```



### 示例2：增量同步



```bash

curl -X POST http://localhost:8080/api/tasks \

  -H "Content-Type: application/json" \

  -d '{

    "name": "实时同步用户表",

    "mode": "INCREMENTAL",

    "sync_level": "TABLE",

    "source_schema": "production",

    "target_schema": "production_slave",

    "tables": ["users", "user_profiles"],

    "batch_size": 1000

  }'

``



### 示例3：全量+增量组合



``bash

curl -X POST http://localhost:8080/api/tasks \

  -H "Content-Type: application/json" \

  -d '{

    "name": "完整数据同步",

    "mode": "ALL",

    "sync_level": "DATABASE",

    "source_schema": "production",

    "target_schema": "production_replica",

    "source_databases": ["production"],

    "target_database": "production_replica",

    "batch_size": 1000

  }'

``



### 示例4：增量同步到 Kafka

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "CDC投递到Kafka",
    "mode": "INCREMENTAL",
    "sync_level": "TABLE",
    "source_schema": "production",
    "target_schema": "production_slave",
    "tables": ["orders", "users"],
    "batch_size": 1000,
    "sink_configs": [
      {
        "type": "KAFKA",
        "options": {
          "brokers": ["127.0.0.1:9092"],
          "topic": "mysql_cdc",
          "routing_mode": "single_topic",
          "key_mode": "pk",
          "batch_size": 1000,
          "required_acks": 1,
          "security": {
            "sasl_mechanism": "SCRAM-SHA-512",
            "sasl_username": "cdc_user",
            "sasl_password": "your_sasl_password",
            "tls_enabled": true,
            "ca_cert_path": "/etc/kafka/ca.pem"
          }
        }
      }
    ]
  }'
```

### 示例5：增量同步到 Webhook

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "CDC事件通知",
    "mode": "INCREMENTAL",
    "sync_level": "TABLE",
    "source_schema": "production",
    "target_schema": "production_slave",
    "tables": ["orders"],
    "batch_size": 1000,
    "sink_configs": [
      {
        "type": "HTTP_WEBHOOK",
        "options": {
          "url": "http://127.0.0.1:9000/cdc/event",
          "method": "POST",
          "timeout_ms": 3000,
          "headers": {
            "Authorization": "Bearer your_token"
          },
          "retry_times": 3,
          "retry_backoff_ms": 500
        }
      }
    ]
  }'
```



## 架构设计



### 项目结构



``tree

mysql-to-sync/

├── main.go                      # 程序入口

├── etc/

│   └── application.toml         # 配置文件

├── internal/                    # 内部模块（DDD架构）

│   ├── api/                     # API层

│   │   ├── handler/             # 请求处理器

│   │   └── router/              # 路由配置

│   ├── checkpoint/              # 位点管理

│   │   ├── redis_checkpoint.go  # Redis位点存储

│   │   └── memory_checkpoint.go # 内存位点存储

│   ├── config/                  # 配置管理

│   ├── metadata/                # 元数据管理

│   │   ├── domain/

│   │   │   ├── entity/          # 表实体

│   │   │   └── service/         # 元数据分析服务

│   │   └── infrastructure/      # Schema检测器

│   ├── sync/                    # 同步核心模块

│   │   ├── application/         # 增量同步服务

│   │   ├── domain/

│   │   │   └── strategy/        # 匹配策略

│   │   └── infrastructure/

│   │       ├── reader/          # 数据读取器

│   │       ├── writer/          # 数据写入器

│   │       └── readonly/        # 只读管理器

│   └── task/                    # 任务管理

│       ├── application/service/ # 任务服务

│       └── domain/entity/       # 任务实体

├── pkg/                         # 公共包

│   ├── binlog/                  # Binlog订阅器

│   └── crypto/                  # AES-GCM 密码加密工具

└── web/                         # 前端界面(Vue3)

``



### 核心组件



#### 1. 元数据分析器 (Metadata Analyzer)

- 自动识别表结构（主键、唯一键、索引）

- 分析表的标识策略（PK Strategy / UK Strategy / Full Columns Strategy）

- 提供数据库和表的元数据查询



#### 2. 数据读取器 (Data Reader)

- **CursorReader**: 无主键表的分页读取器

- **RangeShardingReader**: 有主键表的范围分片读取器

- 支持批量读取，提高读取效率



#### 3. 数据写入器 (Data Writer)

- **BatchWriter**: 批量写入器，支持INSERT/UPDATE/DELETE

- **BufferedWriter**: 带缓冲的写入器，定时刷新

- 自动构建SQL，支持ON DUPLICATE KEY UPDATE



#### 4. Binlog订阅器 (Binlog Subscriber)

- 基于go-mysql-org/go-mysql实现

- 实时捕获INSERT/UPDATE/DELETE事件

- 自动保存和恢复Binlog位点



#### 5. 位点管理器 (Checkpoint Manager)

- Redis持久化存储（推荐）

- 内存存储（备用）

- **增量同步**：保存/恢复 Binlog 订阅位点（`SavePosition` / `GetPosition`）

- **全量同步**：不使用本模块；历史 `context.full_sync_resume` 字段仅作兼容和排查



#### 6. Sink 抽象层 (Sink Abstraction)

- **Sink 接口**：统一的数据写入抽象，定义 `Type()` / `Open()` / `Write()` / `Flush()` / `Close()` 生命周期
- **MySQLSink**：封装现有 `BufferedWriter`，保持 MySQL 增量写入行为完全不变
- **KafkaSink**：基于 `kafka-go` 实现，支持 SASL（PLAIN/SCRAM-SHA-256/SCRAM-SHA-512）、TLS（单向/mTLS）、per-table topic 路由
- **WebhookSink**：基于标准库 `net/http`，支持自定义 Header、可配置重试与退避策略
- **事件归一化器**：将 `BinlogEvent` 转换为统一的 `ChangeEvent`（INSERT/UPDATE/DELETE），隔离 Sink 与 binlog 细节
- **SinkFactory**：按 `sink_configs` 配置创建对应 Sink 实例，空配置默认返回 `[{MYSQL}]`
- **多 Sink 写入**：所有 Sink 写入成功后才推进 checkpoint，任一个失败则任务标记 FAILED



## 同步模式详解



### 1. 全量同步 (FULL)



**适用场景**：

- 一次性数据迁移

- 数据库初始化

- 数据备份



**语义定义**：

FULL 模式执行一次无缝全表遍历，保证分片边界与读取流程本身不漏数据；同步执行期间发生的新增、更新和删除不进行追平。如需覆盖同步期间的变化，请使用 ALL 模式。

- 不执行 `COUNT(*)`
- 不捕获 binlog 位点
- 不保存增量 checkpoint
- 依靠分片计划保证读取全集（sample / numeric range 均生成有界区间覆盖 `(-∞, +∞)`）
- 所有 worker 扫到 EOF、所有事务提交成功，即标记 `FULL_COMPLETED`

**工作流程**：

1. 分析源表结构

2. 在目标库创建表结构

3. 分批读取源表数据

4. 批量写入目标表（全量普通 `INSERT` 场景要求目标端为空或由用户承担冲突风险）

5. 更新进度；全量完成后清理历史 `full_sync_resume`

6. 暂停/失败后重启：全量未完成时拒绝续传，除非开启 DDL 前删除重新准备目标端



**性能优化**：

- 有主键表（数值单列）：按 min/max 主键 range 分片，表内多 worker 并行 keyset 读取

- 有主键表（其他）：单线程或 sample 边界并行 keyset 读取

- 无主键表：单协程流式读取

- 批量插入：全量阶段统一使用普通 `INSERT`；目标端必须由用户保证为空，或开启 `enable_drop_table_before_ddl=true` 在全量前重建为空。关闭该开关时若目标表已有数据，属于不支持场景，可能失败或污染目标数据。开启 `enable_skip_binlog=true` 时，全量写入前会在目标端写入连接上执行 `SET SESSION sql_log_bin=0`，写入完成后恢复为 1，可减少目标端 binlog 膨胀并规避级联复制回环；需目标库账号具备 SUPER 权限（或 MySQL 8 的 `SESSION_VARIABLES_ADMIN`）。



**中断处理**（详见 [全量中断处理与增量恢复指南](docs/guides/FULL_SYNC_RESUME_GUIDE.md)）：

- 全量阶段暂停/失败后不再续传

- 未开启 `enable_drop_table_before_ddl` 时再次启动会被拒绝；开启后会重建目标端并重新全量



### 2. 增量同步 (INCREMENTAL)



**适用场景**：

- 实时数据同步

- 主从复制

- 数据分发



**前置要求**：

- MySQL开启Binlog (ROW格式)

- binlog_row_image=FULL（推荐）

- 用户具有REPLICATION权限



**工作流程**：

1. 连接到MySQL Master

2. 订阅Binlog事件

3. 解析INSERT/UPDATE/DELETE事件

4. 应用变更到目标库

5. 保存Binlog位点



**特性**：

- 实时捕获数据变更

- 延迟 < 1秒

- 自动重连机制

- 审计日志记录



### 3. 全量+增量 (ALL)



**适用场景**：

- 首次部署数据同步

- 数据迁移后需要持续同步



**语义定义**：

ALL 模式先以无锁方式捕获全局 binlog 位点 P0，执行全量基线扫描，再捕获位点 P1，从 P0 回放 binlog 做有界追平（bounded catch-up）到 P1，追平后恢复非 identity 索引，最终进入持续同步。

- 第一次读取前以无锁 `SHOW MASTER STATUS` 获取并持久化 P0（不再使用 `FLUSH TABLES WITH READ LOCK`），失败必须终止
- 全量引擎：`full_load_engine=v1`（默认）与 `v2` 均使用普通短查询，不持有表级 MDL，不再使用长生命周期 RR 快照事务；V2 写入侧使用目标库 `__mts_fl_tx` 事务标记表做 Commit 未知恢复
- 全量基线扫描完成后捕获 P1，从 P0 回放 binlog 做有界追平到 P1：PK/UK INSERT 使用 upsert，UPDATE 正确处理 before/after key
- 追平完成后恢复非 identity 索引，再进入实时同步
- 无 PK/UK 表的 ALL 为 best-effort，不保证严格去重，需任务请求体显式确认 `allow_nopk_all=true`；历史版本的表级 binlog HWM（`table_binlog_hwms`）已下线，仅保留用于读取旧任务存档

**工作流程**：

1. 以无锁 `SHOW MASTER STATUS` 捕获并持久化 binlog 起始位点 P0

2. 执行全量基线扫描（与 FULL 相同的无缝遍历，普通短查询）

3. 基线扫描完成后捕获位点 P1

4. 从 P0 回放 binlog 做有界追平（bounded catch-up）到 P1

5. 追平后恢复非 identity 索引

6. 持续实时同步



**优势**：

- 一站式解决数据同步需求

- 无缝切换，数据不丢失



## 高级功能



### 1. 无主键表同步



对于没有主键或唯一键的表，系统自动采用**全列匹配策略**：



- INSERT: 直接插入

- UPDATE: WHERE条件使用所有列

- DELETE: WHERE条件使用所有列

- 性能影响：同步速度较慢，建议添加主键



### 2. 只读保护机制



同步期间，系统自动：

- 设置目标库普通用户为只读模式

- 防止其他应用修改目标数据

- 同步完成后自动恢复权限



### 3. 自定义数据库配置



每个任务可以配置独立的源和目标数据库：



``json

{

  "name": "跨机房同步",

  "source_db": {

    "host": "192.168.1.100",

    "port": 3306,

    "database": "production",

    "username": "reader",

    "password": "password"

  },

  "target_db": {

    "host": "192.168.2.100",

    "port": 3306,

    "database": "production_replica",

    "username": "writer",

    "password": "password"

  }

}

``



### 4. 任务监控



实时监控任务状态和指标：



- **任务状态**: PENDING/RUNNING/PAUSED/COMPLETED/FAILED/SCHEDULED/STOPPED

- **进度百分比**: 已处理行数/总行数

- **当前位置**: 正在处理的表和位置

- **Binlog位点**: 当前同步的Binlog文件和位置

- **延迟监控**: 与主库的位置差



## 最佳实践



### 1. 生产环境部署建议



**服务器配置**：

- CPU: 4核+

- 内存: 8GB+

- 磁盘: SSD，足够存储日志

- 网络: 低延迟，高带宽



**数据库配置**：

- 使用独立的同步账号

- 合理设置批量大小（建议1000-5000）

- 监控数据库负载



**Redis配置**：

- 部署Redis高可用集群

- 设置合理的过期策略

- 监控内存使用



### 2. 性能优化建议



**全量同步**：

- 增大batch_size（2000-5000）

- 使用多线程并行同步

- 避开业务高峰期



**增量同步**：

- 使用Redis持久化位点

- 监控Binlog延迟

- 定期清理历史Binlog



**数据库优化**：

- 目标表禁用索引和约束

- 同步完成后再启用

- 使用批量INSERT



### 3. 数据一致性保障



**同步前检查**：

- 确认源表有主键或唯一键

- 检查binlog配置

- 验证网络连接



**同步中监控**：

- 监控任务状态

- 检查错误日志

- 验证数据样本



**同步后验证**：

- 对比源表和目标表行数

- 抽样验证数据一致性

- 检查审计日志



### 4. 容灾方案



**服务高可用**：

- 部署多个实例，使用负载均衡

- 定期备份Redis位点数据

- 监控服务健康状态



**数据恢复**：

- 保留Binlog足够时间

- 定期全量备份

- 制定数据恢复预案



## 常见问题



### Q1: 增量同步失败：binlog_row_image不是FULL



**解决方案**：



修改MySQL配置：



``ini

[mysqld]

binlog_row_image=FULL

``



重启MySQL服务。



### Q2: 增量同步延迟较大



**可能原因**：

- 网络延迟

- 目标数据库性能瓶颈

- 批量写入配置不合理



**解决方案**：

- 调整batch_size参数

- 优化目标数据库性能

- 检查网络连接

- 增加worker_count



### Q3: 服务重启后增量同步从头开始



**原因**： 未配置 Redis，增量 Binlog 位点使用内存存储，进程退出后丢失。



**解决方案**： 配置 Redis 并重启服务。



### Q3b: 全量同步暂停后还能继续吗？



**不能继续。** 全量阶段使用普通 `INSERT` 语义，暂停、失败或进程中断后不再使用 `full_sync_resume` 续传。



处理方式：

- 未开启「DDL 前删除目标」：同一旧任务再次启动会失败；需要开启该开关重建后重新全量，或人工清理目标端后创建/重置任务从头跑

- 开启「DDL 前删除目标」：再次启动会重建目标端（DATABASE 级别重建目标库 / TABLE 级别重建目标表）并重新全量

- 全量已整体完成并进入增量阶段：再次启动会跳过全量，从增量 checkpoint 继续



详见 [全量中断处理与增量恢复指南](docs/guides/FULL_SYNC_RESUME_GUIDE.md)。



### Q4: 无主键表同步性能差



**原因**： 使用全列匹配，WHERE条件包含所有列。



**解决方案**：

- 为表添加主键或唯一键

- 调小batch_size

- 使用enable_limit_one保护



### Q5: 同步过程中目标库被其他应用修改



**解决方案**：

- 启用只读保护机制（默认开启）

- 同步期间锁定目标库

- 使用独立的目标库



### Q6: 如何查看同步失败的具体错误？



**解决方案**：



1. 查看任务状态：

``bash

GET /api/tasks/:id

``



2. 查看错误栈：

``json

{

  "context": {

    "error_stack": "具体错误信息"

  }

}

``



3. 查看审计日志（增量同步）



## 故障排查



### 日志查看



**应用日志**：

- 控制台输出

- 日志文件（如果配置）



**MySQL日志**：

- Error Log: 错误信息

- Slow Query Log: 慢查询



**Redis日志**：

- 连接错误

- 权限错误



### 常见错误码



| 错误信息 | 原因 | 解决方案 |

|---------|------|---------|

| connect refused | 数据库未启动 | 启动MySQL服务 |

| access denied | 权限不足 | 授予必要权限 |

| binlog not enabled | 未开启Binlog | 修改配置启用Binlog |

| table not found | 表不存在 | 检查表名和schema |

| connection timeout | 网络问题 | 检查网络连接 |



## 开发指南



### 本地开发



``bash

# 克隆项目

git clone https://github.com/yourusername/mysql-to-sync.git

cd mysql-to-sync



# 安装依赖

go mod download



# 运行服务

go run main.go



# 运行前端

cd web && npm run dev

``



### 运行测试



``bash

# 运行所有测试

go test ./...



# 运行测试并显示覆盖率

go test -cover ./...



# 生成覆盖率报告

go test -coverprofile=coverage.out ./...

go tool cover -html=coverage.out

``



### 代码规范



- 遵循 Go 标准代码规范

- 使用 gofmt 格式化代码

- 使用 go vet 检查代码

- 添加必要的注释



## 更新日志



### v1.2.0 (2026-04-02)

- 🔒 **密码加密存储**：任务中 source_db / target_db 的密码在持久化到数据库或文件时，使用 AES-256-GCM 加密，防止明文泄露

- 🔒 **安全配置**：新增 `[security]` 配置节，支持通过配置文件或环境变量 `MYSQL_TO_ASYNC_SECURITY_ENCRYPT_KEY` 设置加密密钥

- 🔒 **向后兼容**：未设置密钥时行为不变（明文存储）；已有明文旧数据在加载时自动兼容

- 🐛 修复前端更新配置时覆盖 `security` / `sync` 等后端专用字段的问题



### v1.1.1 (2026-03-30)

- ✨ **并发能力增强**: `TaskService` 改为任务级 runtime 隔离，移除单任务运行限制

- ✨ **可测性优化**: `StartTask` 增加测试注入点，支持并发启动场景稳定单测

- ✅ **新增测试**: 增加并发启动双任务隔离测试 `TestStartTask_ConcurrentRuntimeIsolation`



### v1.1.0 (2026-03-25)

- ✨ **性能优化**: 单表分片并行处理，大表同步速度提升2-4倍

- ✨ **UI优化**: 修复表级别同步选择问题，支持自定义数据库配置

- ✨ **搜索功能**: 添加数据库和表搜索框，支持实时过滤和模糊匹配

- ✨ **智能分片**: 自动根据表大小计算最佳分片数量和大小

- ✨ **兼容性增强**: 支持有主键、无主键、复合主键等多种表类型的并行处理

- ✨ **容错机制**: 分片失败自动回退到串行处理

- ✨ **跨库复制**: 支持同服务器和跨服务器的表结构自动复制

- ✨ **配置文档**: 新增详细的配置说明和性能调优指南

- 🐛 修复目标表不存在时继续写入的问题

- 🐛 修复跨库复制表结构的兼容性问题



### v1.0.0 (2026-03-20)

- ✨ 初始版本发布

- ✅ 支持全量同步、增量同步、全量+增量同步

- ✅ 集成Redis位点管理

- ✅ Web管理界面

- ✅ RESTful API

- ✅ 审计日志



## 贡献指南



欢迎提交 Issue 和 Pull Request！



1. Fork 本仓库

2. 创建特性分支 (git checkout -b feature/AmazingFeature)

3. 提交更改 (git commit -m 'Add some AmazingFeature')

4. 推送到分支 (git push origin feature/AmazingFeature)

5. 提交 Pull Request



## 技术支持



- **文档**: [INCREMENTAL_SYNC_GUIDE.md](INCREMENTAL_SYNC_GUIDE.md)

- **Issues**: [GitHub Issues](https://github.com/yourusername/mysql-to-sync/issues)



## 许可证



本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件



---



<div align="center">



**⭐ 如果这个项目对你有帮助，请给一个Star支持一下！⭐**



</div>

