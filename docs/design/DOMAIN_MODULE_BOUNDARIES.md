# 领域模块边界与调用关系说明

本文面向维护者，说明本项目各领域模块的职责边界、主要入参/出参、调用关系，以及任务状态如何流转。业务规则以代码为准；当本文与实现不一致时，应先修正实现或本文之一，避免文档和行为分叉。

## 1. 总览

本项目是 MySQL 到 MySQL 的同步服务，后端采用 Go + Gin，前端采用 Vue 3。后端按 DDD 风格拆分为 API、任务、元数据、同步、checkpoint、配置、审计和指标等模块。

核心调用主干：

```text
main.go
  -> config.LoadConfig / logger.Init
  -> task.NewTaskService
  -> metadata.NewIdentityAnalyzerService
  -> router.SetupRouter
  -> handler.TaskHandler / MetadataHandler
  -> task.TaskService
  -> sync reader / writer / readonly / IncrementalSyncService
  -> checkpoint.Manager / pkg.binlog.Subscriber
```

运行时主数据流：

```text
HTTP request
  -> api/handler 转换请求结构
  -> task/domain/entity.SyncTask 持有任务配置和进度上下文
  -> task/application/service.TaskService 做生命周期和跨领域编排
  -> metadata/domain/service.IdentityAnalyzer 识别表身份策略
  -> sync/infrastructure/reader 读取源库数据
  -> sync/infrastructure/writer 写入目标库
  -> checkpoint.Manager 仅保存增量 binlog 位点
  -> task storage 保存任务归档、状态、历史全量断点字段
```

## 2. 分层边界

| 层/目录 | 职责 | 允许依赖 | 不应承担 |
|---|---|---|---|
| `internal/api` | HTTP 路由、请求校验、请求/响应结构转换 | `task`、`metadata` 应用服务 | 不做同步算法、不直接拼业务 SQL |
| `internal/task/domain/entity` | 任务聚合、生命周期状态、同步阶段、历史全量断点结构 | 标准库、少量共享工具 | 不连数据库、不启动 goroutine |
| `internal/task/application/service` | 任务创建、启动、暂停、调度、运行时隔离、全量/增量编排、任务存储 | `metadata`、`sync`、`checkpoint`、`config` | 不承载底层 SQL 生成细节 |
| `internal/metadata/domain` | 表结构模型和身份策略选择 | 元数据仓库接口 | 不执行同步、不写目标库 |
| `internal/metadata/infrastructure` | 从 `information_schema` / MySQL 元数据读取表、列、PK、UK | `database/sql` | 不决定任务生命周期 |
| `internal/sync/application` | 增量同步服务，把 binlog 事件变成目标库写入 | `pkg/binlog`、writer、checkpoint | 不管理任务存储 |
| `internal/sync/domain/strategy` | PK/UK/全列匹配 WHERE 策略 | metadata entity | 不执行 SQL |
| `internal/sync/infrastructure/reader` | 全量读取源库，支持 keyset、range、cursor | `database/sql`、metadata entity | 不写目标库 |
| `internal/sync/infrastructure/writer` | SQL 构造、批量写、更新、删除、缓冲写入 | metadata entity、strategy | 不决定读路径和任务状态 |
| `internal/sync/infrastructure/readonly` | 目标库只读保护和临时 DDL 写权限 | `database/sql` | 不参与同步数据转换 |
| `internal/checkpoint` | 增量 binlog 位点保存和恢复 | Redis 或内存 | 不保存历史全量断点 |
| `internal/config` | TOML、环境变量覆盖、连接池参数、校验 | 标准库 | 不做业务编排 |
| `pkg/binlog` | 对 `go-mysql` canal 的薄封装，把行事件转为内部事件 | go-mysql | 不写目标库、不保存任务状态 |
| `pkg/crypto` | 任务数据库密码 AES-GCM 加解密 | 标准库 | 不决定何时持久化 |
| `web` | 管理端 UI | REST API | 不绕过 API 直接读写后端存储 |

## 3. 关键领域对象

### 3.1 SyncTask

位置：`internal/task/domain/entity/task.go`

输入来源：

- API 创建/更新任务请求。
- 配置文件默认值和环境变量覆盖。
- 服务端运行时写回的状态、进度、错误、历史断点字段。

输出去向：

- 返回给 API 调用方。
- 持久化到文件或 MySQL `sys_sync_tasks.content`。
- 作为 TaskService 编排全量/增量同步的输入。

重要字段：

| 字段 | 用途 | 维护者 |
|---|---|---|
| `Config` | 任务配置，如源/目标库、模式、表列表、批大小、worker 数、DDL 行为 | API + TaskService |
| `Context.Status` | 外部生命周期状态：`PENDING/RUNNING/PAUSED/COMPLETED/FAILED/SCHEDULED` | TaskService |
| `Context.SyncPhase` | 同步阶段：决定全量是否需要继续跑或能否接增量 | TaskService |
| `Context.FullSyncResume` | 历史兼容字段；当前全量不再续传，进入新一轮全量前清空 | TaskService 全量流程 |
| `Context.LastIncrementalPosition` | 最近增量位点快照，便于排查 | IncrementalSyncService 回调到 TaskService |

### 3.2 TableIdentity

位置：`internal/metadata/domain/entity/table.go`

输入来源：

- `SchemaDetector` 从源库读取列、主键、唯一键。

输出去向：

- reader 根据 `Strategy` 选择读取路径。
- writer 根据 `IdentifyCols` 生成 UPDATE/DELETE 的 WHERE。
- API metadata 接口返回给前端展示风险。

策略含义：

| 策略 | 使用条件 | 读路径 | 写入匹配 |
|---|---|---|---|
| `PK_STRATEGY` | 表有主键 | keyset/range | 主键列 WHERE |
| `UK_STRATEGY` | 无主键但有唯一键 | keyset/sample | 唯一键列 WHERE |
| `FULL_COLUMNS_STRATEGY` | 无主键且无唯一键 | cursor/nopk | 全列 WHERE，UPDATE 使用 before image |

## 4. API 调用边界

`internal/api/router` 只注册路由和中间件。`internal/api/handler` 负责 HTTP 层转换。

典型调用：

```text
POST /api/tasks
  -> TaskHandler.CreateTask
  -> CreateTaskRequest 转 TaskConfig
  -> TaskService.CreateTask
  -> task storage Save
  -> 返回 SyncTask
```

```text
POST /api/tasks/:id/start
  -> TaskHandler.StartTask
  -> TaskService.StartTask
  -> 创建 taskRuntime(sourceDB, targetDB, analyzer, readOnlyManager, cancel)
  -> goroutine 执行 TaskService.executeSync
  -> 返回 "Task started" 或调度信息
```

```text
GET /api/metadata/identity?schema=s&table=t
  -> MetadataHandler.GetTableIdentity
  -> IdentityAnalyzer.AnalyzeTable
  -> 返回 TableIdentity + no-PK warning
```

API 层入参/出参原则：

- 入参只做 JSON 绑定、默认值补齐、基础合法性检查。
- 出参保持 JSON 字段稳定，避免把内部 runtime、DB 连接、明文密码暴露出去。
- 错误码由 handler 根据 service 返回的错误语义转换，service 不依赖 Gin。

## 5. TaskService 编排边界

位置：`internal/task/application/service/task_service.go`

TaskService 是跨领域编排中心，但不是底层同步算法实现。

主要输入：

- `TaskConfig`：来自 API。
- `context.Context`：控制启动任务的生命周期。
- `config.Config`：存储、Redis、连接池、安全和同步默认值。

主要输出：

- 任务存储中的 `SyncTask` 快照。
- 内存 runtime map 中的每任务运行时资源。
- 调用 reader/writer/incremental service 后产生的目标库变更。

运行时隔离：

```text
runtimes[taskID] = taskRuntime{
  sourceDB,
  targetDB,
  analyzer,
  readOnlyManager,
  cancel,
}
```

每个运行任务拥有独立的源/目标 DB 连接池、元数据分析器、只读管理器和 cancel 函数。不要把单个任务的连接或 analyzer 复用给另一个运行任务。

## 6. 状态如何流转

### 6.1 TaskStatus

`TaskStatus` 描述对外生命周期。

```text
CreateTask
  -> PENDING

StartTask(now)
  PENDING/PAUSED/FAILED/COMPLETED -> RUNNING

ScheduleTask
  PENDING/PAUSED/FAILED/COMPLETED -> SCHEDULED

CancelSchedule
  SCHEDULED -> ScheduledFromStatus 或 PENDING

PauseTask / service shutdown
  RUNNING -> PAUSED

executeSync success
  RUNNING -> COMPLETED

executeSync failure
  RUNNING -> FAILED
```

### 6.2 SyncPhase

`SyncPhase` 描述同步进度阶段，不能和 `TaskStatus` 混用。

```text
"" / INIT
  -> FULL_STARTED              全量开始，已捕获或尝试捕获全量起点 binlog 位点
  -> FULL_COMPLETED            全量完成
  -> INCREMENTAL_STARTED       增量已经接管

FULL_STARTED
  -> FULL_COMPLETED            全量成功
  -> FULL_FAILED               全量明确失败

FULL_FAILED
  -> FULL_STARTED              仅在目标端被重建后启动新一轮全量
```

全量中断处理：

- `sync_phase` 是 `FULL_STARTED` 或 `FULL_FAILED` 时，表示全量未完成。
- `enable_drop_table_before_ddl=false` 时，同一旧任务再次启动会被拒绝；若人工清理/重建目标端，需要创建/重置任务后从头跑，或开启 destructive rebuild 后重新全量。
- `enable_drop_table_before_ddl=true` 时，允许启动新一轮全量，因为目标库/目标表会先被重建。

必须清理历史全量断点的情况：

- 每次进入新一轮全量前。
- 开启 `enable_drop_table_before_ddl=true`，因为目标表可能被重建。
- 运维显式要求全新全量。

## 7. 全量同步流程

触发入口：`TaskService.executeSync -> executeFullSync -> syncDatabasePair`

主要输入：

- `SyncTask.Config`
- `taskRuntime.sourceDB/targetDB/analyzer/readOnlyManager`
- `TableIdentity`
- `batch_size`、`worker_count`、`intra_table_worker_count`

主要输出：

- 目标库表结构和数据。
- `Context.TotalRows/ProcessedRows/ProgressPercent/CurrentPosition`。
- `Context.FullSyncResume`。
- `Context.FullSyncStartPosition` 和 `SyncPhase`。

流程：

```text
1. 任务进入 RUNNING。
2. 如果需要全量，短暂执行 FLUSH TABLES WITH READ LOCK 捕获 binlog 起点，然后 UNLOCK。
3. MarkFullSyncStarted(startPosition)。
4. 若 `enable_drop_table_before_ddl=true` 且 `sync_level=DATABASE`：在任何目标表 DDL/数据写入前，对去重后的唯一目标库执行 `DROP DATABASE IF EXISTS` + `CREATE DATABASE IF NOT EXISTS`，任一步失败终止全量；之后建表不再逐表 DROP TABLE。`sync_level=TABLE` 时保持原有逐表 DROP TABLE 行为。该删除仅在全量阶段执行一次，增量阶段不执行。
5. 按库/表遍历。
6. ensureTargetTable：必要时建表、可选 DROP TABLE（仅 TABLE 级别 + 开启删除时）、可选优化索引。
7. IdentityAnalyzer.AnalyzeTable 识别 PK/UK/no-PK。
8. 根据身份策略选择 reader：
   - PK/UK：RangeShardingReader，支持 keyset/range。
   - no-PK：CursorReader，流式读。
9. BatchWriter 写目标库；全量批量写统一使用普通 INSERT，目标端必须由用户保证为空，或通过 `enable_drop_table_before_ddl=true` 重建为空。
10. 更新运行进度；全量不再推进 full_sync_resume 游标。
11. 表完成后 MarkTableDone。
12. 全部完成后 MarkFullSyncCompleted。
13. ALL 模式继续进入增量；FULL 模式完成任务。
```

读路径：

| read_path | 典型场景 |
|---|---|
| `keyset` | 单 worker 主键/唯一键顺序读 |
| `range` | 数值单列主键分片并行（每 worker 独立读） |
| `sample` | 采样边界并行（每 worker 独立读） |
| `nopk` | 无主键流式读 |
| `channel` | 单 reader + channel 多 worker 写（`ChannelSyncExecutor`，预留未接入） |

并行读写的两种模型：

```text
range / sample（当前生产路径）：
  worker0 reader ──► worker0 writer
  worker1 reader ──► worker1 writer
  … 各分片独立读，无共享 batch channel

channel（预留路径，见 channel_sync.go）：
  单 reader ──► batchChan（容量默认 intraWorkers×4）──► N writers
  用于读慢、写快时的削峰；扩大 buffer 不解决长期写入瓶颈
```

### 7.1 短锁起点与增量追平原理

当前实现不再持有长事务或长时间全局读锁。正确性来自下面这组不变量：

```text
T0: 短暂 FTWRL，读取 SHOW MASTER STATUS 得到 P0，然后立即 UNLOCK
T1: 将 P0 保存到 checkpoint.Manager，作为后续增量订阅起点
T2: 全量阶段用普通短 SELECT 读取源表，普通 INSERT 写目标表（目标端需为空或由 DDL 前删除重建）
T3: 全量完成后，ALL 模式启动增量订阅，从 P0 RunFrom
T4: 回放 P0 之后发生过的 INSERT/UPDATE/DELETE，把全量期间的变化追平
```

因此，全量阶段允许不同 worker 在不同时间读取源表。源库在全量期间产生的变更不会靠全量快照保证一致，而是靠从 P0 开始的 binlog 回放追平。

重复和收敛规则：

| 场景 | 处理方式 | 结果 |
|---|---|---|
| 全量重复写同一 PK/UK 行 | 普通 `INSERT` | 目标端应为空；非空目标属于不支持场景，可能失败或污染目标数据 |
| 增量 INSERT 重复到达 PK/UK 行 | 增量 writer 启用 `ON DUPLICATE KEY UPDATE` | 后到事件覆盖旧值，收敛到源库状态 |
| 增量 UPDATE/DELETE PK/UK 行 | 使用 PK/UK WHERE | 精确匹配目标行 |
| 无主键表 UPDATE/DELETE | 使用 before image / 行镜像做全列 WHERE | 尽力匹配，依赖 `binlog_row_image=FULL` |
| 无主键表 INSERT | 退化为 `INSERT IGNORE`，没有真正冲突键 | 不能保证去重，推荐补 PK/UK |

这不是“所有重复都跳过”。全量阶段用普通 `INSERT`，目标端不应存在重复键；增量阶段对 PK/UK 表必须是 upsert，否则全量期间同一行的后续 INSERT/UPDATE 不能可靠收敛。无主键表因为没有冲突键，只能使用全列匹配降低 UPDATE/DELETE 风险，无法获得真正的 INSERT 去重语义。

## 8. 增量同步流程

触发入口：`TaskService.executeIncrementalSync -> sync/application.IncrementalSyncService.Start`

主要输入：

- `SyncConfig`：源库连接、源/目标 schema、表列表、batch size、server id。
- `checkpoint.Manager`：恢复上次 binlog 位点。
- `IdentityAnalyzer`：为每张表创建写入策略。

主要输出：

- 目标库 INSERT/UPDATE/DELETE。
- Redis/内存 checkpoint 中的 binlog 位点。
- 任务归档中的 `LastIncrementalPosition` 快照。
- 审计日志和 Prometheus 指标。

流程：

```text
1. 为源库到目标库构建 dbMapping。
2. 获取 targetDB 专用写连接，并关闭该连接上的外键检查。
3. 为每张表 AnalyzeTable。
4. 创建 BufferedWriter，并对增量 INSERT 启用 upsert。
5. 从 checkpoint.Manager.GetPosition(taskID) 恢复位点；无位点则从主库当前位置开始。
6. pkg/binlog.Subscriber 订阅 ROW binlog。
7. syncEventHandler.OnEvent 处理 INSERT/UPDATE/DELETE。
8. 成功写入后 SavePosition。
9. 可选 PositionPersister 将位点节流写回任务归档。
10. Stop 时 flush writer、恢复外键检查、关闭订阅。
```

增量写语义：

- PK/UK 表 INSERT：`INSERT ... ON DUPLICATE KEY UPDATE`。
- no-PK 表 INSERT：退化为 `INSERT IGNORE`，不能依赖 upsert。
- PK/UK 表 UPDATE/DELETE：使用身份列 WHERE。
- no-PK 表 UPDATE：必须使用 binlog before image 做全列 WHERE。
- no-PK 表 DELETE：使用事件行镜像做全列 WHERE。

## 9. Metadata 模块

接口：`IdentityAnalyzer`

```go
AnalyzeTable(schema, tableName string) (*entity.TableIdentity, error)
GetAllTables(schema string) ([]entity.TableInfo, error)
GetAllDatabases() ([]string, error)
```

边界：

- `domain/service` 负责策略决策：优先 PK，其次 UK，最后全列。
- `infrastructure` 负责 MySQL 元数据查询，不参与同步决策以外的业务流程。
- `TableIdentity.CursorCols` 是全量读取游标列；`IdentifyCols` 是写入匹配列，二者可能不同。

## 10. Reader 模块

接口：`DataReader`

```go
ReadBatch(ctx, offset, limit int64) ([]map[string]interface{}, error)
ReadBatchByKeys(ctx, lastID interface{}, limit int64) ([]map[string]interface{}, error)
GetEstimatedCount(ctx context.Context) (int64, error)
```

边界：

- reader 只从源库读并返回 `[]map[string]interface{}`。
- 不写任务状态；游标推进由 TaskService 在写事务提交后处理。
- 不写目标库；写入由 writer 负责。

## 11. Writer 模块

接口：`DataWriter`

```go
WriteBatch(ctx context.Context, rows []map[string]interface{}) error
UpdateWithBeforeImage(ctx context.Context, row, beforeImage map[string]interface{}) error
Update(ctx context.Context, row map[string]interface{}) error
Delete(ctx context.Context, row map[string]interface{}) error
```

边界：

- `SQLBuilder` 负责 deterministic SQL。
- `MatchStrategy` 只负责 WHERE 片段和参数。
- `BatchWriter` 执行 SQL，并记录审计/指标。
- `BufferedWriter` 只做批量缓冲和定时 flush。

重要规则：

- 全量写统一使用普通 INSERT；目标端必须为空或由 `enable_drop_table_before_ddl=true` 重建为空。
- 增量写必须调用 `EnableUpsert()`，否则重复 INSERT 不会覆盖旧值。
- no-PK 表不能获得真正 upsert，只能依赖全列匹配和 before image 降低风险。

## 12. Checkpoint 与续传边界

`internal/checkpoint.Manager` 管理增量位点。

```go
SavePosition(ctx, taskID, mysql.Position) error
GetPosition(ctx, taskID) (mysql.Position, error)
```

不要把历史全量断点放到 checkpoint：

- 全量断点：`task.Context.FullSyncResume` 是历史兼容字段，随任务归档持久化但当前不再用于续传。
- 增量断点：Redis/内存 checkpoint，保存 binlog file/pos。

两者用途不同，不能互相替代。

## 13. 配置、存储与密码

配置来源：

```text
etc/application.toml
  -> config.LoadConfig
  -> config.ApplyEnvOverrides
  -> config.GlobalConfig
```

任务存储：

- `storage.mode=mysql`：写入 MySQL `sys_sync_tasks`。
- 其他或初始化失败：回退文件存储。

密码规则：

- 任务对象在内存中保持明文，以便运行时连接数据库。
- 持久化前临时加密，序列化后恢复内存明文。
- 不要在日志、API 错误、审计日志里输出密码。

## 14. 修改代码时的落点

| 变更类型 | 推荐位置 |
|---|---|
| 新增任务状态或阶段 | `internal/task/domain/entity`，并同步 TaskService、测试、文档 |
| 新增 API 字段 | handler request/response、TaskConfig、README/docs、web |
| 新增表身份策略 | metadata domain service + sync strategy + writer tests |
| 修改全量读取策略 | reader + TaskService full sync + 全量中断门禁测试 |
| 修改增量 SQL 语义 | writer SQLBuilder/BatchWriter + IncrementalSyncService + SQL tests |
| 修改 checkpoint | checkpoint manager + 增量指南，避免混入历史 FullSyncResume 字段 |
| 修改存储 | TaskStorage 两条路径都要测：file + MySQL |

## 15. 注释维护建议

保留高价值注释：

- 模块边界、状态流转、不变量。
- 有副作用的地方：DROP TABLE、read_only、外键检查、密码加密。
- 事务提交后才能推进游标等时序约束。

减少低价值注释：

- 不要逐行解释 `return`、`if`、`append` 等语法。
- 注释不要重复代码名字，应解释为什么这样做、边界在哪里、失败后如何恢复。
