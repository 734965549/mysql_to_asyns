# MySQL-to-MySQL 同步平台设计文档

本文描述项目的总体设计。更细的模块职责、入参/出参和调用关系见 [领域模块边界与调用关系说明](DOMAIN_MODULE_BOUNDARIES.md)。

## 1. 设计目标

系统目标是在 MySQL 到 MySQL 场景下提供可运维的全量同步、增量同步和全量+增量组合同步能力。

核心约束：

- 支持有主键、有唯一键、无主键三类表。
- 全量同步暂停、失败或进程重启后不再续传；需要重新准备目标端后启动新一轮全量。
- 增量同步基于 ROW binlog，使用 Redis 或内存保存 binlog 位点。
- 历史全量断点字段 `context.full_sync_resume` 仅作兼容和排查，不放到 Redis。
- 目标库 destructive 行为必须显式配置，例如 `enable_drop_table_before_ddl=true`。
- 任务运行时必须隔离，每个运行任务拥有独立源/目标 DB、analyzer、readOnlyManager 和 cancel。

## 2. 领域划分

### Task 领域

Task 是任务聚合根，负责保存同步配置、外部生命周期状态和内部同步阶段。

职责：

- 创建、更新、删除、启动、暂停、定时启动任务。
- 保存任务状态、进度、错误栈和调度信息。
- 清理历史全量断点字段 `FullSyncResume`。
- 编排全量和增量同步，但不直接实现 SQL 生成。

关键状态：

- `TaskStatus`：外部生命周期，例如 `PENDING`、`RUNNING`、`PAUSED`。
- `SyncPhase`：内部同步阶段，例如 `FULL_STARTED`、`FULL_COMPLETED`、`INCREMENTAL_STARTED`。

二者必须保持分离。`TaskStatus=PAUSED` 只表示任务暂停；是否能接增量要看 `SyncPhase` 是否已完成全量。

### Metadata 领域

Metadata 负责识别表结构和数据行身份。

职责：

- 从源库读取列、主键、唯一键和表列表。
- 决定 `TableIdentity.Strategy`。
- 输出 reader/writer 可消费的 `IdentifyCols` 和 `CursorCols`。

策略优先级：

```text
主键 PK -> 唯一键 UK -> 全列匹配 FullColumns
```

### Sync 领域

Sync 负责实际数据同步执行。

职责：

- 全量读取源表并批量写入目标表。
- 增量订阅 binlog 并回放 INSERT/UPDATE/DELETE。
- 根据 `TableIdentity` 选择匹配策略和 SQL 生成方式。
- 目标库只读保护、DDL 临时写权限、外键检查控制。

Sync 不保存任务配置和生命周期状态；这些由 TaskService 维护。

#### Sink 抽象（多目标写入）

增量同步通过 Sink 接口抽象目标端写入，支持同时写入多个目标：

- **Sink 接口**：定义 `Type()` / `Open(ctx)` / `Write(ctx, *ChangeEvent)` / `Flush(ctx)` / `Close(ctx)` 生命周期方法，位于 `internal/sync/domain/sink/`
- **ChangeEvent**：统一事件模型，将 `BinlogEvent` 归一化为 `TaskID` / `SourceSchema` / `SourceTable` / `EventType` / `Before` / `After` / `PrimaryKeys` / `BinlogFile` / `BinlogPos`，隔离 Sink 实现与 binlog 细节
- **MySQLSink**：封装现有 `BufferedWriter`，保持 MySQL 增量写入行为完全不变（兼容性基准）
- **KafkaSink**：基于 `kafka-go`，支持 SASL（PLAIN/SCRAM-SHA-256/SCRAM-SHA-512）、TLS 双向认证、per-table topic 路由
- **WebhookSink**：基于标准库 `net/http`，支持自定义 Header、失败重试与退避策略
- **SinkFactory**：`NewSinks(configs, deps)` 按 `sink_configs` 类型创建对应 Sink 实例；空配置默认返回 `[{MYSQL}]`
- **交付语义**：同一 binlog 事务内全部事件对所有 Sink 写入并 flush 成功后，才在 `OnTransactionCommit` 统一 `SavePosition`；任一事件/Sink 失败则任务 FAILED，位点不越过该事务（At-Least-Once）
- **模式限制**：仅 `INCREMENTAL` 模式支持非 `MYSQL` 类型 Sink；`FULL`/`ALL` 含非 MYSQL Sink 时拒绝启动

### Checkpoint 领域

Checkpoint 只负责增量 binlog 位点。

职责：

- `SavePosition(taskID, pos)` 保存增量位点。
- `GetPosition(taskID)` 恢复增量位点。
- Redis 可持久化，内存实现用于测试或无 Redis 场景。

非职责：

- 不保存全量 row cursor。
- 不判断全量是否完成。

## 3. 全量同步设计

全量同步入口由 TaskService 调用。

流程：

```text
StartTask
  -> executeSync
  -> captureFullSyncStartPosition
  -> MarkFullSyncStarted
  -> syncDatabasePair
  -> ensureTargetTable
  -> AnalyzeTable
  -> reader 读取源表
  -> writer 写目标表（普通 INSERT；目标端需为空）
  -> 提交后更新运行进度
  -> MarkTableDone
  -> MarkFullSyncCompleted
```

全量起点位点通过无锁 `SHOW MASTER STATUS` 捕获（不再使用 `FLUSH TABLES WITH READ LOCK`）。基线扫描完成后再捕获 P1，从 P0 做有界追平到 P1。不要重新引入长事务全局快照、表级长生命周期 RR 快照或 `enable_consistent_snapshot`。

### 无锁起点与增量追平

本项目不依赖“全量期间一直持锁”来保证一致性，而是使用“起点位点 + 空目标全量写 + 增量追平”：

```text
1. 全量开始前以无锁 `SHOW MASTER STATUS` 拿到 binlog 位点 P0。
2. 把 P0 保存为增量 checkpoint。
3. 全量阶段执行普通短查询，目标端使用普通 INSERT 写入；目标表必须为空，或通过 `enable_drop_table_before_ddl=true` 在全量前重建为空。
   - 「DDL 前删除目标」按 `sync_level` 分支：DATABASE 级别在 `MarkFullSyncStarted` 后、任何目标表 DDL/数据写入前，对去重后的唯一目标库执行 `DROP DATABASE IF EXISTS` + `CREATE DATABASE IF NOT EXISTS`（utf8mb4/utf8mb4_unicode_ci），任一步失败终止全量；之后建表不再逐表 `DROP TABLE`。TABLE 级别保持每张表建表前 `DROP TABLE IF EXISTS`。两种级别均使用用户配置的目标库名/目标表名，仅在全量阶段执行一次，增量阶段不执行。
4. ALL 模式全量基线扫描完成后捕获 P1，增量从 P0 开始回放，做有界追平到 P1。
5. P0 之后发生的变更会再次应用到目标库，用于追平全量读取期间的时间差。
```

> **重要限制**：上述"无锁位点 + 非快照全量 + binlog 回放"的严格收敛保证**仅适用于有可靠非空 PK/UK 的表**。无 PK/UK 的表无法保证收敛，原因如下：
> 1. P0 之后源库插入了一行新数据；
> 2. 全量扫描也读取到了该行并写入目标表；
> 3. 随后增量 binlog 回放再次 INSERT 该行；
> 4. 由于无冲突键，`INSERT IGNORE` 无法去重，导致目标表产生重复行。
>
> 无主键表在任何时候都无法保证数据一致性，建议给表补充主键或唯一键。

重复处理不是单一的“全部跳过”：

- 全量批量写统一使用普通 `INSERT`，目标端必须由用户保证为空，或开启 `enable_drop_table_before_ddl=true` 在全量前重建为空；非空目标属于不支持场景，可能失败或污染目标数据。开启 `enable_skip_binlog=true` 时，全量写入前在目标端写入连接上执行 `SET SESSION sql_log_bin=0`，写入完成后恢复为 1，避免目标端 binlog 膨胀与级联复制回环；需目标库账号具备 SUPER 权限。
- 增量 PK/UK 表的 INSERT 使用 `INSERT ... ON DUPLICATE KEY UPDATE`，重复到达时要覆盖旧值，保证最终收敛。
- 增量 UPDATE/DELETE 使用 PK/UK 定位；无主键表使用 before image 或行镜像做全列 WHERE。
- 无主键表 INSERT 没有真正冲突键，无法可靠去重，仍建议给表补主键或唯一键。

### 读路径

| 表类型 | 读取方式 |
|---|---|
| 数值单列主键 | range 分片并行 |
| 主键/唯一键 | keyset 顺序读 |
| 采样边界并行 | sample |
| 无主键 | cursor 流式读 |

### 写入语义

全量写统一使用普通 `INSERT`（无 IGNORE、无 ON DUPLICATE KEY UPDATE）。目标端必须由用户保证为空，或开启 `enable_drop_table_before_ddl=true` 后由程序在全量前重建为空；如果目标表已有数据，属于不支持场景，可能失败或污染目标数据。全量不是增量回放，不使用 upsert 作为默认语义。开启 `enable_skip_binlog=true` 时，全量写入前在目标端写入连接上执行 `SET SESSION sql_log_bin=0`，写入完成后恢复为 1；需目标库账号具备 SUPER 权限。

#### V2 写事务提交与 `__mts_fl_tx`

`full_load_engine=v2` 时，写入侧在每个目标 schema 维护系统表 `__mts_fl_tx`（InnoDB，带表/列注释，含 `run_id`）。每个写事务提交前插入唯一 UUID marker，与业务行同事务提交。客户端遇到连接类 Commit 错误时，**不得**用业务行存在性推断结果（无主键跨事务相同行会误判；非锁定读可能与仍在处理的 COMMIT 竞态），而是对该 marker 做 `SELECT ... FOR UPDATE`：命中则只推进进度，无行则整事务重放；无法判定则 fail-closed。启动前还会：在首次目标端 DDL 前对目标 schema 获取 `GET_LOCK` 强制互斥（禁止同 schema 并发 V2；持有至任务级收尾；要求 MySQL ≥ 5.7.5 以支持同连接多锁；`target_max_open_conns` ≥ 2）；拒绝业务目标表占用保留名 `__mts_fl_tx`（大小写不敏感）；校验所有目标业务表为 InnoDB；并对已存在的 marker 表 fail-closed 校验 `BASE TABLE`/InnoDB/`id` 完整唯一键（`SUB_PART` NULL 或 ≥ 36）与 `run_id` 列。数据流水线成功后按本趟 `run_id` 删除本任务行（**不** `DROP` 共享表；清理独立短超时；失败/暂停保留行）。目标账号除 `CREATE TABLE` 外，还需对 marker 表具备 `INSERT`、`SELECT ... FOR UPDATE`、`DELETE`，以及 `GET_LOCK`/`RELEASE_LOCK`。详见 `docs/CONFIGURATION.md`「写事务提交标记表」。

## 4. 全量中断处理

历史断点字段保存在：

```text
SyncTask.Context.FullSyncResume
```

历史记录曾包含：

- `Done`：表是否完成。
- `ReadPath`：`keyset/range/sample/nopk`。
- `Cursor`：单 worker keyset 游标。
- `ShardCursors`：range 分片游标。
- `SampleBoundaries`：sample 首跑边界。
- `ProcessedRows`：用于展示和排查。

当前全量写入使用普通 `INSERT` 场景，暂停/失败/进程中断后不再使用该字段续传。`sync_phase=FULL_STARTED/FULL_FAILED` 且 `enable_drop_table_before_ddl=false` 时，同一旧任务再次启动会被拒绝；若人工清理/重建目标端，需要创建/重置任务后从头跑，或开启 `enable_drop_table_before_ddl=true` 后启动新一轮全量。进入新一轮全量前会清空历史断点。

## 5. 增量同步设计

增量同步入口：

```text
TaskService.executeIncrementalSync
  -> IncrementalSyncService.Start
  -> checkpoint.GetPosition
  -> SinkFactory.NewSinks(sink_configs)
  -> for each sink: sink.Open() / sink.PrepareTables()
  -> pkg/binlog.Subscriber.Start
  -> OnRow 缓冲同一事务事件
  -> OnXID
    -> 逐条 syncEventHandler.OnEvent（Write + Flush，不推进位点）
    -> 全部成功后 syncEventHandler.OnTransactionCommit
      -> 最终 Flush -> checkpointMgr.SavePosition + 任务存档位点回写
    -> 任一 OnEvent / OnTransactionCommit 失败 -> 返回错误，位点不推进，任务 FAILED
```

增量要求：

- MySQL binlog 必须是 ROW 格式。
- 对无主键表，`binlog_row_image=FULL` 是安全处理 UPDATE/DELETE 的关键前提。
- 增量 checkpoint 保存 binlog file/pos（XID 提交边界）。
- **同一事务内所有事件成功写入并 flush 后，才统一推进 checkpoint / 任务存档位点**；中途失败不得越过该事务，否则重启会永久漏数。

事件处理：

| 事件 | PK/UK 表 | 无主键表 |
|---|---|---|
| INSERT | upsert | `INSERT IGNORE`，不能保证去重 |
| UPDATE | after image 更新，身份列 WHERE | before image 全列 WHERE |
| DELETE | 身份列 WHERE | 全列 WHERE |

整事务成功后保存 checkpoint。checkpoint 保存失败应视为本次事务提交失败，避免后续恢复丢事件。

## 6. 无主键表设计

无主键表使用 `FullColumnsStrategy`。

全量阶段：

- 使用 cursor 流式扫描，避免 OFFSET 深分页。
- 全量中断后不续传；需要按全量中断处理规则重新准备目标端。

增量阶段：

- UPDATE 使用 before image 作为 WHERE 条件。
- DELETE 使用事件携带的行镜像作为 WHERE 条件。
- `enable_limit_one` 控制 SQL 生成中的 `LIMIT 1` 保护语义，应通过 SQL builder 测试覆盖。

风险：

- 如果目标库已有漂移，UPDATE/DELETE 可能匹配 0 行。
- 如果存在完全重复行，无主键场景无法精确定位逻辑上的"第几行"。
- ALL 模式依赖"短锁位点 P0 + 普通短查询全量 + 捕获 P1 + bounded catch-up 到 P1 + 持续增量"收敛无主键表：全量扫描期间发生的变更由 P0..P1 catch-up 阶段重放覆盖，之后转入持续增量。`full_load_engine=v1`/`v2` 均不再生成或依赖表级 binlog HWM（历史上 v2 曾在表级一致性快照窗口内捕获 HWM 并 fail-closed 校验，该机制已下线）。
- 推荐给业务表补主键或唯一键。

## 7. 存储与安全

任务存储支持文件和 MySQL：

- 文件：默认 `data/<taskID>.json`。
- MySQL：`sys_sync_tasks`，内容存 JSON。

密码持久化：

- 配置了 `[security].encrypt_key` 时，任务中的 `source_db.password` 和 `target_db.password` 持久化前会加密。
- 加密不应永久污染内存中的明文任务对象；运行时连接数据库仍使用明文。

## 8. 只读保护

`ReadOnlyManager` 用于同步期间保护目标库：

- 保存原始 `read_only/super_read_only`。
- 同步期间设置 `read_only=ON`，并保证同步账号可写入。
- 执行 DDL 时通过 `WithWriteAccess` 临时开放写权限。
- 同步结束或失败清理时恢复原始状态。

## 9. 扩展原则

- 新增状态或阶段时，先更新 `task/domain/entity`，再更新 TaskService、测试和文档。
- 新增 SQL 行为时，先补 `SQLBuilder` 单测，保证 SQL 和参数顺序确定。
- 修改全量中断处理时，必须覆盖暂停、失败、重启、`enable_drop_table_before_ddl` 四类场景。
- 修改增量 checkpoint 时，确认没有把历史全量断点和 binlog 位点混在一起。
- 修改 API 字段时，同步更新 handler、README/docs、配置示例和 web。
