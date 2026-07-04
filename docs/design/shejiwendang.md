# MySQL-to-MySQL 同步平台设计文档

本文描述项目的总体设计。更细的模块职责、入参/出参和调用关系见 [领域模块边界与调用关系说明](DOMAIN_MODULE_BOUNDARIES.md)。

## 1. 设计目标

系统目标是在 MySQL 到 MySQL 场景下提供可运维的全量同步、增量同步和全量+增量组合同步能力。

核心约束：

- 支持有主键、有唯一键、无主键三类表。
- 全量同步可在暂停、失败或进程重启后续传。
- 增量同步基于 ROW binlog，使用 Redis 或内存保存 binlog 位点。
- 全量续传状态必须保存在任务归档 `context.full_sync_resume`，不放到 Redis。
- 目标库 destructive 行为必须显式配置，例如 `enable_drop_table_before_ddl=true`。
- 任务运行时必须隔离，每个运行任务拥有独立源/目标 DB、analyzer、readOnlyManager 和 cancel。

## 2. 领域划分

### Task 领域

Task 是任务聚合根，负责保存同步配置、外部生命周期状态和内部同步阶段。

职责：

- 创建、更新、删除、启动、暂停、定时启动任务。
- 保存任务状态、进度、错误栈和调度信息。
- 保存全量续传状态 `FullSyncResume`。
- 编排全量和增量同步，但不直接实现 SQL 生成。

关键状态：

- `TaskStatus`：外部生命周期，例如 `PENDING`、`RUNNING`、`PAUSED`。
- `SyncPhase`：内部同步阶段，例如 `FULL_STARTED`、`FULL_COMPLETED`、`INCREMENTAL_STARTED`。

二者必须保持分离。`TaskStatus=PAUSED` 只表示任务暂停；是否能续跑全量要看 `SyncPhase` 和 `FullSyncResume`。

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
  -> writer INSERT IGNORE 写目标表
  -> 提交后更新 FullSyncResume
  -> MarkTableDone
  -> MarkFullSyncCompleted
```

全量起点位点通过短暂 `FLUSH TABLES WITH READ LOCK` 捕获，随后立即 `UNLOCK TABLES`。不要重新引入长事务全局快照或 `enable_consistent_snapshot`。

### 短锁起点与增量追平

本项目不依赖“全量期间一直持锁”来保证一致性，而是使用“起点位点 + 幂等全量写 + 增量追平”：

```text
1. 全量开始前短锁拿到 binlog 位点 P0。
2. 立即解锁，并把 P0 保存为增量 checkpoint。
3. 全量阶段执行普通短查询，目标端默认使用 INSERT IGNORE 写入（`enable_drop_table_before_ddl=true` 时改用普通 INSERT）。
   - 「DDL 前删除目标」按 `sync_level` 分支：DATABASE 级别在 `MarkFullSyncStarted` 后、任何目标表 DDL/数据写入前，对去重后的唯一目标库执行 `DROP DATABASE IF EXISTS` + `CREATE DATABASE IF NOT EXISTS`（utf8mb4/utf8mb4_unicode_ci），任一步失败终止全量；之后建表不再逐表 `DROP TABLE`。TABLE 级别保持每张表建表前 `DROP TABLE IF EXISTS`。两种级别均使用用户配置的目标库名/目标表名，仅在全量阶段执行一次，增量阶段不执行。
4. ALL 模式全量完成后，增量从 P0 开始回放。
5. P0 之后发生的变更会再次应用到目标库，用于追平全量读取期间的时间差。
```

重复处理不是单一的“全部跳过”：

- 全量批量写默认使用 `INSERT IGNORE`，重复键跳过，保证全量重跑或续跑幂等；`enable_drop_table_before_ddl=true` 时目标端已重建为空（DATABASE 级别重建目标库 / TABLE 级别重建目标表），改用普通 `INSERT` 提升性能并禁用续传。
- 增量 PK/UK 表的 INSERT 使用 `INSERT ... ON DUPLICATE KEY UPDATE`，重复到达时要覆盖旧值，保证最终收敛。
- 增量 UPDATE/DELETE 使用 PK/UK 定位；无主键表使用 before image 或行镜像做全列 WHERE。
- 无主键表 INSERT 没有真正冲突键，无法可靠去重，仍建议给表补主键或唯一键。

### 读路径

| 表类型 | 读取方式 | 续传能力 |
|---|---|---|
| 数值单列主键 | range 分片并行 | 行级，按 shard cursor |
| 主键/唯一键 | keyset 顺序读 | 行级 |
| 采样边界并行 | sample | 表级 |
| 无主键 | cursor 流式读 | 表级 |

### 写入语义

全量写默认使用 `INSERT IGNORE`。这是为了让重复执行具备幂等性，并降低目标表已有重复键时的失败概率。全量不是增量回放，不使用 upsert 作为默认语义。

例外：`enable_drop_table_before_ddl=true` 时目标端已被重建为空（DATABASE 级别 `DROP DATABASE`+`CREATE DATABASE` 重建目标库；TABLE 级别 `DROP TABLE`+建表重建目标表），确认为空、不存在主键/唯一键冲突风险，此时全量写改用普通 `INSERT`（无 IGNORE、无 ON DUPLICATE KEY UPDATE），省去唯一键检查的纯开销以提升性能；同时自动禁用全量续传，因为目标端已重建、续传游标不再有意义。

## 4. 全量续传设计

续传状态保存在：

```text
SyncTask.Context.FullSyncResume
```

每张表的续传记录包含：

- `Done`：表是否完成。
- `ReadPath`：`keyset/range/sample/nopk`。
- `Cursor`：单 worker keyset 游标。
- `ShardCursors`：range 分片游标。
- `SampleBoundaries`：sample 首跑边界。
- `ProcessedRows`：用于展示和排查。

游标推进时机：

```text
读取一批 -> 写入目标库事务 -> Commit 成功 -> 更新游标 -> 保存任务归档
```

不能在写事务提交前推进游标，否则失败恢复时会跳过未落库数据。

禁用续传的场景：

- `enable_drop_table_before_ddl=true`。
- 目标表结构被重建或显式要求重新全量。

## 5. 增量同步设计

增量同步入口：

```text
TaskService.executeIncrementalSync
  -> IncrementalSyncService.Start
  -> checkpoint.GetPosition
  -> pkg/binlog.Subscriber.Start
  -> syncEventHandler.OnEvent
```

增量要求：

- MySQL binlog 必须是 ROW 格式。
- 对无主键表，`binlog_row_image=FULL` 是安全处理 UPDATE/DELETE 的关键前提。
- 增量 checkpoint 保存 binlog file/pos。

事件处理：

| 事件 | PK/UK 表 | 无主键表 |
|---|---|---|
| INSERT | upsert | `INSERT IGNORE`，不能保证去重 |
| UPDATE | after image 更新，身份列 WHERE | before image 全列 WHERE |
| DELETE | 身份列 WHERE | 全列 WHERE |

写入成功后保存 checkpoint。checkpoint 保存失败应视为本次事件处理失败，避免后续恢复丢事件。

## 6. 无主键表设计

无主键表使用 `FullColumnsStrategy`。

全量阶段：

- 使用 cursor 流式扫描，避免 OFFSET 深分页。
- 仅支持表级续传；暂停后未完成表会整表重跑。
- 依靠全量 `INSERT IGNORE` 保障重复写的幂等性。

增量阶段：

- UPDATE 使用 before image 作为 WHERE 条件。
- DELETE 使用事件携带的行镜像作为 WHERE 条件。
- `enable_limit_one` 控制 SQL 生成中的 `LIMIT 1` 保护语义，应通过 SQL builder 测试覆盖。

风险：

- 如果目标库已有漂移，UPDATE/DELETE 可能匹配 0 行。
- 如果存在完全重复行，无主键场景无法精确定位逻辑上的“第几行”。
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
- 修改全量续传时，必须覆盖暂停、失败、重启、`enable_drop_table_before_ddl` 四类场景。
- 修改增量 checkpoint 时，确认没有把全量续传和 binlog 位点混在一起。
- 修改 API 字段时，同步更新 handler、README/docs、配置示例和 web。
