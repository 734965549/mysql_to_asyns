# 全量同步断点续传指南

本文说明全量同步（`FULL` / `ALL` 模式的全量阶段）在**暂停后重新启动**时的续传行为，以及与增量位点（Binlog Checkpoint）的区别。

---

## 两种「断点」不要混淆

| 类型 | 存储位置 | 作用范围 | 是否依赖 Redis |
|------|----------|----------|----------------|
| **全量续传断点** | 任务存档 `context.full_sync_resume` | 全量阶段：跳过已完成表、从上次已提交主键继续 | 否，随任务 JSON 落盘 |
| **增量 Binlog 位点** | Checkpoint Manager（Redis / 内存） | 增量阶段：从上次 binlog 位置订阅 | 推荐 Redis |

全量续传由 `SyncPhase` + `FullSyncResume` 共同驱动；增量续传由 `checkpointManager` + `LastIncrementalPosition` 驱动。

---

## 何时会续传

满足以下全部条件时，暂停/失败后再次点击「启动」会**续传**而不是整库重跑：

1. `sync_phase` 为 `FULL_STARTED` 或 `FULL_FAILED`（`FullSyncIncomplete()` 为真）
2. 未开启 **DDL 前 DROP TABLE**（`enable_drop_table_before_ddl=false`）
3. 任务存档中仍保留 `full_sync_resume` 断点

若全新启动（`sync_phase` 为空或已完成全量）、或开启了 DROP TABLE，系统会在全量开始前**清空** `full_sync_resume`，按全新一轮执行。

---

## 续传粒度（按读取路径）

| 读取路径 | 典型场景 | 表级跳过 | 行级续传 |
|----------|----------|----------|----------|
| `keyset` | 单线程主键顺序读 | ✅ | ✅ 整表一个游标 `cursor` |
| `range` | 数值单列主键 + 表内并行分片 | ✅ | ✅ 每分片 `shard_cursors[w]` |
| `sample` | 非数值/复合主键并行采样 | ✅ | ⚠️ 仅表级（整表重跑，INSERT IGNORE 幂等） |
| `nopk` | 无主键流式读 | ✅ | ⚠️ 仅表级（整表重跑） |

**正确性原则**：游标只在**事务提交成功后**推进。暂停时未提交的批次会回滚，续传从「最后一次成功 commit 的末尾主键」之后继续，避免丢行。

---

## 数据结构（任务存档）

`context.full_sync_resume` 为 map，key = `sourceSchema.tableName`：

```json
{
  "db1.users": {
    "done": false,
    "read_path": "range",
    "intra_workers": 4,
    "shard_cursors": {
      "0": { "vals": ["12500"] },
      "2": { "vals": ["48000"] }
    }
  },
  "db1.orders": {
    "done": true
  }
}
```

- `done: true`：该表全量已完成，重启时**跳过**数据同步与索引 drop/restore
- `cursor`：单线程 keyset 路径的整表游标
- `shard_cursors`：range 并行路径各 worker 游标
- `read_path` / `intra_workers`：用于校验断点是否仍与当前配置一致

---

## 用户操作说明

### 暂停后继续

1. 执行中点击 **暂停** → 状态变为 `PAUSED`，`sync_phase` 保持 `FULL_STARTED`
2. 再次点击 **启动** → 跳过 `done=true` 的表，未完成表从断点继续
3. 日志关键字：`Resume: table ... already completed, skipping` / `continue after ...`

### 全量整体完成后

- `sync_phase` 变为 `FULL_COMPLETED`（ALL 模式随后接增量）
- `full_sync_resume` 被清空，不再占用存档空间

### 开启「DDL 前 DROP TABLE」时

续传**自动禁用**。每次启动会重建目标表，必须从全量头开始，否则会因跳过已 DROP 的表而丢数据。

---

## 与 ALL / INCREMENTAL 模式的关系

| 模式 | 全量未完成时重启 | 全量已完成时重启 |
|------|------------------|------------------|
| FULL | 续传全量 | 任务完成，一般无需再启 |
| ALL | 续传全量 | 跳过全量，直接接增量（`HasFullSyncEverCompleted()`） |
| INCREMENTAL | 不允许（须先完成过一次全量） | 从 binlog checkpoint 接增量 |

---

## 相关 API

```bash
POST /api/tasks/:id/pause    # 暂停；保留 sync_phase 与 full_sync_resume
POST /api/tasks/:id/start    # 启动/续传
```

任务详情 / 列表接口返回的 `context` 中含 `sync_phase`、`full_sync_resume`（调试与运维排查用）。

---

## 测试

单元测试见 `internal/task/application/service/resume_test.go`，覆盖游标序列化、range 分片确定性、断点生命周期等。
