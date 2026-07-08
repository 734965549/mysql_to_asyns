# 全量中断处理与增量恢复指南

本文说明全量同步（`FULL` / `ALL` 模式的全量阶段）被暂停、失败或进程中断后的处理方式，以及它与增量 Binlog checkpoint 的区别。

## 两种断点不要混淆

| 类型 | 存储位置 | 当前用途 | 是否依赖 Redis |
|------|----------|----------|----------------|
| 全量历史断点 | 任务存档 `context.full_sync_resume` | 历史兼容字段；当前全量不再续传，进入新一轮全量前会清空 | 否 |
| 增量 Binlog 位点 | Checkpoint Manager（Redis / 内存） | 增量阶段从上次 binlog 位置继续订阅 | 推荐 Redis |

全量阶段写入使用普通 `INSERT`。为了避免暂停/失败后重复写入已落库数据，全量未完成时不再使用 `full_sync_resume` 续传。

## 全量未完成时

当 `sync_phase` 为 `FULL_STARTED` 或 `FULL_FAILED` 时，表示全量阶段已经开始但没有完整完成。

- `enable_drop_table_before_ddl=false`：同一旧任务再次启动会被拒绝。若人工清理/重建目标端，需要创建/重置任务后从头跑；或开启 `enable_drop_table_before_ddl` 由程序重建后重新全量。
- `enable_drop_table_before_ddl=true`：允许启动新一轮全量；DATABASE 级别会重建目标库，TABLE 级别会在建表前重建目标表。

全量未完成时不能直接进入增量，因为目标端还没有完整的全量基线。

## 全量完成后

全量完成后：

- `sync_phase` 变为 `FULL_COMPLETED`。
- `full_sync_resume` 会被清空。
- `ALL` 模式随后进入增量并记录增量 checkpoint。

如果任务已进入 `INCREMENTAL_STARTED`，后续重启会跳过全量，直接从增量 checkpoint 继续。

## 与 ALL / INCREMENTAL 模式的关系

| 模式 | 全量未完成时启动 | 全量已完成或增量已接管时启动 |
|------|------------------|------------------------------|
| FULL | 拒绝启动，除非开启 `enable_drop_table_before_ddl` 重新全量 | 任务通常已完成，无需再启 |
| ALL | 拒绝启动，除非开启 `enable_drop_table_before_ddl` 重新全量 | 跳过全量，直接接增量 |
| INCREMENTAL | 不允许，必须先完成过一次全量 | 从 binlog checkpoint 接增量 |

## 相关 API

```bash
POST /api/tasks/:id/pause    # 暂停；sync_phase 保持 FULL_STARTED
POST /api/tasks/:id/start    # 全量未完成时按上述规则启动或拒绝
```

任务详情 / 列表接口返回的 `context` 中仍可能包含历史 `full_sync_resume` 字段，仅用于兼容和排查，不代表当前版本会执行全量续传。

## 测试

单元测试见 `internal/task/application/service/resume_test.go` 和 `task_service_test.go`，覆盖历史游标序列化、断点清理、全量未完成启动门禁，以及增量启动门禁。
