# 全量中断处理与增量恢复指南

本文说明全量同步（`FULL` / `ALL` 模式的全量阶段）被暂停、失败或进程中断后的处理方式，以及它与增量 Binlog checkpoint 的区别。

## 两种断点不要混淆

| 类型 | 存储位置 | 当前用途 | 是否依赖 Redis |
|------|----------|----------|----------------|
| 全量历史断点 | 任务存档 `context.full_sync_resume` | 历史兼容字段；当前全量不再续传，进入新一轮全量前会清空 | 否 |
| 增量 Binlog 位点 | Checkpoint Manager（Redis / 内存） | 增量阶段从上次 binlog 位置继续订阅 | 推荐 Redis |

全量阶段写入使用普通 `INSERT`。为了避免暂停/失败后重复写入已落库数据，全量未完成时不再使用 `full_sync_resume` 续传。

## V2 写事务 Commit 未知恢复（与全量断点无关）

`full_load_engine=v2` 的进程内写入恢复**不**依赖 `full_sync_resume`，也不根据业务行是否存在猜测 Commit 结果。

每个目标 schema 会自动创建系统表 `__mts_fl_tx`（InnoDB，表/列带注释，含 `run_id`）。写事务在 `Commit` 前插入唯一 UUID，与业务数据同事务提交；客户端遇到连接类 Commit 错误时，换连后对该 UUID 做锁定当前读（`SELECT ... FOR UPDATE`）：命中则只推进进度，无行则整事务重放，无法判定则 fail-closed。启动前还会：在首次目标端 DDL 前对目标 schema 获取 `GET_LOCK` 互斥（持有至任务级收尾；MySQL ≥ 5.7.5；`target_max_open_conns` ≥ 2）；拒绝业务目标表占用保留名 `__mts_fl_tx`；校验目标业务表为 InnoDB；并对已存在 marker 表做结构 fail-closed 校验（含完整唯一索引 / `SUB_PART`）。数据流水线成功后按本趟 `run_id` 删除本任务 marker 行（不 `DROP` 共享表；独立短超时）；失败/暂停不清理。目标账号需具备对 marker 表的 `CREATE TABLE`/`INSERT`/`SELECT ... FOR UPDATE`/`DELETE`，以及 `GET_LOCK`/`RELEASE_LOCK`。

详见 `docs/CONFIGURATION.md`「写事务提交标记表（`__mts_fl_tx`）」与 `docs/design/shejiwendang.md`「V2 写事务提交与 `__mts_fl_tx`」。

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
