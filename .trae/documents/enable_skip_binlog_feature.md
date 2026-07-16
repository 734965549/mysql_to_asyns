# 实现计划：写入前临时关闭 binlog（enable_skip_binlog）

## Summary

新增任务级开关 `enable_skip_binlog`，在全量同步写入数据前于目标端专用写入连接上执行 `SET SESSION sql_log_bin=0`，写入结束后恢复 `SET SESSION sql_log_bin=1`，从而避免目标端把全量批量导入的数据再次写入自身 binlog（加速导入、减少 binlog 膨胀、规避级联复制回环）。开关在前端任务表单中以 checkbox 提供，默认关闭。范围仅限全量同步，不改动增量同步。

## Current State Analysis

- 全量同步在目标端专用 `*sql.Conn` 上统一通过 [session_options.go](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/application/service/session_options.go) 的 `disableFullSyncWriteSession(ctx, conn, label)` 关闭 `FOREIGN_KEY_CHECKS=0` / `UNIQUE_CHECKS=0` 并 verify，`restoreFullSyncWriteSession(conn, label)` 在 defer 中恢复。该专用连接在整个批量写入期间被复用，因此 `sql_log_bin` 在同一连接上对后续 INSERT 生效。
- 该函数在 5 处被调用：[task_service.go](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/application/service/task_service.go) 的 4 条全量读取路径（nopk/keyset/range/sample，L2877/L3151/L3441/L3681）与 [channel_sync.go:304](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/application/service/channel_sync.go#L304)。所有调用点所在方法均持有 `task *taskEntity.SyncTask`，可访问 `task.Config`。
- 任务级 `enable_` 开关的贯穿链路已成熟，参考 `enable_drop_table_before_ddl`：[TaskConfig](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/domain/entity/task.go#L74-100) 实体字段 -> [task_handler.go](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/api/handler/task_handler.go) 的 `CreateTaskRequest`(bool) / `UpdateTaskRequest`(*bool+omitempty) -> [App.vue](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/web/src/App.vue) 的 taskForm/重置/回填/checkbox/详情展示。
- `SET SESSION sql_log_bin=0` 需 SUPER 权限（或 MySQL 8 的 `SESSION_VARIABLES_ADMIN`），与现有 `FOREIGN_KEY_CHECKS`、`read_only_manager` 的 SUPER 权限假设一致。
- 增量同步 [sync_service.go](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/sync/application/sync_service.go) 的 `writeConn` 也设置了 `FOREIGN_KEY_CHECKS=0`，但本次范围不含增量，不动 `SyncConfig` 与 `sync_service.go`。

## Assumptions & Decisions

- **作用范围**：仅全量同步（用户决策）。增量同步不改动。
- **字段名**：`enable_skip_binlog`（用户决策，遵循 `enable_` 前缀约定，JSON 字段名稳定）。
- **失败处理**：`SET SESSION sql_log_bin=0` 执行失败时直接返回错误终止任务（用户决策，与 `FOREIGN_KEY_CHECKS` 一致，避免"以为关了实际没关"的静默误导）。
- **执行顺序**：disable 时在现有 FK/UNIQUE 关闭 + verify 之后再执行 `sql_log_bin=0`；restore 时在 FK/UNIQUE 恢复之后再执行 `sql_log_bin=1`。这样保持现有 SQL 期望顺序不变，`skipBinlog=false` 时行为完全不变。
- **不做单独 verify**：`SET SESSION sql_log_bin=0` 要么成功设值、要么直接报权限错误，ExecContext 的错误即构成 fail-hard，无需像 FK/UNIQUE 那样追加 SELECT verify，保持改动最小。
- **权限前置条件**：目标库账号需具备 SUPER（或 `SESSION_VARIABLES_ADMIN`）。在前端 checkbox 文案中提示此前提。

## Proposed Changes

### 1. 实体层：新增配置字段
文件：[internal/task/domain/entity/task.go](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/domain/entity/task.go#L95-97)
- 在 `TaskConfig` 中 `EnableDropTableBeforeDDL` 之后新增字段：
  ```go
  EnableSkipBinlog bool `json:"enable_skip_binlog"` // 全量同步写入前在目标端临时关闭 sql_log_bin，写入后恢复；需目标账号具备 SUPER 权限
  ```
- 字段为 `bool`，零值 `false`，向后兼容已有任务存档（反序列化缺失该键即为 false）。

### 2. 全量同步 session 层：增加 binlog 关闭/恢复
文件：[internal/task/application/service/session_options.go](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/application/service/session_options.go)
- 新增常量：
  ```go
  fullSyncDisableBinlogSQL  = "SET SESSION sql_log_bin=0"
  fullSyncRestoreBinlogSQL  = "SET SESSION sql_log_bin=1"
  ```
- `disableFullSyncWriteSession` 签名增加 `skipBinlog bool` 参数。在现有 verify 通过后追加：
  ```go
  if skipBinlog {
      if _, err := conn.ExecContext(ctx, fullSyncDisableBinlogSQL); err != nil {
          return fmt.Errorf("disable binlog for %s: %w", label, err)
      }
  }
  ```
- `restoreFullSyncWriteSession` 签名增加 `skipBinlog bool` 参数。在现有 FK/UNIQUE 恢复之后追加（warn on error，与其他 restore 一致）：
  ```go
  if skipBinlog {
      if _, err := conn.ExecContext(ctx, fullSyncRestoreBinlogSQL); err != nil {
          logger.Warn("[Task] Failed to restore sql_log_bin for %s: %v", label, err)
      }
  }
  ```

### 3. 全量同步调用点：传递开关
文件：[internal/task/application/service/task_service.go](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/application/service/task_service.go)（4 处：L2877/L3151/L3441/L3681）
- 每处调用改为传入 `task.Config.EnableSkipBinlog`：
  ```go
  if err := disableFullSyncWriteSession(ctx, conn, writeSessionLabel, task.Config.EnableSkipBinlog); err != nil { ... }
  ```
- 对应 defer 中的 restore 同样传入：
  ```go
  defer func() {
      restoreFullSyncWriteSession(conn, writeSessionLabel, task.Config.EnableSkipBinlog)
      conn.Close()
  }()
  ```

文件：[internal/task/application/service/channel_sync.go](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/application/service/channel_sync.go#L304)（1 处）
- `processBatchTask` 已接收 `task *taskEntity.SyncTask`，同样传入 `task.Config.EnableSkipBinlog`：
  ```go
  if err := disableFullSyncWriteSession(ctx, conn, writeSessionLabel, task.Config.EnableSkipBinlog); err != nil { ... }
  defer restoreFullSyncWriteSession(conn, writeSessionLabel, task.Config.EnableSkipBinlog)
  ```

### 4. API 层：请求/响应字段
文件：[internal/api/handler/task_handler.go](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/api/handler/task_handler.go)
- `CreateTaskRequest` 新增：`EnableSkipBinlog bool `json:"enable_skip_binlog"``
- `UpdateTaskRequest` 新增：`EnableSkipBinlog *bool `json:"enable_skip_binlog,omitempty"``（指针，区分"未传"与"传 false"，与 `enable_read_only`/`enable_drop_table_before_ddl` 一致）
- `CreateTask` 映射：`taskCfg.EnableSkipBinlog = req.EnableSkipBinlog`
- `UpdateTask` 映射：
  ```go
  if req.EnableSkipBinlog != nil {
      task.Config.EnableSkipBinlog = *req.EnableSkipBinlog
  }
  ```

### 5. 前端 UI
文件：[web/src/App.vue](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/web/src/App.vue)
- taskForm 初始定义（~L300-320）与重置函数（~L750-773）新增：`enable_skip_binlog: false`
- 编辑回填（~L1515-1537）新增：`enable_skip_binlog: task.config.enable_skip_binlog || false`
- 表单 checkbox 渲染（在 `enable_drop_table_before_ddl` checkbox 附近，`v-if="targetType === 'MYSQL'"` 块内）新增一个 `<a-checkbox v-model="taskForm.enable_skip_binlog">`，文案说明"全量同步写入前临时关闭目标端 binlog（sql_log_bin=0），可加速导入并减少目标 binlog 体积；需目标库账号具备 SUPER 权限"，并以 `a-typography-text type="secondary"` 给出副标题说明。
- 详情页展示（~L3041 附近开关状态展示区）新增：`{{ detailPageTask.config.enable_skip_binlog ? '开启' : '关闭' }}`

### 6. 测试更新
文件：[internal/task/application/service/full_sync_insert_mode_test.go](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/application/service/full_sync_insert_mode_test.go)
- 现有直接调用 `disableFullSyncWriteSession(...)` 的 3 处（L232/L254/L276）补 `false` 参数（默认路径，期望不变）。
- 现有辅助 `expectTargetWriteSession` / `expectParallelTargetWriteSessions` 无需改动（默认 skipBinlog=false，不触发新 SQL）。
- 新增测试 `TestDisableFullSyncWriteSession_SkipBinlog`：用 sqlmock 期望顺序 `FK=0 → UNIQUE=0 → verify(0,0) → sql_log_bin=0 → restore FK=1 → UNIQUE=1 → sql_log_bin=1`，断言成功。
- 新增测试 `TestDisableFullSyncWriteSession_SkipBinlogFailsHard`：让 `sql_log_bin=0` 返回权限错误，断言返回 error 且包含 "disable binlog"。

### 7. 文档更新
- [README.md](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/README.md)：任务字段表（~L463）新增 `enable_skip_binlog` 行；JSON 示例（~L426-427、~L516-517）补 `"enable_skip_binlog": false`；全量 INSERT 说明段（~L1077）补一句 binlog 关闭语义与 SUPER 权限前提。
- [docs/design/shejiwendang.md](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/docs/design/shejiwendang.md)：全量写 session 行为段（~L128/L144）补 `enable_skip_binlog` 说明。
- 核对 [docs/CONFIGURATION.md](file:///d:/E盘/BaiduNetdiskDownload/go/mysql_to_asyns/docs/CONFIGURATION.md)：该文件聚焦全局 TOML/env 配置，任务级字段不在其范围，预计无需改动（实施时确认）。

## Verification Steps

1. 后端编译与静态检查：
   ```bash
   go build ./...
   go vet ./...
   ```
2. 针对性测试（session 与全量写路径）：
   ```bash
   go test ./internal/task/...
   go test -run TestDisableFullSyncWriteSession ./internal/task/application/service/...
   ```
3. 全量测试回归：
   ```bash
   go test ./...
   ```
4. 前端构建（改动 web/ 时）：
   ```bash
   cd web
   npm run build
   ```
5. 手动验证（可选）：创建任务勾选 `enable_skip_binlog`，启动全量同步，确认目标端连接执行了 `SET SESSION sql_log_bin=0`（通过目标端 `SHOW PROCESSLIST` / general log 或对无 SUPER 权限账号触发 fail-hard 错误信息验证）；全量结束后恢复 `sql_log_bin=1`。
6. 完成前自检：API JSON 字段名稳定、全量/增量 binlog 语义未被混淆（本次只动全量）、SUPER 权限前提在前端文案与文档中已提示、destructive 行为未引入。
