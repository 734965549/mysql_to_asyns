# 多库全量同步改为并发执行 —— 实现方案

## 一、摘要

将 `executeFullSync` 中多库全量同步从**串行**改为**并发**，使多个数据库对可以同时同步，缩短多库场景下的全量同步总耗时。

当前 `executeFullSync` 中库间是串行的 `for _, p := range pairs` 循环，每个库对内部表间已支持并发。本次改造在上层增加一层库间并发控制，同时保持内部表级并发不变。

## 二、当前状态分析

### 2.1 当前并发层级

```
StartTask → go execSync (1 goroutine)
  └─ executeFullSync
       ├─ 行数估算 (串行遍历 pairs)
       ├─ DROP DATABASE + CREATE DATABASE (可选，串行)
       ├─ 库间循环 (串行) ← 本次改造目标
       │    └─ syncDatabasePair (单库对)
       │         ├─ 阶段1：表结构准备 (串行)
       │         ├─ 阶段2：表数据同步 (并发, sem+WaitGroup)
       │         └─ 阶段3：索引恢复 (被收集到 pendingIndexRestores，不在此处执行)
       └─ 阶段3：统一索引恢复 (并发, sem+WaitGroup)
```

### 2.2 关键代码位置

| 文件 | 行号 | 说明 |
|------|------|------|
| `internal/task/application/service/task_service.go` | L2330-2361 | 库间串行循环 |
| `internal/task/application/service/task_service.go` | L2509-3939 | `syncDatabasePair` 函数 |
| `internal/task/application/service/task_service.go` | L137-157 | `taskRuntime` 和 `pendingIndexRestore` 结构体 |
| `internal/task/domain/entity/task.go` | L88-89 | `WorkerCount` 和 `IntraTableWorkerCount` 配置字段 |
| `internal/task/domain/entity/task.go` | L483-505 | `EffectiveIndexRestoreWorkers` 推导函数 |

### 2.3 共享资源并发安全性分析

| 资源 | 类型 | 并发安全性 |
|------|------|------------|
| `runtime.sourceDB` | `*sql.DB` | ✅ 安全（database/sql 连接池天然并发安全） |
| `runtime.targetDB` | `*sql.DB` | ✅ 安全 |
| `runtime.analyzer` | `IdentityAnalyzerService` | ✅ 安全（内部使用 `*sql.DB`） |
| `runtime.readOnlyManager` | `ReadOnlyManager` | ✅ 已在 `executeSync` 层级统一管理，不受库间并发影响 |
| `pendingIndexRestores` 切片 | `[]pendingIndexRestore` | ❌ 需要加锁（多个库对并发 append） |
| `syncDatabasePair` 内部 DB 连接 | 各库对独立获取连接 | ✅ 安全（`runtime.targetDB.Conn(ctx)` 返回独立连接） |

## 三、方案设计

### 3.1 整体思路

将 `executeFullSync` 中 L2330-2361 的 `for _, p := range pairs` 串行循环改为 **sem + WaitGroup + errChan** 并发模式，与现有的表级并发模式保持一致。

### 3.2 新增配置项：`db_worker_count`

在任务配置中新增 `db_worker_count` 字段，控制库级并发度。

```go
// task.go 配置结构体中新增
DbWorkerCount int `json:"db_worker_count"` // 库级并发数；0=自动（默认等于库对数量，封顶4）
```

推导规则（参考 `EffectiveIndexRestoreWorkers` 模式）：
- 配置值 > 0：使用配置值，受 hardMax 封顶
- 配置值 = 0：取 `min(库对数量, 4)`
- hardMax：内置 8

### 3.3 核心改动：`executeFullSync` 库间并发化

**改动位置**：`task_service.go` L2330-2361

**改动前**（串行）：

```go
var pendingIndexRestores []pendingIndexRestore
for _, p := range pairs {
    if err := s.syncDatabasePair(ctx, task, runtime, p.src, p.dst, ...); err != nil {
        return err
    }
}
```

**改动后**（并发）：

```go
var (
    pendingIndexRestores []pendingIndexRestore
    pendingMu            sync.Mutex
)
dbCtx, dbCancel := context.WithCancel(ctx)
defer dbCancel()

dbWorkerCount := taskEntity.EffectiveDbWorkers(task.Config.DbWorkerCount, len(pairs))
estimatedTotalGoroutines := dbWorkerCount * task.Config.WorkerCount
logger.Info("[Task %s] 多库并发启动: db_pairs=%d, db_workers=%d, table_workers=%d, 估算总并发=%d (请确认连接池上限)",
    taskID, len(pairs), dbWorkerCount, task.Config.WorkerCount, estimatedTotalGoroutines)

sem := make(chan struct{}, dbWorkerCount)
var wg sync.WaitGroup
errChan := make(chan error, len(pairs))

for _, p := range pairs {
    if s.isTaskStopped(taskID) {
        dbCancel()
        break
    }
    wg.Add(1)
    go func(p dbPair) {
        defer wg.Done()
        defer func() {
            if r := recover(); r != nil {
                errChan <- fmt.Errorf("db pair %s->%s panic: %v", p.src, p.dst, r)
            }
        }()

        sem <- struct{}{}
        defer func() { <-sem }()

        if err := s.syncDatabasePair(dbCtx, task, runtime, p.src, p.dst,
            tablesBySource[p.src], &pendingIndexRestores, &pendingMu, dbLevelRebuilt); err != nil {
            errChan <- err
            dbCancel() // 取消其他在途库对（多次调用幂等，安全）
        }
    }(p)
}

wg.Wait()
close(errChan)

// 收集所有错误后再判断优先级：优先返回"用户停止"，其次返回第一个真实错误。
// 注意：不能在循环内直接 return，否则若第一个是普通错误会漏掉后续的停止错误。
var (
    firstRealErr error
    stopped      bool
)
for err := range errChan {
    if errors.Is(err, errFullSyncStoppedByUser) {
        stopped = true
        continue
    }
    if firstRealErr == nil {
        firstRealErr = err
    }
}
if stopped {
    return errFullSyncStoppedByUser
}
if firstRealErr != nil {
    return firstRealErr
}
```

### 3.4 连带改动：`syncDatabasePair` 签名变更

由于 `pendingIndexRestores` 需要并发安全，需要传入 `*sync.Mutex`：

**改动前**：

```go
func (s *TaskService) syncDatabasePair(ctx context.Context, task *taskEntity.SyncTask,
    runtime *taskRuntime, sourceSchema, targetSchema string,
    specifiedTables []string, pending *[]pendingIndexRestore, dbLevelRebuilt bool) error
```

**改动后**：

```go
func (s *TaskService) syncDatabasePair(ctx context.Context, task *taskEntity.SyncTask,
    runtime *taskRuntime, sourceSchema, targetSchema string,
    specifiedTables []string, pending *[]pendingIndexRestore, pendingMu *sync.Mutex,
    dbLevelRebuilt bool) error
```

在 `syncDatabasePair` 内部（L3924-3935），append 操作需要加锁：

```go
if pending != nil && task.Config.OptimizeIndex {
    pendingMu.Lock()
    for _, r := range ready {
        if len(r.savedIndexes) == 0 {
            continue
        }
        *pending = append(*pending, pendingIndexRestore{
            targetSchema: targetSchema,
            targetTable:  r.targetName,
            indexes:      append([]map[string]interface{}(nil), r.savedIndexes...),
        })
    }
    pendingMu.Unlock()
}
```

### 3.5 新增辅助函数：`effectiveDbWorkers`

在 `internal/task/domain/entity/task.go` 中新增，参考 `EffectiveIndexRestoreWorkers` 的写法：

```go
// EffectiveDbWorkers 计算库级并发度。
//   - configured: 任务配置 db_worker_count
//   - pairCount: 库对数量
//
// 推导规则：configured<=0 时取 min(pairCount,4)；再受 hardMax=8 封顶；最低 1。
func EffectiveDbWorkers(configured, pairCount int) int {
    const defaultCap = 4
    const builtinHardMax = 8

    n := configured
    if n <= 0 {
        n = pairCount
        if n <= 0 {
            n = 1
        }
        if n > defaultCap {
            n = defaultCap
        }
    }
    if n > builtinHardMax {
        n = builtinHardMax
    }
    if n < 1 {
        n = 1
    }
    return n
}
```

### 3.6 错误处理策略

- 使用 `context.WithCancel` 派生 `dbCtx`，任一库对失败时调用 `dbCancel()` 取消所有在途库对
- `syncDatabasePair` 内部使用 `dbCtx` 替代 `ctx`，确保能响应取消信号
- **必须收集所有错误后再判断优先级**：先遍历 `errChan` 收集 `firstRealErr` 和 `stopped` 标记，循环结束后再决定返回值。切勿在循环内直接 `return`，否则若第一个错误是普通失败，会漏掉后续可能存在的 `errFullSyncStoppedByUser`（用户停止），导致停止语义被普通错误掩盖
- 优先级：`errFullSyncStoppedByUser`（用户停止） > 第一个真实错误
- `dbCancel()` 可被多个 goroutine 重复调用，`context.CancelFunc` 是幂等的，无需额外保护

### 3.7 停止信号处理

- 启动 goroutine 前检查 `isTaskStopped`，已停止则直接 break
- `syncDatabasePair` 内部多处已有 `isTaskStopped` 检查，配合 `dbCtx` 取消，双重保障

## 四、涉及文件清单

| 文件 | 改动类型 | 说明 |
|------|----------|------|
| `internal/task/domain/entity/task.go` | 新增字段+函数 | 添加 `DbWorkerCount` 配置字段 + `EffectiveDbWorkers` 推导函数 |
| `internal/task/application/service/task_service.go` | 修改 | (1) `executeFullSync` 库间并发化 (2) `syncDatabasePair` 签名变更 + append 加锁 |
| `internal/task/domain/entity/task_test.go` | 新增测试 | `EffectiveDbWorkers` 的单元测试 |
| `internal/config/config.go` | 可选新增 | 如需全局 `DbWorkerHardMax` 配置 |
| `web/` 前端 | 可选 | 任务创建/编辑表单中增加 `db_worker_count` 字段 |
| `docs/CONFIGURATION.md` | 可选 | 文档更新 |

## 五、验证步骤

### 5.1 单元测试

```bash
go test ./internal/task/domain/entity/ -run TestEffectiveDbWorkers -v
```

### 5.2 全量测试

```bash
go test ./internal/task/... -v
go test ./internal/sync/... -v
go test ./... -v
```

### 5.3 静态检查

```bash
go vet ./...
```

### 5.4 手动验证场景

1. **单库场景**：单库任务行为不变，仅 1 个库对，并发无影响
2. **多库场景**：配置 3 个库对，`db_worker_count=2`，验证同时只有 2 个库对在运行
3. **多库+停止**：多库同步中触发停止，验证所有库对正确退出
4. **多库+一库失败**：模拟一个库对失败，验证其他库对被取消
5. **多库+索引恢复**：验证并发收集的 `pendingIndexRestores` 完整无遗漏

## 六、风险与注意事项

1. **数据库连接池压力**：多库并发时，总连接数 = 库并发数 × 表并发数 × 单表内 worker 数。用户需根据实际连接池大小合理配置。在并发启动时打印估算总并发日志（已在方案 3.3 中体现），便于用户核对连接池上限。

2. **`syncDatabasePair` 内部 `panic` 保护**：已有的 `recover` 在表级 goroutine 中，库级 goroutine 也需要 `recover` 保护（已在方案中体现）。

3. **`pendingIndexRestores` 切片并发安全**：使用 `sync.Mutex` 保护 append，确保不丢失索引恢复条目。

4. **向后兼容**：`db_worker_count` 默认值 0 时，行为是默认并发（`min(库对数量, 4)`），单库场景不受影响。如果用户希望保持串行，可显式设置 `db_worker_count=1`。

5. **`dbCtx` 无法取消阶段1的 DDL 操作（已知限制，非本次引入）**：

   经代码验证，`syncDatabasePair` 调用链中的 `ensureTargetTable`（task_service.go L4347、L4367 等）和 `dropNonPrimaryKeyIndexes`（task_service.go L6737）大量使用 `context.Background()` 甚至无 context 的 `Exec`：

   ```go
   // ensureTargetTable L4347 - 获取连接用 context.Background()
   tgtDDLConn, _ := targetDB.Conn(context.Background())
   // ensureTargetTable L4367 - CREATE TABLE LIKE 用 context.Background()
   tgtDDLConn.ExecContext(context.Background(), "CREATE TABLE ...")
   // dropNonPrimaryKeyIndexes L6737 - 完全没有 context 参数
   _, err := targetDB.Exec(dropQuery)
   ```

   这意味着 `dbCancel()` 只能取消阶段2（数据同步）的在途操作，**无法取消阶段1（表结构准备）的 DDL**。此限制在当前串行模式下已存在，本次并发改造不会使其恶化。若需彻底解决，需单独重构 `ensureTargetTable` 和 `dropNonPrimaryKeyIndexes` 的 context 传递，建议作为后续独立优化项。

6. **`failTaskUnlessCancelled` 的 TOCTOU 竞态（已知限制，非本次引入）**：

   `failTaskUnlessCancelled`（task_service.go L5730）是 check-then-act 非原子操作：

   ```go
   func (s *TaskService) failTaskUnlessCancelled(ctx context.Context, taskID, errMsg string) {
       if ctx.Err() != nil || s.isTaskStopped(taskID) {  // 第1步：读锁检查
           return
       }
       s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)  // 第2步：写锁修改
   }
   ```

   两步之间任务可能被用户停止，导致已停止的任务被覆盖标记为 Failed。此竞态在当前表级并发中已存在，多库并发会增加多个库对同时失败的概率。本次改造**不修复**此问题，但需在测试中验证：多库失败场景下任务最终状态是否合理。若需彻底解决，建议后续将状态变更改为基于状态机的 CAS（Compare-And-Swap）原子更新。

7. **`ReadOnlyManager` 的 read_only 频繁切换干扰（需重点关注）**：

   经代码验证，`ensureTargetTable` -> `s.withDDL` -> `ReadOnlyManager.WithWriteAccess`（read_only_manager.go L104-144）会修改 **MySQL 实例级全局变量**：

   ```go
   // WithWriteAccess 内部
   m.targetDB.Exec("SET GLOBAL super_read_only = 0")
   m.targetDB.Exec("SET GLOBAL read_only = 0")
   defer func() {
       m.targetDB.Exec("SET GLOBAL read_only = 1")  // 恢复
   }()
   return fn()
   ```

   **问题**：每个任务只有一个 `runtime.readOnlyManager`，所有库对共享。多库并发时：
   - 库对A 的阶段1 DDL 关闭 `read_only=OFF` -> 执行 DDL -> 恢复 `read_only=ON`
   - 库对B 的阶段2 数据写入（INSERT，**不经过 withDDL**）恰好在此窗口执行
   - 若写入用户**非 SUPER 权限**，会被 `read_only=ON` 拦截导致写入失败

   `ReadOnlyManager` 内部有 `m.mu` 互斥锁能串行化 DDL 操作本身，但**无法防止 DDL 恢复 read_only 后、其他库对的数据写入被拦截**。

   **建议**：
   - 方案 A（推荐，改动小）：开启 `enable_read_only` 时，多库并发前**一次性关闭 read_only**，所有库对完成后**统一恢复**，而非每个库对独立切换。即把 `SetReadOnly`/`RestoreReadOnly` 的调用范围从 `executeSync` 级别保持不变即可（当前已是任务级，不在 `syncDatabasePair` 内），但需确认 `withDDL` 在并发场景下是否仍被触发。若 `enable_drop_table_before_ddl=true` 且 `db_level_rebuilt=true`，阶段1 的逐表 DDL 会被跳过（L2589 的 `effectiveDropBeforeDDL` 为 false），可规避此问题。
   - 方案 B（改动大）：为每个库对创建独立的 `ReadOnlyManager` 实例，但 MySQL 实例级变量无法按库隔离，此方案不可行。
   - **结论**：依赖写入用户具有 SUPER 权限（当前设计已假设，见 read_only_manager.go L33-34 注释），或确保 `enable_drop_table_before_ddl=true` 走库级重建路径规避逐表 DDL。

8. **总并发度不受控，可能耗尽连接池（需重点关注）**：

   经代码验证，`workerCount`（task_service.go L2543-2549）是**每个库对独立从 `task.Config.WorkerCount` 读取的**，不感知库级并发数。

   多库并发时的峰值并发 goroutine 数：

   ```
   总并发 = dbWorkerCount × workerCount × intraWorkers
   ```

   例如：4 库并发 × `workerCount=4` × `intraWorkers=4` = **64 个数据同步 worker**，全部共享同一个 `runtime.sourceDB` 和 `runtime.targetDB` 连接池。

   `legacyCap=16` 和 `hardMax=64`（task_service.go L835-861）只封顶**单表内并发**，不感知库级并发数。

   **建议**（二选一）：
   - 方案 A（推荐）：引入全局 worker 预算信号量，跨库对共享。在 `executeFullSync` 中创建 `globalSem := make(chan struct{}, totalBudget)`，传入 `syncDatabasePair`，表级 `sem` 从全局预算中获取许可。`totalBudget` 可由配置项 `max_total_workers` 控制，默认等于 `workerCount`（即总并发不变，库级和表级自动分配）。
   - 方案 B：每个库对的 `workerCount` 动态折算：`effectiveWorkerCount = max(1, task.Config.WorkerCount / dbWorkerCount)`。简单但可能导致单库并发度过低。

9. **`estimatedRows` 行数估算串行遍历（建议并发优化）**：

   `executeFullSync` 中行数估算（task_service.go L2163-2225）是**串行遍历 pairs** 的：

   ```go
   for _, p := range pairs {
       // 串行调用 analyzer.AnalyzeTable + reader.GetEstimatedCount
       estimatedRows += count
       allTableEntries = append(allTableEntries, ...)
   }
   s.updateTaskEstimatedRows(taskID, estimatedRows)
   s.initRunningProgress(taskID, allTableEntries, "full")
   ```

   **问题**：多库场景下，行数估算阶段也是串行的，无法利用多库并发加速。`AnalyzeTable` 和 `GetEstimatedCount` 都使用 `runtime.sourceDB`（`*sql.DB` 并发安全），可以并发。

   **建议**：将行数估算也改为并发（与库间数据同步并发使用相同的 `dbWorkerCount`），用 `atomic.AddInt64` 累加 `estimatedRows`，用 `sync.Mutex` 保护 `allTableEntries` 的 append，`wg.Wait()` 后再调用 `initRunningProgress`。此优化**非必须**，但可进一步缩短多库场景的全量同步启动时间。

10. **多库并发时日志交错混乱（建议优化）**：

    当前日志前缀只有 `[Task %s]`（taskID），**没有库对标识**。多库并发时无法区分是哪个库对的阶段切换：

    ```go
    // L2560 - 无库标识
    logger.Info("[Task %s] 阶段1: 同步 %d 个表结构...", taskID, len(tables))
    // L2622 - 无库标识
    logger.Info("[Task %s] 阶段1完成：%d 个表结构就绪...", taskID, len(ready))
    ```

    `ensureTargetTable` 内部的日志（L4336、L4377、L4461）甚至只有 `[Task]` 前缀，**连 taskID 都缺失**：

    ```go
    logger.Info("[Task] Target table %s.%s already exists", ...)  // L4336
    ```

    **建议**：在 `syncDatabasePair` 的关键日志中增加库对标识前缀，如 `[Task %s][DB %s->%s]`，便于多库并发时排查问题。同时修复 `ensureTargetTable` 内部日志缺失 taskID 的问题（需传入 taskID 参数）。此优化**非必须**，但显著提升可观测性。

## 七、实施优先级建议

为保证改造质量，建议按以下优先级分批实施：

### 第一批：必须完成（核心改造 + 安全保障）

| 序号 | 内容 | 对应章节 |
|------|------|----------|
| 1 | 新增 `DbWorkerCount` 配置字段 + `EffectiveDbWorkers` 推导函数 | 3.2 / 3.5 |
| 2 | `executeFullSync` 库间并发化（含错误收集逻辑修复） | 3.3 |
| 3 | `syncDatabasePair` 签名变更 + `pendingMu` 加锁 | 3.4 |
| 4 | 单元测试 `EffectiveDbWorkers` | 五 |

### 第二批：强烈建议（避免资源耗尽和写入失败）

| 序号 | 内容 | 对应章节 |
|------|------|----------|
| 5 | 全局 worker 预算信号量，防止连接池耗尽 | 风险 8 |
| 6 | `enable_read_only` + 非 `drop_table_before_ddl` 场景的 read_only 切换规避 | 风险 7 |

### 第三批：可选优化（提升性能和可观测性）

| 序号 | 内容 | 对应章节 |
|------|------|----------|
| 7 | `estimatedRows` 行数估算并发化 | 风险 9 |
| 8 | 日志增加库对标识前缀 `[Task %s][DB %s->%s]` | 风险 10 |

### 已知限制（本次不修复，记录在案）

| 序号 | 内容 | 对应章节 |
|------|------|----------|
| 9 | `ensureTargetTable` / `dropNonPrimaryKeyIndexes` 的 `context.Background()` 无法取消 | 风险 5 |
| 10 | `failTaskUnlessCancelled` 的 TOCTOU 竞态 | 风险 6 |
| 11 | `refreshOverallProgressLocked` 的 `CurrentTable` 多库并发时只显示最后一个 running 表 | 风险 3（代码验证补充） |