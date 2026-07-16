# 多库全量同步改为并发执行 —— 实现计划

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

### 2.3 共享资源分析

| 资源 | 类型 | 并发安全性 |
|------|------|------------|
| `runtime.sourceDB` | `*sql.DB` | ✅ 安全（database/sql 连接池天然并发安全） |
| `runtime.targetDB` | `*sql.DB` | ✅ 安全 |
| `runtime.analyzer` | `IdentityAnalyzerService` | ✅ 安全（内部使用 `*sql.DB`） |
| `runtime.readOnlyManager` | `ReadOnlyManager` | ⚠️ 已在 `executeSync` 层级统一管理，不受库间并发影响 |
| `pendingIndexRestores` 切片 | `[]pendingIndexRestore` | ❌ 需要加锁（多个库对并发 append） |
| `syncDatabasePair` 内部 | 各库对独立获取连接 | ✅ 安全（`runtime.targetDB.Conn(ctx)` 返回独立连接） |

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

dbWorkerCount := effectiveDbWorkers(task.Config.DbWorkerCount, len(pairs))
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
            dbCancel()
        }
    }(p)
}

wg.Wait()
close(errChan)

// 检查错误
for err := range errChan {
    if errors.Is(err, errFullSyncStoppedByUser) {
        return errFullSyncStoppedByUser
    }
    return err // 返回第一个非停止错误
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
        *pending = append(*pending, pendingIndexRestore{...})
    }
    pendingMu.Unlock()
}
```

### 3.5 错误处理策略

- 使用 `context.WithCancel` 派生 `dbCtx`，任一库对失败时调用 `dbCancel()` 取消所有在途库对
- `syncDatabasePair` 内部使用 `dbCtx` 替代 `ctx`，确保能响应取消信号
- 优先返回 `errFullSyncStoppedByUser`（用户停止），其次返回第一个真实错误

### 3.6 停止信号处理

- 启动 goroutine 前检查 `isTaskStopped`，已停止则直接 break
- `syncDatabasePair` 内部多处已有 `isTaskStopped` 检查，配合 `dbCtx` 取消，双重保障

## 四、涉及文件清单

| 文件 | 改动类型 | 说明 |
|------|----------|------|
| `internal/task/domain/entity/task.go` | 新增字段 | 添加 `DbWorkerCount` 配置字段 + `EffectiveDbWorkers` 推导函数 |
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

1. **数据库连接池压力**：多库并发时，总连接数 = 库并发数 × 表并发数 × 单表内 worker 数。用户需根据实际连接池大小合理配置。建议在日志中打印总并发 goroutine 估算值。

2. **`syncDatabasePair` 内部 `panic` 保护**：已有的 `recover` 在表级 goroutine 中，库级 goroutine 也需要 `recover` 保护（已在方案中体现）。

3. **`pendingIndexRestores` 切片并发安全**：使用 `sync.Mutex` 保护 append，确保不丢失索引恢复条目。

4. **向后兼容**：`db_worker_count` 默认值 0 时的行为需要明确——是保持串行（向后兼容）还是默认并发？建议默认并发（`min(库对数量, 4)`），因为这对新用户更友好，且单库场景不受影响。