# 单元测试文档

## 概述

本文档描述了 MySQL-to-Async 项目的单元测试策略、测试覆盖范围和运行方法。

## 测试策略

### 测试层次

1. **单元测试**: 测试单个函数或方法的行为
2. **集成测试**: 测试多个组件之间的交互
3. **端到端测试**: 测试完整的业务流程

### 测试原则

- 每个测试应该是独立的，不依赖其他测试
- 使用 mock 和 stub 隔离外部依赖
- 测试覆盖正常流程和异常情况
- 测试命名清晰，描述测试意图

## 测试目录结构

```
mysql-to-sync/
├── internal/
│   ├── config/
│   │   └── config_test.go
│   ├── metadata/
│   │   ├── domain/
│   │   │   ├── entity/
│   │   │   │   └── table_test.go
│   │   │   └── service/
│   │   │       └── identity_analyzer_test.go
│   │   └── infrastructure/
│   │       └── schema_detector_test.go
│   ├── sync/
│   │   ├── application/
│   │   │   └── sync_service_test.go
│   │   ├── domain/
│   │   │   └── strategy/
│   │   │       └── match_strategy_test.go
│   │   └── infrastructure/
│   │       ├── reader/
│   │       │   └── cursor_reader_test.go
│   │       └── writer/
│   │           ├── sql_builder_test.go
│   │           └── data_writer_test.go
│   └── task/
│       ├── application/
│       │   └── service/
│       │       ├── task_service_test.go
│       │       ├── task_event_recorder_test.go
│       │       └── resume_test.go   # 历史全量断点字段与清理
│       ├── infrastructure/
│       │   └── storage/
│       │       └── file_task_event_store_test.go
│       └── domain/
│           └── entity/
│               ├── task_test.go
│               └── task_event_test.go
└── pkg/
    ├── taskevent/
    │   └── sanitize_test.go
    │   └── logger_test.go
    └── storage/
        └── storage_test.go
```

## 运行测试

### 运行所有测试

```bash
go test ./...
```

### 运行特定包的测试

```bash
go test ./internal/config/...
go test ./internal/task/...
```

### 运行特定测试

```bash
go test -run TestTaskService ./internal/task/...
go test -run TestSQLBuilder ./internal/sync/...
```

### 查看测试覆盖率

```bash
# 生成覆盖率报告
go test -coverprofile=coverage.out ./...

# 查看覆盖率统计
go tool cover -func=coverage.out

# 生成 HTML 覆盖率报告
go tool cover -html=coverage.out -o coverage.html
```

### 详细输出

```bash
go test -v ./...
```

### 可选：真实 MySQL 全量快照并发集成（§12.1）

默认 `go test ./...` **不编译** `//go:build integration` 用例。本地或夜间任务需真实 InnoDB（可复用仓库 `docker-compose.yml` 的 `mysql-source`）：

```bash
# 示例 DSN（root，需具备 CREATE DATABASE）
export TEST_MYSQL_DSN='root:root_password@tcp(127.0.0.1:3306)/?parseTime=true&multiStatements=true'

go test -tags=integration -count=1 -timeout=10m -v ./internal/sync/fullload/ -run TestIntegration
```

覆盖场景：

| 测试文件 | 用例 | 描述 |
|----------|------|------|
| `integration_b5_test.go` | `TestIntegration_B5_MultiTableFairness_*` | 8 小表 + 大 JSON 并行全量，行数一致 |
| `integration_b5_test.go` | `TestIntegration_B5_Backpressure_*` | 慢目标写入触发器，小表仍可完整写入 |
| `integration_b5_test.go` | `TestIntegration_B5_OversizedJSONRow_*` | 单行超 batch_bytes + `ROW_EXCEEDS_BATCH_BYTES` |
| `integration_b5_test.go` | `TestIntegration_B5_NoPKLargeText_*` | 无 PK 大字段表行数一致 |
| `integration_b5_test.go` | `TestIntegration_B5_ReadBudgetPeakWithinCap` | 读预算峰值不超过 cap |
| `fault_injection_test.go` | `TestIntegration_FaultInjection_*` | 源查询超时 / 慢 writer barrier（需较长超时） |

辅助：`integration_mysql_test.go` 提供 `openIntegrationMySQL` / `seedBillRows` 等 helper。

未设置 `TEST_MYSQL_DSN` 时上述用例会 `Skip`。

## 测试覆盖范围

### Config 模块

| 测试用例 | 描述 |
|----------|------|
| TestLoadConfig | 测试配置文件加载 |
| TestConfigDefaults | 测试默认配置值 |
| TestConfigValidation | 测试配置验证 |

### Task 模块

| 测试用例 | 描述 |
|----------|------|
| TestCreateTask | 测试任务创建 |
| TestGetTask | 测试获取任务 |
| TestGetAllTasks | 测试获取所有任务 |
| TestUpdateTask | 测试更新任务 |
| TestDeleteTask | 测试删除任务 |
| TestStartTask | 测试启动任务 |
| TestStartTask_ConcurrentRuntimeIsolation | 测试并发启动两个任务时 runtime 隔离 |
| TestPauseTask | 测试暂停任务 |
| TestTaskStatus | 测试任务状态转换 |
| resume_test.go | 历史全量断点字段：游标序列化、断点清理、全量续传禁用 |

### Sync 模块

| 测试用例 | 描述 |
|----------|------|
| TestSQLBuilder_Insert | 测试 INSERT 语句构建 |
| TestSQLBuilder_Update | 测试 UPDATE 语句构建 |
| TestSQLBuilder_Delete | 测试 DELETE 语句构建 |
| TestSQLBuilder_BatchInsert | 测试批量 INSERT 语句构建 |
| TestMatchStrategy | 测试匹配策略 |
| TestCursorReader | 测试游标读取器 |
| TestRangeShardingReader | 测试范围分片读取器 |

### Fullload V2（B3/B4/B5）

| 测试文件 / 用例 | 描述 |
|-----------------|------|
| `read_budget_test.go` | 全局读取预算、单表占用上限、Acquire/Release |
| `chunk_scheduler_test.go` | 公平 chunk 轮询、单表 burst |
| `queue_test.go` | 写队列公平出队、表级 soft limit、单事务单表 |
| `stress_fairness_test.go` | 多表调度不饿死、读预算峰值、背压事件（B5-T02/T03） |
| `table_progress_test.go` | 表无进展 / 恢复事件、表级读行快照（B3-T07） |
| `events_test.go` | 背压状态机、EventSink nil 安全 |
| `chunk_test.go` → `TestPlanKeysetBoundaries_EstimateFailedEmitsEvent` | TABLE_ROWS 失败 fallback + 事件 |
| `reader_test.go` → `TestScanUpTo_BytesSplitAndOversizedRowCallback` | 字节拆批与超大单行回调 |
| `fault_injection_test.go`（`//go:build integration`） | 源查询超时 → 表级重试 → staging 发布 |

### TaskEvent（B1/B5）

| 测试文件 / 用例 | 描述 |
|-----------------|------|
| `task_event_recorder_test.go` | 60s 指纹聚合、ERROR 不抑制 |
| `task_event_lifecycle_test.go` | Start/Pause/Resume execution 轮次、Complete/Failed、DeleteTask 清事件（B5-T05） |
| `file_task_event_store_test.go` / `mysql_task_event_store_test.go` | Append/List/Delete/seq 恢复 |
| `task_event_store_contract_test.go` | 文件存储契约、Prune 保留 ERROR |
| `task_event_handler_test.go` | GET `/events` 二次脱敏、参数校验 |
| `pkg/taskevent/sanitize_test.go` | DSN/password/Bearer/嵌套 details 脱敏 |
| `linttasklog/lint_test.go` | fullload 禁止 `[Task` 业务 Warn/Error |


| 测试用例 | 描述 |
|----------|------|
| TestTableIdentity | 测试表标识实体 |
| TestIdentityAnalyzer | 测试标识分析器 |
| TestSchemaDetector | 测试模式检测器 |

## Mock 和 Stub

### 数据库 Mock

使用 `github.com/DATA-DOG/go-sqlmock` 模拟数据库操作：

```go
func TestWithMockDB(t *testing.T) {
    db, mock, err := sqlmock.New()
    if err != nil {
        t.Fatalf("Error creating mock: %v", err)
    }
    defer db.Close()

    // 设置期望
    mock.ExpectQuery("SELECT COUNT").WillReturnRows(
        sqlmock.NewRows([]string{"count"}).AddRow(100),
    )

    // 执行测试
    // ...
}
```

### Redis Mock

使用 `github.com/alicebob/miniredis` 模拟 Redis：

```go
func TestWithMockRedis(t *testing.T) {
    s := miniredis.RunT(t)
    
    // 使用模拟的 Redis 地址
    addr := s.Addr()
    // ...
}
```

## 持续集成

### GitHub Actions 配置

```yaml
name: Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Go
      uses: actions/setup-go@v4
      with:
        go-version: '1.21'
    
    - name: Run tests
      run: go test -v -coverprofile=coverage.out ./...
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        files: ./coverage.out
```

## 最佳实践

1. **使用 Table-Driven Tests**: 使用表格驱动测试覆盖多种场景
2. **清理资源**: 使用 `t.Cleanup()` 或 `defer` 清理测试资源
3. **并行测试**: 使用 `t.Parallel()` 加速独立测试
4. **跳过测试**: 使用 `t.Skip()` 跳过需要特定条件的测试

## 测试报告

运行测试后，可以生成以下报告：

- 控制台输出：显示每个测试的执行结果
- 覆盖率报告：显示代码覆盖率百分比
- HTML 报告：可视化显示覆盖的代码行

## 常见问题

### Q: 如何测试需要数据库连接的代码？

A: 使用 `sqlmock` 模拟数据库连接，或者使用 Docker 容器运行临时数据库实例。

### Q: 如何测试 `StartTask` 并发场景且不依赖真实 MySQL？

A: `TaskService` 提供了用于测试的注入点，可在单元测试中替换启动链路中的外部依赖：

- `initRuntimeFn`：替代真实数据库初始化，返回测试 runtime；
- `executeSyncFn`：替代异步同步执行函数，仅用于捕获启动行为；
- 然后在测试中并发调用 `StartTask`，断言 `runtimes[taskID]` 存在且不同任务对应不同 runtime 实例。

这种方式可以稳定验证并发行为，不受本地 MySQL、网络或权限影响。

### Q: 如何测试私有方法？

A: 在同一个包中编写测试文件，可以直接访问私有方法。

### Q: 如何处理测试中的错误？

A: 使用 `t.Error()`、`t.Fatal()` 或 `require` 包来处理测试断言失败。

## 参考资源

- [Go Testing Package](https://golang.org/pkg/testing/)
- [Testify](https://github.com/stretchr/testify)
- [go-sqlmock](https://github.com/DATA-DOG/go-sqlmock)
- [miniredis](https://github.com/alicebob/miniredis)
