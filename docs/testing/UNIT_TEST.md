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
mysql-to-async/
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
│       │       └── task_service_test.go
│       └── domain/
│           └── entity/
│               └── task_test.go
└── pkg/
    ├── logger/
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
| TestPauseTask | 测试暂停任务 |
| TestTaskStatus | 测试任务状态转换 |

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

### Metadata 模块

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

### Q: 如何测试私有方法？

A: 在同一个包中编写测试文件，可以直接访问私有方法。

### Q: 如何处理测试中的错误？

A: 使用 `t.Error()`、`t.Fatal()` 或 `require` 包来处理测试断言失败。

## 参考资源

- [Go Testing Package](https://golang.org/pkg/testing/)
- [Testify](https://github.com/stretchr/testify)
- [go-sqlmock](https://github.com/DATA-DOG/go-sqlmock)
- [miniredis](https://github.com/alicebob/miniredis)