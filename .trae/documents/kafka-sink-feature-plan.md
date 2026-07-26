# Kafka Sink 功能实施计划

## 摘要

为增量同步链路增加 Kafka 目标端能力。沿用 [plans/incremental-multi-sink-design.md](file:///d:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/plans/incremental-multi-sink-design.md) 的 Sink 抽象思路，落地 `ChangeEvent` + `Sink` 接口 + `SinkFactory`，先把现有 MySQL 增量写入重构为 `MySQLSink`，再实现 `KafkaSink`（使用已引入的 `segmentio/kafka-go`）。配置契约对齐前端已预埋的 `sink_configs`（复数数组，支持多 Sink）。本阶段不实现 Webhook Sink，保留扩展点。

**交付语义**：At-Least-Once（Sink 写入成功后才推进 binlog checkpoint）。
**适用范围**：仅增量同步（INCREMENTAL）。FULL/ALL 模式当 sink 非 MYSQL 时拒绝启动。

---

## 假设与决策（用户未答，采用合理默认）

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 实现范围 | Sink 抽象 + MySQLSink 适配 + KafkaSink（不含 Webhook） | 既保证架构干净（后续扩展零成本），又控制交付范围 |
| 配置契约 | 对齐前端 `sink_configs`（复数数组） | 前端 App.vue 已完整实现并发送该字段，改后端兼容成本最低 |
| Topic 路由 | 单 topic（`routing_mode=single_topic`） | 设计文档默认方案，实现最简；预留字段供后续扩展 per_table |
| 全量到 Kafka | 不支持，仅增量 | 设计文档明确多目标仅作用于增量；FULL/ALL + 非 MYSQL sink 拒绝启动 |
| Kafka 安全 | 第一阶段仅明文连接（无 SASL/SSL） | 控制复杂度；SASL/SSL 留作后续扩展点 |
| 一致性语义 | At-Least-Once | Sink Write 成功后才 SavePosition |
| Kafka 客户端 | `segmentio/kafka-go`（已在 go.mod） | 设计文档选型，已引入依赖 |

---

## 当前状态分析

### 已具备
- `go.mod` 已引入 `github.com/segmentio/kafka-go v0.4.50`（未使用）
- 前端 `web/src/App.vue` 已预埋 Kafka UI：`SINK_TYPES`、`singleKafkaConfig`、`sinkConfigsPayload`，发送 `sink_configs: [{type, options}]`，Kafka options 含 `brokers/topic/key_mode/batch_size/required_acks`，并支持回填
- `pkg/binlog.BinlogEvent` 已是统一事件模型（含 BeforeImage/Rows/Position）
- `checkpoint.Manager` 可复用（SavePosition/GetPosition）
- 设计文档 `plans/incremental-multi-sink-design.md` 已定义 ChangeEvent/Sink/SinkFactory 蓝图

### 缺失
- 后端 `TaskConfig`（[task.go:74](file:///d:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/domain/entity/task.go#L74)）无 sink 字段
- `task_handler.go` 的 `CreateTaskRequest`（[task_handler.go:117](file:///d:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/api/handler/task_handler.go#L117)）不接收 `sink_configs`
- 无 `Sink` 接口、`ChangeEvent`、`SinkFactory`、`KafkaSink`
- `IncrementalSyncService`（[sync_service.go:43](file:///d:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/sync/application/sync_service.go#L43)）直接持有 `writers map[string]*writer.BufferedWriter`，与 MySQL 强耦合
- `executeIncrementalSync`（[task_service.go:3943](file:///d:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/application/service/task_service.go#L3943)）未传递 sink 配置

### 关键耦合点
`IncrementalSyncService.Start`（[sync_service.go:140-262](file:///d:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/sync/application/sync_service.go#L140)）在 232-258 行直接为每张表创建 `BufferedWriter` 并 `EnableUpsert()`；`syncEventHandler`（[sync_service.go:481](file:///d:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/sync/application/sync_service.go#L481)）按 EventType 分发到 `handleInsert/handleUpdate/handleDelete` 直接操作 `s.writers[key]`。这是重构主切入点。

---

## 提议变更

### 1. 新增 Sink 领域模型
**文件**：`internal/sync/domain/sink/sink.go`（新建）
**内容**：定义 `SinkType` 常量（MYSQL/KAFKA）、`SinkConfig`（Type + Options map）、`ChangeEvent`、`Sink` 接口、可选 `BatchSink`/`TablePreparer` 接口。

```go
type SinkType string
const (
    SinkTypeMySQL SinkType = "MYSQL"
    SinkTypeKafka SinkType = "KAFKA"
)

type SinkConfig struct {
    Type    SinkType               `json:"type"`
    Options map[string]interface{} `json:"options,omitempty"`
}

type ChangeEvent struct {
    TaskID       string
    SourceSchema string
    SourceTable  string
    EventType    string // INSERT/UPDATE/DELETE
    EventTime    time.Time
    BinlogFile   string
    BinlogPos    uint32
    PrimaryKeys  map[string]interface{}
    Before       map[string]interface{}
    After        map[string]interface{}
    TraceID      string
}

type Sink interface {
    Type() string
    Open(ctx context.Context) error
    Write(ctx context.Context, event *ChangeEvent) error
    Flush(ctx context.Context) error
    Close(ctx context.Context) error
}

// MySQLSink 实现，用于预建表 writer
type TablePreparer interface {
    PrepareTables(ctx context.Context, dbMapping map[string]string, tables []string) error
}
```
**为什么**：接口放 domain 层，不依赖基础设施；`TablePreparer` 用类型断言保留 MySQL 预建 writer 的快速失败行为，不污染 Sink 接口。

### 2. 新增事件归一化器
**文件**：`internal/sync/application/event_normalizer.go`（新建）
**内容**：把 `pkg/binlog.BinlogEvent` 转成 `*ChangeEvent`。INSERT 用 Rows[0] 作 After；UPDATE 用 BeforeImage[0] 作 Before、Rows[0] 作 After；DELETE 用 BeforeImage[0] 作 Before。从 identity 提取 PrimaryKeys。
**为什么**：隔离 canal/go-mysql 细节，Sink 只认 ChangeEvent。

### 3. 新增 SinkFactory
**文件**：`internal/sync/infrastructure/sink/factory.go`（新建）
**内容**：`NewSinks(configs []SinkConfig, deps SinkDeps) ([]Sink, error)`。`SinkDeps` 携带 MySQLSink 所需的 targetDB/analyzer/batchSize。根据 Type 创建 MySQLSink/KafkaSink。校验 Type 合法性、必填 options。
**为什么**：集中创建逻辑，核心编排不感知具体实现。

### 4. 新增 MySQLSink
**文件**：`internal/sync/infrastructure/sink/mysql/sink.go`（新建）
**内容**：封装现有 `writer.BufferedWriter`。
- `Open`：获取 writeConn，`SET SESSION FOREIGN_KEY_CHECKS=0`
- `PrepareTables`：遍历 dbMapping/tables，`analyzer.AnalyzeTable` 后创建 BufferedWriter + `EnableUpsert()`，缓存到内部 `map[string]*writer.BufferedWriter`（key=`srcDB.table`）
- `Write`：按 EventType 调用 BufferedWriter 的 WriteBatch/UpdateWithBeforeImage/Delete；UPDATE 时检测 identity 变化走 `deleteAndUpsert`（迁移自现有 [sync_service.go:640](file:///d:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/sync/application/sync_service.go#L640)）
- `Flush`/`Close`：委托 BufferedWriter
**为什么**：保持现有 MySQL 增量行为完全不变（兼容性基准）。

### 5. 新增 KafkaSink
**文件**：`internal/sync/infrastructure/sink/kafka/sink.go`（新建）
**内容**：使用 `github.com/segmentio/kafka-go` 的 `kafka.Writer`。
- options：`brokers`([]string)、`topic`(string)、`key_mode`("pk"/"none"，默认 pk)、`batch_size`(int，默认 1000)、`batch_timeout_ms`(int，默认 500)、`required_acks`(int，默认 1)、`routing_mode`("single_topic"，预留)
- `Open`：创建 `kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Async: false, RequiredAcks: required_acks, BatchSize: batchSize, BatchTimeout: ...}`
- `Write`：序列化 ChangeEvent 为 JSON（含 task_id/source_schema/source_table/event_type/event_time/binlog_file/binlog_pos/before/after）；key 按 key_mode 取主键拼接或 `schema.table:binlog_pos`；调用 `writer.WriteMessages` 同步发送
- `Flush`：kafka-go 同步 writer 无需显式 flush（no-op）
- `Close`：关闭 kafka.Writer（幂等）
- 重试：kafka-go 内置重试，第一阶段不额外加业务重试层
**为什么**：事件投递标准化为 JSON，下游易消费；同步发送保证 At-Least-Once。

### 6. 改造 IncrementalSyncService
**文件**：`internal/sync/application/sync_service.go`（修改）
**变更**：
- 结构体：`writers map[string]*writer.BufferedWriter` → `sinks []sink.Sink`；移除 `identities`/`targetSchemas`/`writeConn`（下沉到 MySQLSink）
- `Start` 方法（140-262 行）：移除直接建 writer 逻辑；改为 `s.sinks = factory.NewSinks(...)`；遍历 sinks 调 `Open`；对实现 `TablePreparer` 的调 `PrepareTables(dbMapping, tables)`
- `syncEventHandler.OnEvent`（481 行）：调 `normalizer.ToChangeEvent(binlogEvent)` 生成 ChangeEvent；遍历 `s.sinks` 逐个 `Write`，全部成功后 `checkpointMgr.SavePosition`；任一失败返回错误（上层标记 FAILED，不推进位点）
- 删除 `handleInsert/handleUpdate/handleDelete`（逻辑迁入 MySQLSink.Write）
- `SyncConfig` 增加 `SinkConfigs []sink.SinkConfig` 字段
**为什么**：解耦事件订阅与目标写入，支撑多 Sink。

### 7. TaskConfig 增加 SinkConfigs
**文件**：`internal/task/domain/entity/task.go`（修改，[task.go:74-100](file:///d:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/domain/entity/task.go#L74)）
**变更**：`TaskConfig` 末尾增加 `SinkConfigs []sink.SinkConfig`（json `sink_configs,omitempty`）。为避免循环依赖，`SinkConfig` 定义在 `internal/sync/domain/sink`，task 实体 import 它（task 已依赖 sync 包语义；若循环，则在 task 包内定义等价结构并转换）。
**兼容**：老任务 `sink_configs` 为空，启动时默认填充 `[{type:MYSQL}]`。

### 8. API 请求/响应扩展
**文件**：`internal/api/handler/task_handler.go`（修改，[task_handler.go:117-138](file:///d:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/api/handler/task_handler.go#L117)）
**变更**：
- `CreateTaskRequest` / `UpdateTaskRequest` 增加 `SinkConfigs []SinkConfigRequest`（json `sink_configs,omitempty`）
- `SinkConfigRequest{ Type string; Options map[string]interface{} }`
- 创建/更新时映射到 `TaskConfig.SinkConfigs`
- 向后兼容：请求未传 sink_configs 时，若 Mode=INCREMENTAL 且配置了非 MYSQL sink 则校验 brokers 非空；否则默认 MYSQL
- 响应回显 `sink_configs`（前端回填已就绪）
**为什么**：对齐前端契约，不破坏老 API。

### 9. executeIncrementalSync 传递 sink 配置 + FULL 模式拦截
**文件**：`internal/task/application/service/task_service.go`（修改，[task_service.go:3943](file:///d:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/application/service/task_service.go#L3943)）
**变更**：
- `executeIncrementalSync`：把 `task.Config.SinkConfigs` 填入 `syncApp.SyncConfig`
- `StartTask`（1259 行）或 `executeFullSync`：增加校验——若 `SinkConfigs` 含非 MYSQL 类型且 Mode 为 FULL 或 ALL，返回错误拒绝启动（"Kafka sink 仅支持 INCREMENTAL 模式"）
- ALL 模式：若 sink 非 MYSQL，全量阶段无法投递，直接拒绝
**为什么**：Kafka 不支持全量存量投递，必须显式拦截。

### 10. 配置示例与文档
**文件**：`etc/application.toml.example`（修改，仅在需要全局默认时）
**决策**：第一阶段 Kafka 配置只在任务级 `sink_configs.options` 表达，不新增全局 `[kafka]` 段（符合设计文档 5.3：目标类型属任务属性）。
**文档**：更新 `README.md` API 接口段补充 `sink_configs` 字段说明；更新 `docs/CONFIGURATION.md`（若涉及）。

---

## 数据流（改造后）

```
MySQL Binlog
  -> pkg/binlog.Subscriber
    -> BinlogEvent
      -> event_normalizer.ToChangeEvent
        -> ChangeEvent
          -> for each sink in IncrementalSyncService.sinks:
               sink.Write(ChangeEvent)
             全部成功 -> checkpointMgr.SavePosition
             任一失败 -> 返回错误，任务 FAILED，位点不推进
```

---

## 测试规划

### 单元测试
- `event_normalizer_test.go`：INSERT/UPDATE/DELETE 三类 BinlogEvent → ChangeEvent 转换正确性（Before/After/PrimaryKeys）
- `sink/factory_test.go`：各 SinkType 创建、非法 Type 报错、必填 options 校验
- `sink/mysql/sink_test.go`：用 `go-sqlmock` 验证 INSERT/UPDATE/DELETE SQL 与现有行为一致；UPDATE 主键变化走 deleteAndUpsert
- `sink/kafka/sink_test.go`：用 `kafka-go` 的 `mock` 或抽象 Writer 接口验证 JSON 序列化、key 生成（pk 模式 vs none 模式）、options 默认值
- `task_handler_test.go`：创建带 sink_configs 的任务、老任务兼容（无 sink_configs 默认 MYSQL）、非 MYSQL + FULL 拒绝

### 集成测试
- MySQL → MySQL 增量回归（确保 MySQLSink 不退化）
- MySQL → Kafka 端到端（需本地 Kafka，可用 `testcontainers` 或标记 `// +build integration` 跳过 CI）
- checkpoint 恢复：Kafka 写入成功后位点推进；模拟 Sink 失败位点不推进

### 故障测试
- Kafka 不可用时任务进入 FAILED、位点不推进
- 重启后从最近 checkpoint 重新消费（At-Least-Once 验证）

### 验证命令
```bash
go test ./internal/sync/...
go test ./internal/task/...
go test ./internal/api/...
go vet ./...
cd web && npm run build   # 前端契约对齐验证
```

---

## 实施顺序（严格按序）

1. 新增 `internal/sync/domain/sink/sink.go`（接口 + ChangeEvent + SinkConfig）
2. 新增 `event_normalizer.go` + 单测
3. 新增 `sink/factory.go` + `sink/mysql/sink.go`（封装现有 writer）+ 单测
4. 改造 `IncrementalSyncService` 持有 `[]Sink`，删除直接 writer 调用；MySQL 增量回归通过
5. `TaskConfig` 增加 `SinkConfigs`；`task_handler` 请求/响应扩展；老任务兼容
6. `executeIncrementalSync` 传 sink 配置；FULL/ALL 非 MYSQL 拦截
7. 新增 `sink/kafka/sink.go` + 单测
8. 端到端验证 + 文档更新

---

## 风险与规避

| 风险 | 规避 |
|------|------|
| MySQLSink 重构导致增量行为退化 | 步骤 4 后必须跑全量 MySQL 增量回归测试，行为对齐现有 handleInsert/Update/Delete |
| task 包 import sync/domain/sink 循环依赖 | 若循环，在 task 包内定义 SinkConfig 镜像结构并提供转换函数 |
| Kafka 同步发送阻塞影响延迟 | batch_size/batch_timeout_ms 可调；同步发送是 At-Least-Once 的必要代价 |
| 老任务无 sink_configs 字段 | 启动时默认填充 MYSQL，存储层 JSON 反序列化空值兼容 |
| 前端 sink_configs 与设计文档 sink 单数不一致 | 本计划明确对齐前端 sink_configs（复数），设计文档视为历史参考 |

---

## 验收标准

1. 创建 `mode=INCREMENTAL` + `sink_configs=[{type:KAFKA, options:{brokers, topic}}]` 的任务，启动后 INSERT/UPDATE/DELETE 事件能投递到 Kafka topic，消息体为标准 JSON
2. Kafka 写入失败时任务进入 FAILED，binlog 位点不推进；重启后从上次位点重放
3. 老任务（无 sink_configs）继续以 MySQL 增量方式运行，行为无变化
4. `mode=FULL` 或 `ALL` + 非 MYSQL sink 被拒绝启动并返回明确错误
5. 前端创建 Kafka 任务、回填编辑、MULTI 多 Sink 模式均可正常工作
6. `go test ./...` 与 `go vet ./...` 通过，`cd web && npm run build` 通过
