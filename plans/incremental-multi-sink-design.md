# 增量多目标同步能力规划与设计文档

## 1. 文档目标

本文档用于指导当前项目从“增量同步仅支持 MySQL 目标端”演进到“增量同步支持多种目标类型（MySQL、Kafka、消息队列等）”。

本文档是实现基线，后续开发必须遵守以下原则：

1. 不推翻现有全量同步架构，优先复用现有任务模型、Binlog 订阅模型、checkpoint 机制。
2. 多目标能力仅先作用于增量同步链路，全量同步暂不扩展到 Kafka/MQ。
3. 第一阶段必须先抽象统一 Sink 框架，再接入具体目标类型，禁止直接在现有 `IncrementalSyncService` 上堆 `if/else`。
4. 第一批正式支持的目标类型限定为：`MYSQL`、`KAFKA`、`HTTP_WEBHOOK`。
5. `ROCKETMQ`、`RABBITMQ`、`PULSAR` 作为第二阶段扩展点预留，不进入第一阶段交付范围。

## 2. 当前项目现状

基于当前代码实现，现状如下：

- 任务模式编排在 [internal/task/application/service/task_service.go](D:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/application/service/task_service.go) 中，已经支持 `FULL`、`INCREMENTAL`、`ALL`。
- 增量同步入口在 [internal/task/application/service/task_service.go](D:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/application/service/task_service.go) 中，当前由 `executeIncrementalSync` 创建 `IncrementalSyncService`。
- 增量同步主服务在 [internal/sync/application/sync_service.go](D:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/sync/application/sync_service.go) 中，核心链路是：
  `Binlog Subscriber -> syncEventHandler -> writer.BufferedWriter/BatchWriter -> MySQL target`
- Binlog 事件模型在 [pkg/binlog/subscriber.go](D:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/pkg/binlog/subscriber.go) 中，已具备统一事件结构 `BinlogEvent`。
- 当前 checkpoint 已经抽象为 `checkpoint.Manager`，这对多目标增量同步完全可复用。
- 当前任务配置定义在 [internal/task/domain/entity/task.go](D:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/domain/entity/task.go)，但目标端配置仍然是“数据库导向”的，不适合表达 Kafka、Webhook 等目标。

### 2.1 当前架构的优势

- 已有统一的 Binlog 事件入口。
- 已有统一任务生命周期管理。
- 已有 checkpoint 能力。
- 已有表身份识别能力，可继续服务于 MySQL Sink。
- 增量同步已被封装为独立服务，具备可替换内部写入器的条件。

### 2.2 当前架构的主要问题

- `IncrementalSyncService` 同时承担了事件订阅、事件转换、目标写入、审计处理多个职责，耦合较重。
- 当前目标端写入逻辑直接绑定 MySQL writer，导致无法自然扩展到消息类目标。
- 任务配置中的 `TargetSchema`、`TargetDatabases` 等字段天然偏向数据库，不适合作为通用目标定义。
- checkpoint 当前按“任务已消费到某 binlog 位点”保存，尚未显式区分“事件已安全送达目标端再提交位点”的语义边界。

## 3. 建设目标

### 3.1 业务目标

系统在增量同步模式下，能够把 MySQL Binlog 变更事件投递到不同目标类型，至少覆盖：

- MySQL
- Kafka
- HTTP Webhook

并满足以下能力：

- 支持表级和库级订阅。
- 支持 INSERT、UPDATE、DELETE 三类事件。
- 支持断点续传。
- 支持失败重试。
- 支持每个任务配置独立目标类型。
- 支持后续平滑扩展更多 Sink，而不改动核心编排。

### 3.2 非功能目标

- 不影响现有全量同步能力。
- 不破坏现有 MySQL 增量同步行为。
- 对单目标任务，增量延迟目标仍控制在秒级。
- 新增一种 Sink 的改动范围必须限制在独立包内，原则上不修改核心订阅链路。

## 4. 总体设计思路

本次改造采用“事件标准化 + Sink 插件化 + 任务配置通用化”的方案。

核心思路：

1. `pkg/binlog` 继续负责产出原始变更事件。
2. 新增“标准增量事件模型”，把原始 Binlog 事件转换为统一投递对象。
3. 新增 `Sink` 抽象接口，由不同目标类型分别实现。
4. `IncrementalSyncService` 不再直接持有 MySQL writer，而是持有一个或多个 `Sink`。
5. checkpoint 提交时机从“事件处理完成”收敛为“Sink 成功确认投递完成”。

## 5. 技术选型

### 5.1 第一阶段目标类型选型

#### MySQL

用途：保持兼容现有能力，作为默认目标。

选型结论：

- 继续复用现有 `writer.BufferedWriter`、`writer.BatchWriter`。
- 将其封装为 `MySQLSink`，而不是直接被 `IncrementalSyncService` 调用。

原因：

- 当前能力最成熟，改造成本最低。
- 仍然需要表 identity、before image、schema 映射等数据库语义。

#### Kafka

用途：承接 CDC 事件流、作为下游异步处理总线。

选型结论：

- Go 客户端优先选 `segmentio/kafka-go`。

原因：

- API 简单，接入成本低。
- 对当前项目体量更友好。
- 足够支撑第一阶段“单 topic / 多 topic 路由、批量写入、重试”需求。

不选复杂客户端的原因：

- 第一阶段不追求事务消息、幂等生产者、超复杂路由能力。
- 应优先控制实现复杂度，避免把问题放大为消息中间件平台项目。

#### HTTP Webhook

用途：为外部系统提供轻量事件通知出口，便于快速集成。

选型结论：

- 使用标准库 `net/http` 即可，不额外引入第三方 HTTP SDK。

原因：

- 需求简单，避免不必要依赖。
- 便于用户接企业内部网关、函数计算、业务服务。

### 5.2 第二阶段预留目标

- RocketMQ：适合国内企业内部场景，但第一阶段先不接入。
- RabbitMQ：适合传统消息队列场景，后续可通过相同 Sink 接口接入。
- Pulsar：偏平台化，先不进入当前项目最小可交付范围。

### 5.3 配置格式选型

选型结论：

- 任务维度新增通用目标配置结构。
- 全局配置文件 `application.toml` 只保留默认连接与公共参数，不承载完整多目标任务定义。

原因：

- 当前系统是任务中心模型，目标类型本质上属于任务属性，不是进程全局属性。
- 如果把多目标配置全部塞进全局配置，会让任务并发场景下的隔离变差。

## 6. 目标架构设计

建议新增分层如下：

```text
MySQL Binlog Subscriber
    -> Event Normalizer
        -> Sink Router
            -> MySQL Sink
            -> Kafka Sink
            -> HTTP Webhook Sink
    -> Checkpoint Committer
```

### 6.1 标准事件模型

新增统一事件对象，建议命名为 `ChangeEvent`：

```go
type ChangeEvent struct {
    TaskID         string
    SourceSchema   string
    SourceTable    string
    EventType      string
    EventTime      time.Time
    BinlogFile     string
    BinlogPos      uint32
    PrimaryKeys    map[string]interface{}
    Before         map[string]interface{}
    After          map[string]interface{}
    RawRows        []map[string]interface{}
    TraceID        string
}
```

设计原则：

- 对外部 Sink 暴露统一语义，不暴露 canal/go-mysql 细节。
- 对 UPDATE 事件同时保留 `Before` 与 `After`。
- 对 MySQL Sink 允许使用原始 identity 信息进行精确更新删除。

### 6.2 Sink 抽象

新增统一接口，建议定义在：
[internal/sync/domain/sink/sink.go](D:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/sync/domain/sink/sink.go)

```go
type Sink interface {
    Type() string
    Open(ctx context.Context) error
    Write(ctx context.Context, event *ChangeEvent) error
    Flush(ctx context.Context) error
    Close(ctx context.Context) error
}
```

如需批量能力，可扩展第二版：

```go
type BatchSink interface {
    Sink
    WriteBatch(ctx context.Context, events []*ChangeEvent) error
}
```

约束：

- `Write` 成功返回才允许推进 checkpoint。
- `Flush` 用于批量 Sink 的主动提交。
- `Close` 必须幂等。

### 6.3 Sink Router

新增 `SinkFactory` 与 `SinkRouter`：

- `SinkFactory`：根据任务目标配置创建具体 Sink。
- `SinkRouter`：负责把标准事件路由到目标 Sink。

第一阶段每个任务只允许一个主目标类型，但内部保留“一个任务多个 Sink”的结构，为第二阶段多播做准备。

### 6.4 Checkpoint 语义

必须明确新规则：

- 只有当事件被目标 Sink 成功处理后，才允许保存对应 binlog 位点。
- 如果 Sink 写入失败，则当前事件视为未完成，checkpoint 不推进。
- 系统重启后允许从最近成功 checkpoint 重新消费，接受“至少一次投递”语义。

结论：

- 第一阶段一致性语义定义为：`At-Least-Once`。
- 不承诺端到端 `Exactly-Once`。

这是必须写进文档和代码注释里的约束，避免后续误判。

## 7. 任务配置设计

当前 [internal/task/domain/entity/task.go](D:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns/internal/task/domain/entity/task.go) 中的目标配置过于数据库化，必须升级。

建议新增：

```go
type SinkType string

const (
    SinkTypeMySQL       SinkType = "MYSQL"
    SinkTypeKafka       SinkType = "KAFKA"
    SinkTypeHTTPWebhook SinkType = "HTTP_WEBHOOK"
)

type SinkConfig struct {
    Type    SinkType               `json:"type"`
    Options map[string]interface{} `json:"options"`
}
```

然后在 `TaskConfig` 中新增：

```go
Sink SinkConfig `json:"sink"`
```

### 7.1 第一阶段各目标的 Options 约定

#### MYSQL

```json
{
  "type": "MYSQL",
  "options": {
    "target_schema": "test_target",
    "target_databases": ["db1_target", "db2_target"]
  }
}
```

说明：

- 第一阶段 MySQL Sink 继续兼容 `TargetSchema`、`TargetDatabases`，但新接口优先读 `sink.options`。
- 老字段保留一个过渡版本，后续再逐步废弃。

#### KAFKA

```json
{
  "type": "KAFKA",
  "options": {
    "brokers": ["127.0.0.1:9092"],
    "topic": "mysql_cdc",
    "routing_mode": "single_topic",
    "key_mode": "pk",
    "required_acks": 1,
    "batch_size": 100,
    "batch_timeout_ms": 500
  }
}
```

#### HTTP_WEBHOOK

```json
{
  "type": "HTTP_WEBHOOK",
  "options": {
    "url": "http://127.0.0.1:9000/cdc/event",
    "method": "POST",
    "timeout_ms": 3000,
    "headers": {
      "Authorization": "Bearer xxx"
    },
    "retry_times": 3
  }
}
```

## 8. 模块拆分设计

建议新增目录结构：

```text
internal/sync/
  application/
    incremental_service.go
    event_normalizer.go
  domain/
    sink/
      sink.go
      event.go
  infrastructure/
    sink/
      factory.go
      mysql/
        sink.go
      kafka/
        sink.go
      webhook/
        sink.go
```

### 8.1 各模块职责

- `event_normalizer.go`
  负责把 `pkg/binlog.BinlogEvent` 转成标准 `ChangeEvent`。

- `domain/sink/sink.go`
  只放接口，不依赖基础设施实现。

- `infrastructure/sink/factory.go`
  根据任务配置实例化目标 Sink。

- `infrastructure/sink/mysql/sink.go`
  封装现有 MySQL writer。

- `infrastructure/sink/kafka/sink.go`
  负责事件序列化、topic 路由、发送确认、重试。

- `infrastructure/sink/webhook/sink.go`
  负责 HTTP 投递、超时控制、重试。

## 9. 关键实现策略

### 9.1 MySQL Sink 策略

实现要求：

- 复用现有 `AnalyzeTable`、`BufferedWriter`、`BatchWriter`。
- INSERT、UPDATE、DELETE 行为必须与当前实现保持一致。
- 无主键表仍沿用 `before image + FullColumnsStrategy`。

这是兼容性基准，不能退化。

### 9.2 Kafka Sink 策略

事件消息体建议统一为 JSON：

```json
{
  "task_id": "task_001",
  "source_schema": "db1",
  "source_table": "user",
  "event_type": "UPDATE",
  "event_time": "2026-04-01T10:00:00+08:00",
  "binlog_file": "mysql-bin.000001",
  "binlog_pos": 12345,
  "before": {...},
  "after": {...}
}
```

关键规则：

- `key` 默认取主键拼接值；无主键表则退化为 `schema.table + binlog position`。
- 第一阶段默认单 topic。
- 发送成功后才提交 checkpoint。
- 同一个任务内部事件处理顺序优先保证“单表近似有序”，不强求全局严格有序。

### 9.3 Webhook Sink 策略

关键规则：

- 每条事件单独 POST。
- 返回 HTTP 2xx 视为成功。
- 非 2xx 视为失败并重试。
- 重试耗尽后当前任务进入失败或暂停，由配置决定；第一阶段建议直接失败。

### 9.4 错误处理策略

统一规则：

- Sink 初始化失败：任务启动失败。
- Sink 写入失败：任务状态改为 `FAILED`，停止继续推进 checkpoint。
- Sink Flush 失败：视同写入失败。
- 审计日志失败：仅记录告警，不阻断主流程。

## 10. 接口与兼容性设计

### 10.1 API 请求体变更

当前创建任务接口需扩展 `sink` 字段。

新增后示例：

```json
{
  "name": "订单增量到Kafka",
  "mode": "INCREMENTAL",
  "sync_level": "TABLE",
  "source_schema": "trade",
  "tables": ["orders"],
  "batch_size": 500,
  "worker_count": 4,
  "sink": {
    "type": "KAFKA",
    "options": {
      "brokers": ["127.0.0.1:9092"],
      "topic": "mysql_cdc"
    }
  }
}
```

### 10.2 向后兼容策略

第一阶段必须兼容老任务：

- 若请求体未传 `sink`，则默认生成 `sink.type=MYSQL`。
- 若老字段 `target_schema`、`target_databases` 存在，则映射到 MySQL Sink 配置。
- 老任务无需迁移即可继续运行。

## 11. 实施阶段规划

严格按以下顺序推进，禁止跳步。

### 阶段 1：事件模型与 Sink 接口抽象

交付内容：

- 新增标准 `ChangeEvent`
- 新增 `Sink` 接口
- 新增 `SinkFactory`
- `IncrementalSyncService` 改造成面向 `Sink` 编程

验收标准：

- 代码中不再直接由 `IncrementalSyncService` 操作 MySQL writer。

### 阶段 2：MySQL Sink 适配

交付内容：

- 实现 `MySQLSink`
- 老增量同步行为保持不变

验收标准：

- 原有 MySQL 增量任务回归测试全部通过。

### 阶段 3：任务配置与 API 升级

交付内容：

- `TaskConfig` 增加 `SinkConfig`
- API 支持创建不同目标类型的任务
- 老字段兼容逻辑完成

验收标准：

- 老任务和新任务都能创建并启动。

### 阶段 4：Kafka Sink

交付内容：

- 实现 `KafkaSink`
- 支持单 topic 写入
- 支持 key 策略
- 支持基础重试

验收标准：

- INSERT/UPDATE/DELETE 能投递到 Kafka。

### 阶段 5：Webhook Sink

交付内容：

- 实现 `WebhookSink`
- 支持基础认证头、超时、重试

验收标准：

- 事件可成功推送到 HTTP 服务并在失败时终止任务。

### 阶段 6：监控与可观测性补强

交付内容：

- 按 Sink 维度输出吞吐、失败数、最后成功位点、重试次数
- 任务详情页展示目标类型

验收标准：

- 能区分是 Binlog 读取问题还是目标端投递问题。

## 12. 测试规划

必须覆盖以下测试层次：

### 单元测试

- `ChangeEvent` 转换正确性
- `SinkFactory` 创建逻辑
- `MySQLSink` 的 insert/update/delete 路径
- `KafkaSink` 的序列化、key 生成、重试逻辑
- `WebhookSink` 的状态码处理、超时、重试逻辑

### 集成测试

- MySQL -> MySQL 增量回归
- MySQL -> Kafka 端到端
- MySQL -> Webhook 端到端
- checkpoint 恢复测试

### 故障测试

- Kafka 不可用
- Webhook 返回 500
- checkpoint 保存失败
- Binlog 重连后恢复

## 13. 风险与规避

### 风险 1：把多目标逻辑继续堆进现有增量服务

后果：

- 代码迅速失控，后续接入 RocketMQ/RabbitMQ 成本激增。

规避：

- 第一阶段必须先完成 `Sink` 抽象。

### 风险 2：过早追求 Exactly-Once

后果：

- 实现复杂度远超项目现阶段，导致迟迟无法交付。

规避：

- 第一阶段明确采用 `At-Least-Once`。

### 风险 3：任务配置模型设计不通用

后果：

- 每新增一种目标类型都要改任务结构。

规避：

- 使用 `sink.type + sink.options` 的开放式模型。

### 风险 4：Kafka/Webhook 和 MySQL Sink 共享同一套数据库语义

后果：

- 抽象被 MySQL 特性绑死。

规避：

- `ChangeEvent` 只表达变更事实，不表达数据库内部 writer 细节。

## 14. 最终实施结论

本项目的正确演进路线不是“在现有增量同步代码里继续加目标类型判断”，而是：

1. 抽象统一 `ChangeEvent`
2. 抽象统一 `Sink`
3. 先把现有 MySQL 增量能力改造成 `MySQLSink`
4. 再接 Kafka
5. 再接 Webhook

第一阶段的最终标准方案如下：

- 增量同步统一事件模型：`ChangeEvent`
- 交付语义：`At-Least-Once`
- 首批目标类型：`MYSQL`、`KAFKA`、`HTTP_WEBHOOK`
- 扩展机制：`SinkFactory + Sink Interface`
- 兼容策略：老任务默认映射为 `MYSQL Sink`

## 15. 建议的下一步开发顺序

如果严格按本文档执行，建议直接按下面的开发顺序落地：

1. 新增 `ChangeEvent` 和 `Sink` 接口
2. 重构 `IncrementalSyncService`
3. 落地 `MySQLSink`
4. 改造 `TaskConfig` 和 API
5. 落地 `KafkaSink`
6. 落地 `WebhookSink`
7. 补齐测试与监控

这份文档对应的是“最稳妥、最适合你当前代码结构”的方案，不建议偏离。
