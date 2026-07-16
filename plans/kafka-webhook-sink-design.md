# Kafka + Webhook Sink 设计与实施文档

> 关联文档：[plans/incremental-multi-sink-design.md](./incremental-multi-sink-design.md)（增量多目标架构蓝图）
>
> 本文是上述蓝图的落地实施计划，范围已扩展为 **Kafka + Webhook**，并补充 SASL/SSL、per-table topic 等细节。旧的 `.trae/documents/kafka-sink-feature-plan.md`（窄范围：仅 Kafka 明文单 topic）已被本文取代。

## 摘要

为增量同步链路增加 **Kafka** 与 **HTTP Webhook** 两种目标端能力。采用「事件标准化 + Sink 插件化」方案：先抽象统一 `Sink` 接口与 `ChangeEvent`，把现有 MySQL 增量写入重构为 `MySQLSink`，再实现 `KafkaSink`（含 SASL/SSL、per-table topic）与 `WebhookSink`。配置契约对齐前端已预埋的 `sink_configs`（复数数组）。

- **交付语义**：At-Least-Once（所有 Sink 写入成功后才推进 binlog checkpoint）。
- **适用范围**：仅增量同步（`INCREMENTAL`）。`FULL`/`ALL` 模式当 sink 含非 MYSQL 类型时拒绝启动。
- **客户端**：Kafka 使用已引入的 `github.com/segmentio/kafka-go v0.4.50`；Webhook 使用标准库 `net/http`。无需新增外部依赖。

---

## 假设与决策（已确认锁定）

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 架构方式 | 完整 Sink 抽象重构 | 先抽象 `Sink` 接口 + `ChangeEvent`，MySQL 增量改造成 `MySQLSink`，再接入 Kafka/Webhook。符合 `plans/incremental-multi-sink-design.md` 原则，扩展性好 |
| 交付范围 | Kafka + Webhook | 覆盖设计文档首批三种目标（MYSQL/KAFKA/HTTP_WEBHOOK） |
| SASL 机制 | PLAIN + SCRAM-SHA-256/512 | kafka-go 纯 Go 原生支持，覆盖绝大多数生产场景，无需 cgo |
| TLS | CA 证书校验 + 可选 mTLS（客户端证书/密钥） | 覆盖单向与双向认证 |
| Topic 路由 | `single_topic`（默认）+ `per_table`（`{prefix}.{schema}.{table}`） | 单 topic 最简；per_table 按 `topic_prefix.schema.table` 命名，避免冲突 |
| 消息 key | `key_mode=pk`（主键拼接，默认）/ `none`（`schema.table:binlog_pos`） | 与设计文档一致 |
| 消息格式 | 自定义 JSON（非 Debezium） | 简洁，下游易消费 |
| Webhook 失败行为 | 重试耗尽后任务 `FAILED` | 与设计文档一致，语义简单 |
| 全量到 Kafka/Webhook | 不支持，仅增量 | FULL/ALL + 非 MYSQL sink 拒绝启动 |
| 配置位置 | 任务级 `sink_configs.options`，不新增全局 `[kafka]` 段 | 目标类型属任务属性（设计文档 5.3） |

---

## 当前状态分析

### 已具备
- `go.mod` 已引入 `github.com/segmentio/kafka-go v0.4.50`（未使用）- [go.mod:13](../go.mod#L13)
- 前端 `web/src/App.vue` 已预埋 Kafka UI：`SINK_TYPES`、`singleKafkaConfig`、`sinkConfigsPayload`，发送 `sink_configs: [{type, options}]`（含 brokers/topic/key_mode/batch_size/required_acks），并支持回填 - [App.vue:226](../web/src/App.vue#L226)
- `pkg/binlog.BinlogEvent` 已是统一事件模型（含 BeforeImage/Rows/Position）- [subscriber.go:26](../pkg/binlog/subscriber.go#L26)
- `checkpoint.Manager` 可复用（SavePosition/GetPosition）- [redis_checkpoint.go:33](../internal/checkpoint/redis_checkpoint.go#L33)
- `DataWriter` 接口与 `BufferedWriter`/`BatchWriter` 已封装良好 - [data_writer.go:24](../internal/sync/infrastructure/writer/data_writer.go#L24)
- 存储加密 `pkg/crypto`（AES-GCM，`ENC~` 前缀）+ `SyncTask.EncryptPasswords/DecryptPasswords` - [task.go:519](../internal/task/domain/entity/task.go#L519)、[aes.go](../pkg/crypto/aes.go)
- 设计文档 `plans/incremental-multi-sink-design.md` 已定义 ChangeEvent/Sink/SinkFactory 蓝图

### 缺失
- `TaskConfig`（[task.go:74](../internal/task/domain/entity/task.go#L74)）无 sink 字段
- `task_handler.go` 的 `CreateTaskRequest`（[task_handler.go:117](../internal/api/handler/task_handler.go#L117)）不接收 `sink_configs`
- 无 `Sink` 接口、`ChangeEvent`、`SinkFactory`、`KafkaSink`、`WebhookSink`
- `IncrementalSyncService`（[sync_service.go:43](../internal/sync/application/sync_service.go#L43)）直接持有 `writers map[string]*writer.BufferedWriter`，与 MySQL 强耦合
- `executeIncrementalSync`（[task_service.go:3943](../internal/task/application/service/task_service.go#L3943)）未传递 sink 配置
- 加密逻辑只覆盖 DB 密码，未覆盖 sink option 中的密钥（SASL 密码、Webhook auth）
- 前端缺少 SASL/SSL 字段与 per-table topic_prefix 字段

### 关键耦合点（重构主切入点）
`IncrementalSyncService.Start`（[sync_service.go:140-262](../internal/sync/application/sync_service.go#L140)）在 232-258 行直接为每张表创建 `BufferedWriter` 并 `EnableUpsert()`；`syncEventHandler`（[sync_service.go:481](../internal/sync/application/sync_service.go#L481)）按 EventType 分发到 `handleInsert/handleUpdate/handleDelete` 直接操作 `s.writers[key]`。

---

## 提议变更

### 阶段 A - Sink 领域模型与事件归一化

#### A1. 新增 Sink 领域模型
**文件**：`internal/sync/domain/sink/sink.go`（新建）
**内容**：
- `SinkType` 常量：`MYSQL`、`KAFKA`、`HTTP_WEBHOOK`
- `SinkConfig{ Type SinkType; Options map[string]interface{} }`（json `sink_configs` 元素）
- `ChangeEvent`（TaskID/SourceSchema/SourceTable/EventType/EventTime/BinlogFile/BinlogPos/PrimaryKeys/Before/After/TraceID）
- `Sink` 接口：`Type()`/`Open(ctx)`/`Write(ctx, *ChangeEvent)`/`Flush(ctx)`/`Close(ctx)`
- 可选 `TablePreparer` 接口：`PrepareTables(ctx, dbMapping, tables)`（用类型断言保留 MySQL 预建 writer 快速失败）
- 可选 `BatchSink` 接口：`WriteBatch(ctx, []*ChangeEvent)`（第二版优化用，本期 Kafka/Webhook 先实现单条 Write）
- `SecretPath` 声明机制：每个 sink 类型可声明其 options 中的密钥路径（见阶段 C 加密扩展）
**为什么**：接口放 domain 层，不依赖基础设施；统一事件模型隔离 canal/go-mysql 细节。

#### A2. 新增事件归一化器
**文件**：`internal/sync/application/event_normalizer.go`（新建）
**内容**：`ToChangeEvent(binlogEvent *binlog.BinlogEvent, identity *entity.TableIdentity, taskID string) (*ChangeEvent, error)`
- INSERT：`Rows[0]` -> After
- UPDATE：`BeforeImage[0]` -> Before，`Rows[0]` -> After
- DELETE：`BeforeImage[0]` -> Before
- 从 identity 提取 PrimaryKeys
**为什么**：Sink 只认 ChangeEvent，不感知 canal 细节。

---

### 阶段 B - MySQLSink 适配 + IncrementalSyncService 重构

#### B1. 新增 MySQLSink
**文件**：`internal/sync/infrastructure/sink/mysql/sink.go`（新建）
**内容**：封装现有 `writer.BufferedWriter`。
- `Open`：获取 writeConn，`SET SESSION FOREIGN_KEY_CHECKS=0`
- `PrepareTables`：遍历 dbMapping/tables，`analyzer.AnalyzeTable` 后创建 `BufferedWriter` + `EnableUpsert()`，缓存到 `map[string]*writer.BufferedWriter`（key=`srcDB.table`）
- `Write`：按 EventType 调用 `BufferedWriter` 的 WriteBatch/UpdateWithBeforeImage/Delete；UPDATE 检测主键变化走 `deleteAndUpsert`（迁移自 [sync_service.go:640](../internal/sync/application/sync_service.go#L640)）
- `Flush`/`Close`：委托 BufferedWriter（Close 幂等）
**为什么**：保持现有 MySQL 增量行为完全不变（兼容性基准）。

#### B2. 新增 SinkFactory
**文件**：`internal/sync/infrastructure/sink/factory.go`（新建）
**内容**：`NewSinks(configs []SinkConfig, deps SinkDeps) ([]Sink, error)`
- `SinkDeps` 携带 MySQLSink 所需的 targetDB/analyzer/batchSize/dbMapping/tables
- 按 `Type` 创建 MySQLSink/KafkaSink/WebhookSink
- 校验 Type 合法性、必填 options
- 空 configs 默认返回 `[{MYSQL}]`（老任务兼容）
**为什么**：集中创建逻辑，核心编排不感知具体实现。

#### B3. 改造 IncrementalSyncService
**文件**：`internal/sync/application/sync_service.go`（修改）
**变更**：
- 结构体：`writers map[string]*writer.BufferedWriter` -> `sinks []sink.Sink`；移除 `identities`/`targetSchemas`/`writeConn`（下沉到 MySQLSink）
- `Start`（140-262 行）：移除直接建 writer 逻辑；改为 `s.sinks = factory.NewSinks(...)`；遍历 sinks 调 `Open`；对实现 `TablePreparer` 的调 `PrepareTables`
- `syncEventHandler.OnEvent`（481 行）：调 `normalizer.ToChangeEvent` 生成 ChangeEvent；遍历 `s.sinks` 逐个 `Write`；全部成功后 `checkpointMgr.SavePosition`；任一失败返回错误（上层标记 FAILED，不推进位点）
- 删除 `handleInsert/handleUpdate/handleDelete`（逻辑迁入 MySQLSink.Write）
- `SyncConfig` 增加 `SinkConfigs []sink.SinkConfig` 字段
**为什么**：解耦事件订阅与目标写入，支撑多 Sink。
**回归要求**：本阶段完成后必须跑全量 MySQL 增量回归测试，确认行为与重构前一致。

---

### 阶段 C - TaskConfig / API / 存储加密扩展

#### C1. TaskConfig 增加 SinkConfigs
**文件**：`internal/task/domain/entity/task.go`（修改，[task.go:74-100](../internal/task/domain/entity/task.go#L74)）
**变更**：`TaskConfig` 末尾增加 `SinkConfigs []sink.SinkConfig`（json `sink_configs,omitempty`）。
- 依赖方向：`task` 实体 import `internal/sync/domain/sink`（若循环依赖，则在 task 包内定义镜像结构并转换）
- 兼容：老任务 `sink_configs` 为空，启动时默认填充 `[{type:MYSQL}]`

#### C2. 存储加密扩展（覆盖 sink option 密钥）
**文件**：`internal/task/domain/entity/task.go`（修改，[task.go:519](../internal/task/domain/entity/task.go#L519)）
**变更**：扩展 `EncryptPasswords`/`DecryptPasswords`，在处理 DB 密码后追加遍历 `SinkConfigs` 调用 `sink.EncryptSinkSecrets(options, key)` / `DecryptSinkSecrets(options, key)`。
- 密钥路径声明（由各 sink 实现注册到 factory）：
  - KAFKA：`security.sasl_password`（string）
  - HTTP_WEBHOOK：`headers`（map -> 序列化为 JSON 整体加密为 `ENC~` 字符串；解密时反序列化回 map）
- 复用 `pkg/crypto`（AES-GCM、`ENC~` 前缀、`NormalizeKey`），保持「存储前加密、内存保留明文、defer 还原」的现有模式
**为什么**：AGENTS.md 明确禁止泄露任务密码；SASL 密码与 Webhook auth token 同属敏感凭据，必须落盘加密。

#### C3. API 请求/响应扩展
**文件**：`internal/api/handler/task_handler.go`（修改，[task_handler.go:117](../internal/api/handler/task_handler.go#L117)）
**变更**：
- `CreateTaskRequest`/`UpdateTaskRequest` 增加 `SinkConfigs []SinkConfigRequest`（json `sink_configs,omitempty`）
- `SinkConfigRequest{ Type string; Options map[string]interface{} }`
- 创建/更新时映射到 `TaskConfig.SinkConfigs`
- 向后兼容：未传 sink_configs 时默认 MYSQL
- 响应回显 `sink_configs`（前端回填已就绪）
**为什么**：对齐前端契约，不破坏老 API。

#### C4. executeIncrementalSync 传递 sink 配置 + FULL 模式拦截
**文件**：`internal/task/application/service/task_service.go`（修改，[task_service.go:3943](../internal/task/application/service/task_service.go#L3943)、[task_service.go:1259](../internal/task/application/service/task_service.go#L1259)）
**变更**：
- `executeIncrementalSync`：把 `task.Config.SinkConfigs` 填入 `syncApp.SyncConfig`
- `StartTask`：增加校验--若 `SinkConfigs` 含非 MYSQL 类型且 Mode 为 `FULL`/`ALL`，返回错误拒绝启动（"Kafka/Webhook sink 仅支持 INCREMENTAL 模式"）
**为什么**：Kafka/Webhook 不支持全量存量投递，必须显式拦截。

---

### 阶段 D - KafkaSink（SASL/SSL + per-table topic）

#### D1. KafkaSink 实现
**文件**：`internal/sync/infrastructure/sink/kafka/sink.go`（新建）
**内容**：使用 `github.com/segmentio/kafka-go` 的 `kafka.Writer`。
- options 契约（见下「配置契约」）：
  - `brokers`([]string)、`topic`(string)、`routing_mode`("single_topic" 默认 / "per_table")、`topic_prefix`(string，per_table 用)、`key_mode`("pk" 默认 / "none")
  - `batch_size`(int，默认 1000)、`batch_timeout_ms`(int，默认 500)、`required_acks`(int，默认 1)
  - `security`：`sasl_mechanism`("PLAIN"/"SCRAM-SHA-256"/"SCRAM-SHA-512")、`sasl_username`、`sasl_password`、`tls_enabled`(bool)、`ca_cert_path`、`client_cert_path`(可选)、`client_key_path`(可选)、`insecure_skip_verify`(bool，默认 false)
- `Open`：构建 `kafka.Writer{Addr: kafka.TCP(brokers...), Async: false, RequiredAcks, BatchSize, BatchTimeout}`；按 `security` 配置 `Transport`（`kafka.SASL` Mechanism + `*tls.Config`）
  - SASL Mechanism：PLAIN -> `plain.Mechanism`；SCRAM -> `scram.Mechanism`(SHA-256/512)
  - TLS：`tls.Config{RootCAs: loadCA(ca_cert_path), Certificates: loadX509KeyPair(cert,key), InsecureSkipVerify}`（仅当 `tls_enabled=true`）
- `Write`：
  - topic 解析：`single_topic` -> 固定 `topic`；`per_table` -> `{topic_prefix}.{schema}.{table}`
  - key：`key_mode=pk` -> 主键值拼接（无主键退化为 `schema.table:binlog_pos`）；`none` -> `schema.table:binlog_pos`
  - value：ChangeEvent 序列化为 JSON
  - `writer.WriteMessages` 同步发送
- `Flush`：kafka-go 同步 writer 无需显式 flush（no-op）
- `Close`：关闭 kafka.Writer（幂等）
- 声明密钥路径 `security.sasl_password` 供加密层使用
**为什么**：事件投递标准化为 JSON；同步发送保证 At-Least-Once；SASL/SSL 覆盖生产环境。

---

### 阶段 E - WebhookSink

#### E1. WebhookSink 实现
**文件**：`internal/sync/infrastructure/sink/webhook/sink.go`（新建）
**内容**：使用标准库 `net/http`。
- options 契约：`url`(string)、`method`("POST" 默认)、`timeout_ms`(int，默认 3000)、`headers`(map[string]string)、`retry_times`(int，默认 3)、`retry_backoff_ms`(int，默认 500)
- `Open`：校验 url 非空、scheme 合法；构建 `http.Client{Timeout}`
- `Write`：ChangeEvent 序列化为 JSON 作为 body；逐次重试（指数/固定退避）；HTTP 2xx 视为成功，否则重试；重试耗尽返回错误（上层标记 FAILED）
- `Flush`：no-op（逐条发送）
- `Close`：no-op（幂等）
- 声明密钥路径 `headers`（整体加密）供加密层使用
**为什么**：轻量事件通知出口；失败即 FAILED 与设计文档一致。

---

### 阶段 F - 前端扩展

#### F1. Kafka 配置表单增强
**文件**：`web/src/App.vue`（修改，[App.vue:4421](../web/src/App.vue#L4421)）
**变更**：
- `singleKafkaConfig` 增加：`routing_mode`（下拉 single_topic/per_table）、`topic_prefix`（per_table 时显示）、`security` 折叠区（sasl_mechanism/sasl_username/sasl_password/tls_enabled/ca_cert_path/client_cert_path/client_key_path/insecure_skip_verify）
- payload 构建保持 `sink_configs: [{type, options}]` 结构，options 嵌套 `security`
- 回填逻辑适配新字段（brokers 数组与逗号分隔字符串互转已有，照此扩展）
**为什么**：对齐后端 SASL/SSL 与 per-table topic 能力。

#### F2. Webhook 配置表单
**文件**：`web/src/App.vue`（修改）
**变更**：新增 `singleWebhookConfig`（url/method/timeout_ms/headers 键值对/retry_times/retry_backoff_ms），SINK_TYPES 已含 HTTP_WEBHOOK 选项；payload 与回填同 Kafka 模式。
**为什么**：前端补齐 Webhook 配置入口。

---

### 阶段 G - 文档与配置示例

#### G1. 文档更新
**文件**：
- `README.md`：API 接口段补充 `sink_configs` 字段说明与 Kafka/Webhook 示例
- `docs/CONFIGURATION.md`：补充任务级 sink_configs 配置说明（不新增全局段）
- `docs/design/shejiwendang.md`：补充 Sink 抽象边界（可选，若涉及 DDD 边界更新）
**为什么**：保持文档与 API 行为一致（AGENTS.md 要求）。

#### G2. 配置示例
**文件**：`etc/application.toml.example`（修改，仅在需要全局默认时）
**决策**：第一阶段 Kafka/Webhook 配置只在任务级 `sink_configs.options` 表达，不新增全局段。

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

## 配置契约

### KAFKA options
```json
{
  "type": "KAFKA",
  "options": {
    "brokers": ["127.0.0.1:9092"],
    "topic": "mysql_cdc",
    "routing_mode": "single_topic",
    "topic_prefix": "cdc",
    "key_mode": "pk",
    "batch_size": 1000,
    "batch_timeout_ms": 500,
    "required_acks": 1,
    "security": {
      "sasl_mechanism": "SCRAM-SHA-512",
      "sasl_username": "user",
      "sasl_password": "***",
      "tls_enabled": true,
      "ca_cert_path": "/etc/kafka/ca.pem",
      "client_cert_path": "/etc/kafka/client.pem",
      "client_key_path": "/etc/kafka/client.key",
      "insecure_skip_verify": false
    }
  }
}
```
> `routing_mode=per_table` 时 topic 解析为 `{topic_prefix}.{schema}.{table}`；`security` 整段可选（明文时省略）。

### HTTP_WEBHOOK options
```json
{
  "type": "HTTP_WEBHOOK",
  "options": {
    "url": "http://127.0.0.1:9000/cdc/event",
    "method": "POST",
    "timeout_ms": 3000,
    "headers": { "Authorization": "Bearer ***" },
    "retry_times": 3,
    "retry_backoff_ms": 500
  }
}
```

### 消息体（Kafka value / Webhook body，统一 JSON）
```json
{
  "task_id": "task_001",
  "source_schema": "db1",
  "source_table": "orders",
  "event_type": "UPDATE",
  "event_time": "2026-07-14T10:00:00+08:00",
  "binlog_file": "mysql-bin.000001",
  "binlog_pos": 12345,
  "primary_keys": { "id": 42 },
  "before": { "id": 42, "status": 0 },
  "after": { "id": 42, "status": 1 }
}
```

---

## 安全与加密

- sink option 中的密钥（Kafka `security.sasl_password`、Webhook `headers`）必须落盘加密，复用 `pkg/crypto` AES-GCM + `ENC~` 前缀。
- 加密入口：扩展 `SyncTask.EncryptPasswords/DecryptPasswords`（[task.go:519](../internal/task/domain/entity/task.go#L519)），遍历 `SinkConfigs` 调用各 sink 声明的密钥路径加密/解密。
- 存储（MySQL/File）层现有「存储前 `EncryptPasswords`、defer 还原明文、加载后 `DecryptPasswords`」流程不变，自动覆盖新字段。
- 响应回显时不应返回密钥明文（前端展示掩码或留空，由 handler 决定；本期先回填占位，避免泄露）。

---

## 测试规划

### 单元测试
- `event_normalizer_test.go`：INSERT/UPDATE/DELETE -> ChangeEvent 转换正确性（Before/After/PrimaryKeys）
- `sink/factory_test.go`：各 SinkType 创建、非法 Type 报错、必填 options 校验、空 configs 默认 MYSQL
- `sink/mysql/sink_test.go`：用 `go-sqlmock` 验证 INSERT/UPDATE/DELETE SQL 与现有行为一致；UPDATE 主键变化走 deleteAndUpsert
- `sink/kafka/sink_test.go`：抽象 Writer 接口验证 JSON 序列化、key 生成（pk/none）、topic 解析（single/per_table）、options 默认值、SASL/TLS 配置构建
- `sink/webhook/sink_test.go`：用 `httptest.Server` 验证 2xx 成功、非 2xx 重试、重试耗尽返回错误、超时
- `task_handler_test.go`：创建带 sink_configs 的任务、老任务兼容（无 sink_configs 默认 MYSQL）、非 MYSQL + FULL/ALL 拒绝
- `task_test.go`：SinkConfigs 加密/解密往返（SASL 密码、Webhook headers）

### 集成测试
- MySQL -> MySQL 增量回归（确保 MySQLSink 不退化）
- MySQL -> Kafka 端到端（本地 Kafka，标记 `// +build integration` 跳过 CI）
- MySQL -> Webhook 端到端（httptest）
- checkpoint 恢复：Sink 写入成功后位点推进；模拟 Sink 失败位点不推进

### 故障测试
- Kafka 不可用时任务 FAILED、位点不推进
- Webhook 返回 500 重试耗尽后 FAILED
- 重启后从最近 checkpoint 重新消费（At-Least-Once 验证）

### 验证命令
```bash
go test ./internal/sync/...
go test ./internal/task/...
go test ./internal/api/...
go vet ./...
cd web && npm run build
```

---

## 实施顺序（严格按序）

1. 阶段 A：新增 `domain/sink/sink.go`（接口 + ChangeEvent + SinkConfig）+ `event_normalizer.go` + 单测
2. 阶段 B：新增 `sink/factory.go` + `sink/mysql/sink.go`（封装现有 writer）+ 单测；改造 `IncrementalSyncService` 持有 `[]Sink`，删除直接 writer 调用；**MySQL 增量回归通过**
3. 阶段 C：`TaskConfig` 增加 `SinkConfigs`；扩展 `EncryptPasswords/DecryptPasswords` 覆盖 sink 密钥；`task_handler` 请求/响应扩展；老任务兼容；`executeIncrementalSync` 传 sink 配置 + FULL/ALL 拦截
4. 阶段 D：新增 `sink/kafka/sink.go`（SASL/SSL + per-table topic）+ 单测
5. 阶段 E：新增 `sink/webhook/sink.go` + 单测
6. 阶段 F：前端 Kafka 表单增强（SASL/SSL + per-table）+ Webhook 表单
7. 阶段 G：端到端验证 + 文档更新

---

## 风险与规避

| 风险 | 规避 |
|------|------|
| MySQLSink 重构导致增量行为退化 | 阶段 B 完成后必须跑全量 MySQL 增量回归，行为对齐现有 handleInsert/Update/Delete |
| task 包 import sync/domain/sink 循环依赖 | 若循环，在 task 包内定义 SinkConfig 镜像结构并转换 |
| Kafka 同步发送阻塞影响延迟 | batch_size/batch_timeout_ms 可调；同步发送是 At-Least-Once 的必要代价 |
| SASL/SSL 配置错误导致连接失败 | Open 阶段做连通性预检（可选 `kafka.DialContext` 探测）；返回明确错误 |
| 老任务无 sink_configs 字段 | 启动时默认填充 MYSQL，存储层 JSON 反序列化空值兼容 |
| sink 密钥泄露 | EncryptPasswords 扩展覆盖；响应回显掩码处理 |
| per-table topic 自动创建权限 | 文档说明需 Kafka broker 允许 auto.create.topics 或预先创建；失败时 Open 预检 |
| 前端 sink_configs 与设计文档 sink 单数不一致 | 本计划对齐前端 sink_configs（复数），设计文档视为历史参考 |

---

## 验收标准

1. 创建 `mode=INCREMENTAL` + `sink_configs=[{type:KAFKA, options:{brokers, topic, security:{...}}}]` 任务，启动后 INSERT/UPDATE/DELETE 投递到 Kafka，消息体为标准 JSON；SASL/SSL 连接成功
2. `routing_mode=per_table` 时各表事件投递到 `{prefix}.{schema}.{table}` topic
3. 创建 `sink_configs=[{type:HTTP_WEBHOOK, options:{url,...}}]` 任务，事件 POST 到目标 URL；非 2xx 重试耗尽后任务 FAILED
4. 任一 Sink 写入失败时任务 FAILED，binlog 位点不推进；重启后从上次位点重放
5. 老任务（无 sink_configs）继续以 MySQL 增量方式运行，行为无变化
6. `mode=FULL`/`ALL` + 非 MYSQL sink 被拒绝启动并返回明确错误
7. sink option 密钥（SASL 密码、Webhook headers）落盘为 `ENC~` 密文，内存与运行时为明文
8. 前端创建 Kafka（含 SASL/SSL、per-table）、Webhook 任务，回填编辑、多 Sink 模式均可正常工作
9. `go test ./...` 与 `go vet ./...` 通过，`cd web && npm run build` 通过
