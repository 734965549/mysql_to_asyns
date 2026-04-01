# Multi-Sink API 接口文档

## 概述

Multi-Sink 功能允许增量同步任务将变更事件同时写入多个目标端（MySQL、Kafka、HTTP Webhook）。通过在创建/更新任务时配置 `sink_configs` 字段来启用。

**向后兼容**：如果不传 `sink_configs`，系统自动使用默认 MySQL Sink，行为与旧版本完全一致。

## 支持的 Sink 类型

| 类型 | 标识 | 说明 |
|------|------|------|
| MySQL | `MYSQL` | 写入目标 MySQL 数据库（默认行为） |
| Kafka | `KAFKA` | 发送到 Kafka Topic |
| HTTP Webhook | `HTTP_WEBHOOK` | POST 到指定 HTTP 端点 |

---

## 创建带 Sink 配置的任务

```
POST /api/tasks
Content-Type: application/json
```

### 请求体

```json
{
  "name": "多目标增量同步",
  "mode": "INCREMENTAL",
  "source_schema": "production",
  "tables": ["users", "orders"],
  "batch_size": 1000,
  "sink_configs": [
    {
      "type": "MYSQL",
      "options": {
        "target_schema": "backup_db",
        "batch_size": 1000
      }
    },
    {
      "type": "KAFKA",
      "options": {
        "brokers": ["kafka1:9092", "kafka2:9092"],
        "topic": "cdc_events",
        "key_mode": "pk",
        "batch_size": 100,
        "batch_timeout_ms": 500,
        "required_acks": 1
      }
    },
    {
      "type": "HTTP_WEBHOOK",
      "options": {
        "url": "https://api.example.com/webhook/cdc",
        "method": "POST",
        "timeout_ms": 3000,
        "retry_times": 3,
        "headers": {
          "Authorization": "Bearer <token>",
          "X-Source": "mysql-to-async"
        }
      }
    }
  ]
}
```

### sink_configs 字段说明

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| type | string | 是 | Sink 类型：`MYSQL`、`KAFKA`、`HTTP_WEBHOOK` |
| options | object | 否 | 该 Sink 类型的配置选项 |

---

## 各 Sink 类型 Options 详解

### MYSQL Options

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| target_schema | string | 与 source_schema 相同 | 目标数据库名 |
| target_databases | []string | - | 目标数据库列表（与 source_databases 一一对应） |
| batch_size | int | 1000 | 批量写入大小 |

**示例：**

```json
{
  "type": "MYSQL",
  "options": {
    "target_schema": "replica_db",
    "batch_size": 2000
  }
}
```

### KAFKA Options

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| brokers | []string | **必填** | Kafka Broker 地址列表 |
| topic | string | `mysql_cdc` | 目标 Topic |
| key_mode | string | `pk` | 消息 Key 模式：`pk`（主键拼接）或 `table`（schema.table） |
| batch_size | int | 100 | 批量发送大小 |
| batch_timeout_ms | int | 500 | 批量发送超时（毫秒） |
| required_acks | int | 1 | 应答模式：0=不等待，1=Leader确认，-1=全部ISR确认 |

**消息格式：**

- Key：主键拼接值（如 `id=42`），无主键退化为 `schema.table:binlog_file:binlog_pos`
- Value：JSON 序列化的 `ChangeEvent`

**示例：**

```json
{
  "type": "KAFKA",
  "options": {
    "brokers": ["localhost:9092"],
    "topic": "user_changes",
    "key_mode": "pk",
    "required_acks": -1
  }
}
```

### HTTP_WEBHOOK Options

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| url | string | **必填** | Webhook URL |
| method | string | `POST` | HTTP 方法 |
| timeout_ms | int | 3000 | 请求超时（毫秒） |
| retry_times | int | 3 | 失败重试次数 |
| headers | map[string]string | - | 自定义 HTTP 请求头 |

**行为说明：**

- 每条 ChangeEvent 单独发送一次 HTTP 请求
- HTTP 2xx 视为成功，非 2xx 触发重试
- 重试采用线性退避策略（attempt * 500ms）
- 重试耗尽后任务进入失败状态

**示例：**

```json
{
  "type": "HTTP_WEBHOOK",
  "options": {
    "url": "https://hooks.example.com/sync",
    "timeout_ms": 5000,
    "retry_times": 5,
    "headers": {
      "Authorization": "Bearer my-secret-token"
    }
  }
}
```

---

## 更新任务的 Sink 配置

```
PUT /api/tasks/:id
Content-Type: application/json
```

**注意：** 只允许更新非运行状态的任务。

```json
{
  "sink_configs": [
    {
      "type": "KAFKA",
      "options": {
        "brokers": ["new-kafka:9092"],
        "topic": "new_topic"
      }
    }
  ]
}
```

更新时 `sink_configs` 为**整体替换**，不是增量合并。

---

## ChangeEvent 数据模型

所有 Sink 接收到的标准事件格式：

```json
{
  "task_id": "task_abc123",
  "source_schema": "production",
  "source_table": "users",
  "event_type": "INSERT",
  "event_time": "2026-04-01T10:30:00Z",
  "binlog_file": "mysql-bin.000003",
  "binlog_pos": 4567,
  "primary_keys": {"id": 42},
  "before": null,
  "after": {"id": 42, "name": "alice", "email": "alice@example.com"},
  "trace_id": ""
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| task_id | string | 任务 ID |
| source_schema | string | 源数据库名 |
| source_table | string | 源表名 |
| event_type | string | 事件类型：`INSERT`、`UPDATE`、`DELETE` |
| event_time | datetime | 事件发生时间 |
| binlog_file | string | Binlog 文件名 |
| binlog_pos | uint32 | Binlog 位点 |
| primary_keys | object | 主键字段及值（可选） |
| before | object | 变更前数据（UPDATE/DELETE 有值） |
| after | object | 变更后数据（INSERT/UPDATE 有值） |
| trace_id | string | 追踪 ID（预留） |

---

## 使用示例

### 示例 1：仅同步到 Kafka

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "用户变更推送到Kafka",
    "mode": "INCREMENTAL",
    "source_schema": "production",
    "tables": ["users"],
    "sink_configs": [
      {
        "type": "KAFKA",
        "options": {
          "brokers": ["kafka:9092"],
          "topic": "user_cdc"
        }
      }
    ]
  }'
```

### 示例 2：同时同步到 MySQL + Webhook

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "订单双写",
    "mode": "INCREMENTAL",
    "source_schema": "production",
    "tables": ["orders"],
    "sink_configs": [
      {
        "type": "MYSQL",
        "options": {"target_schema": "orders_backup"}
      },
      {
        "type": "HTTP_WEBHOOK",
        "options": {
          "url": "https://analytics.example.com/ingest",
          "headers": {"X-API-Key": "secret"}
        }
      }
    ]
  }'
```

### 示例 3：向后兼容（不传 sink_configs）

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "传统MySQL同步",
    "mode": "INCREMENTAL",
    "source_schema": "production",
    "target_schema": "replica",
    "tables": ["users"]
  }'
```

系统自动创建默认 MySQL Sink，使用 `target_schema` 和 `target_databases` 字段。

---

## 错误处理

### Sink 写入失败

- 任何一个 Sink 写入失败，整个事件处理中止
- Checkpoint 不会推进（At-Least-Once 语义）
- 失败信息记录到审计日志
- 任务状态变为 `FAILED`

### Sink 创建失败

创建任务时如果 `sink_configs` 配置无效（如 Kafka 缺少 brokers），启动增量同步时会报错：

```json
{
  "error": "create sink[0] type=KAFKA failed: kafka sink requires at least one broker"
}
```

---

## 架构说明

```
BinlogEvent → EventNormalizer → [ChangeEvent] → Sink1.Write()
                                              → Sink2.Write()
                                              → ...
                                              → All Sink.Flush()
                                              → Checkpoint.Save()
```

- **EventNormalizer**：将原始 BinlogEvent 标准化为 ChangeEvent 列表
- **SinkFactory**：根据配置创建具体 Sink 实例
- **IncrementalSyncService**：面向 Sink 接口编程，不直接依赖任何具体写入器
- **Checkpoint 语义**：所有 Sink 写入+Flush 成功后才提交 checkpoint
