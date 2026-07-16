# MySQL-to-Async 配置说明

## 配置文件位置

配置文件位于：`etc/application.toml`

首次使用时，请复制示例文件：
```bash
cp etc/application.toml.example etc/application.toml
```

## 环境变量配置（推荐 K8s / 容器部署）

支持使用环境变量覆盖配置文件（前缀：`MYSQL_TO_ASYNC_`）。

- 有配置文件：先读取 `application.toml`，再用环境变量覆盖。
- 无配置文件：程序使用默认配置并读取环境变量（适合 K8s 挂载 `ConfigMap/Secret`）。

常用变量示例：

```bash
MYSQL_TO_ASYNC_HTTP_HOST=0.0.0.0
MYSQL_TO_ASYNC_HTTP_PORT=8080

MYSQL_TO_ASYNC_DATASOURCE_HOST=mysql-source
MYSQL_TO_ASYNC_DATASOURCE_PORT=3306
MYSQL_TO_ASYNC_DATASOURCE_DATABASE=source_db
MYSQL_TO_ASYNC_DATASOURCE_USERNAME=sync_user
MYSQL_TO_ASYNC_DATASOURCE_PASSWORD=sync_password

MYSQL_TO_ASYNC_TARGET_HOST=mysql-target
MYSQL_TO_ASYNC_TARGET_PORT=3306
MYSQL_TO_ASYNC_TARGET_DATABASE=target_db
MYSQL_TO_ASYNC_TARGET_USERNAME=sync_user
MYSQL_TO_ASYNC_TARGET_PASSWORD=sync_password

MYSQL_TO_ASYNC_REDIS_HOST=redis
MYSQL_TO_ASYNC_REDIS_PORT=6379
MYSQL_TO_ASYNC_REDIS_DB=0

MYSQL_TO_ASYNC_STORAGE_MODE=file
MYSQL_TO_ASYNC_STORAGE_DATA_DIR=/app/data

MYSQL_TO_ASYNC_SECURITY_ENCRYPT_KEY=your-32-byte-secret-key-here!!!!
```

K8s Deployment 片段示例：

```yaml
env:
  - name: MYSQL_TO_ASYNC_HTTP_HOST
    value: "0.0.0.0"
  - name: MYSQL_TO_ASYNC_HTTP_PORT
    value: "8080"
  - name: MYSQL_TO_ASYNC_DATASOURCE_HOST
    valueFrom:
      configMapKeyRef:
        name: mysql-to-sync-config
        key: datasource_host
  - name: MYSQL_TO_ASYNC_DATASOURCE_PASSWORD
    valueFrom:
      secretKeyRef:
        name: mysql-to-sync-secret
        key: datasource_password
  - name: MYSQL_TO_ASYNC_SECURITY_ENCRYPT_KEY
    valueFrom:
      secretKeyRef:
        name: mysql-to-sync-secret
        key: security_encrypt_key
```

## 配置项详解

### 基础配置

#### [http] - HTTP服务配置

```toml
[http]
  host = "127.0.0.1"    # HTTP服务监听地址
  port = 8080            # HTTP服务端口
```

#### [datasource] - 源数据库默认配置

```toml
[datasource]
  provider = "mysql"     # 数据库类型，目前仅支持 mysql
  host = "192.168.1.100"
  port = 3306
  database = "test"
  username = "root"
  password = "123456"
  debug = true            # 是否启用调试模式
```

**说明**：
- 这些是默认值，如果创建任务时没有指定自定义源数据库，将使用此配置
- 建议使用只读权限的数据库账号进行数据同步

#### [target] - 目标数据库默认配置

```toml
[target]
  host = "192.168.1.100"
  port = 3306
  database = "test_target"
  username = "root"
  password = "123456"
```

**说明**：
- 这些是默认值，如果创建任务时没有指定目标数据库，将使用此配置
- 目标数据库需要写入权限
- 系统会自动创建不存在的数据库和表

#### [storage] - 任务存储配置

```toml
[storage]
  mode = "file"          # 存储模式: "file" 或 "mysql"
  
  # 文件存储配置
  data_dir = "data"
  
  # MySQL存储配置
  host = "127.0.0.1"
  port = 3306
  database = "mysql_to_async"
  username = "root"
  password = "123456"
```

**说明**：
- **file模式**：任务信息存储在JSON文件中（默认：`data/`目录）
  - 优点：简单、无需额外数据库
  - 缺点：不支持集群部署
  
- **mysql模式**：任务信息存储在MySQL数据库中
  - 优点：支持集群部署、数据持久化更好
  - 缺点：需要额外的数据库
  - 系统会自动创建 `sys_sync_tasks` 表

#### [security] - 安全配置

```toml
[security]
  encrypt_key = "your-32-byte-secret-key-here!!!!"   # 任务密码加密密钥
```

**说明**：
- 用于加密同步任务中 `source_db` 和 `target_db` 的数据库密码，防止密码以明文存储在数据库或文件中
- 加密算法：AES-256-GCM，密文以 `ENC~` 前缀标识
- **密钥长度**：建议 32 字节；不足自动补齐，超过截断
- **留空则不加密**，行为与未配置前完全一致
- **向后兼容**：加载时自动识别明文密码和 `ENC~` 密文，旧数据无需迁移
- 也可通过环境变量设置：`MYSQL_TO_ASYNC_SECURITY_ENCRYPT_KEY`

#### [redis] - Redis配置

```toml
[redis]
  host = "127.0.0.1"
  port = 6379
  password = ""           # Redis密码（如果有）
  db = 0                 # Redis数据库编号
```

**说明**：
- **可选配置**，用于保存增量同步的Checkpoint位点
- 如果不配置Redis，使用内存存储位点
  - 优点：简单、无需Redis
  - 缺点：服务重启后位点丢失，增量同步会重新开始
- 如果配置了Redis：
  - 优点：位点持久化，服务重启后可以继续增量同步
  - 缺点：需要Redis服务

#### [log] - 日志配置

```toml
[log]
  level = "debug"        # 日志级别: debug, info, warn, error

  [log.console]
    enable = true         # 是否输出到控制台
    no_color = false      # 是否禁用彩色输出

  [log.file]
    enable = true         # 是否输出到文件
    dir = "logs"         # 日志文件目录（默认为 logs/）
```

### 性能优化配置

#### [sync] - 同步性能配置

```toml
[sync]
  # 批次大小（行数）
  batch_size = 1000
  
  # 并发Worker数量
  worker_count = 4
  
  # 单表分片大小（行数）
  chunk_size = 100000
```

**batch_size（批次大小）**
- 含义：每次读取和写入的行数
- 影响因素：
  - 较大的值（5000-10000）：提高吞吐量，减少数据库往返次数，但占用更多内存
  - 较小的值（500-2000）：内存占用少，适合内存受限环境
- 建议值：1000 - 10000
- 调整建议：
  - 网络延迟高：增大批次大小
  - 内存受限：减小批次大小
  - 表字段多/数据量大：减小批次大小

**worker_count（并发Worker数）**
- 含义：同时处理的表数量或分片数量
- 影响因素：
  - 较大的值（8-16）：提高并发度，加快速度，但增加数据库压力
  - 较小的值（2-4）：减少数据库压力，适合数据库性能有限的情况
- 建议值：2 - 8
- 调整建议：
  - CPU核心多、数据库性能强：增大worker数量
  - 数据库性能有限：减小worker数量
  - 表数量少：worker数量影响较小
  - 表数量多：增大worker数量可以显著提升速度

**chunk_size（分片大小）**
- 含义：大表被切分的每个分片的行数
- 影响因素：
  - 较小的值（50000）：分片多，并行度高，但增加协调开销
  - 较大的值（200000）：分片少，并行度低，但协调开销小
- 建议值：50000 - 200000
- 调整建议：
  - 超大表（千万行以上）：使用较小的分片（50000-100000）
  - 中等表（百万行）：使用中等分片（100000-150000）
  - 小表（十万行以下）：使用较大的分片（150000-200000）

**index_restore_hard_max（索引回放并发硬上限，全局配置）**
- 含义：`index_restore_worker_count=0` 自动推导或用户显式设置超过该值时，以此值作为绝对上限
- 默认：`<=0` 时使用内置值 16
- 建议值：16（保守）；目标库性能强劲、临时表空间充足时可适当提高，但通常不建议超过 `target_max_open_conns / 2`
- 注意：该字段位于 `[sync]` 全局配置段，对所有开启 `optimize_index` 的任务生效

### 功能开关

#### [features] - 功能开关

```toml
[features]
  # 只读模式
  enable_readonly = true
  
  # 索引优化
  optimize_index = false
```

**enable_readonly（只读模式）**
- 含义：同步期间将目标数据库的普通用户设为只读
- 作用：防止同步期间目标数据库被外部写入，导致数据不一致
- 建议值：
  - 生产环境：`true`
  - 测试环境：`false`
- 注意：需要MySQL 5.7+，且有足够权限

**optimize_index（索引优化）**
- 含义：同步前删除非主键索引，所有数据库、所有表的数据同步完成后，再按表顺序统一重建
- 作用：大幅提升写入性能（特别是有大量索引的表）
- 建议值：
  - 大表同步：`true`
  - 小表同步：`false`
- 注意：同步期间表性能可能下降

**index_restore_worker_count（索引回放并发度，任务级字段）**
- 含义：`optimize_index=true` 时，阶段3索引回放的表级并发度，不同表的索引重建并行执行
- 默认：0（自动推导为 `min(worker_count, 4)`，并受 `index_restore_hard_max` 封顶）
- 建议值：4（保守）；目标库 CPU/IO/临时空间充足时可适当调高
- 注意：
  - 单表内多个索引仍串行重建，避免同表 MDL 锁竞争
  - 并发度建议 ≤ `target_max_open_conns - 2`，每条 `CREATE INDEX` 会占用一条目标库连接
  - 并发回放期间目标实例 CPU、磁盘 IO、临时表空间负载会上升，需确保资源充足
  - MySQL 单条 `CREATE INDEX` 不可被 context 中断，任务暂停后最多等当前那张表的索引重建结束才生效

## 性能调优建议

### 场景1：小规模同步（< 100万行）
```toml
[sync]
  batch_size = 1000
  worker_count = 2
  chunk_size = 100000
```

### 场景2：中等规模同步（100万 - 1000万行）
```toml
[sync]
  batch_size = 2000
  worker_count = 4
  chunk_size = 100000
```

### 场景3：大规模同步（> 1000万行）
```toml
[sync]
  batch_size = 5000
  worker_count = 8
  chunk_size = 50000

[features]
  optimize_index = true
```

### 场景4：网络延迟高环境
```toml
[sync]
  batch_size = 10000      # 增大批次大小
  worker_count = 2       # 减少并发数
  chunk_size = 100000
```

### 场景5：内存受限环境
```toml
[sync]
  batch_size = 500       # 减小批次大小
  worker_count = 2       # 减少并发数
  chunk_size = 150000    # 增大分片大小
```

## 任务级别配置覆盖

在创建任务时，可以覆盖全局配置：

```json
{
  "batch_size": 5000,           // 覆盖全局 batch_size
  "worker_count": 8,            // 覆盖全局 worker_count
  "optimize_index": true,        // 覆盖全局 optimize_index
  "tx_commit_every_n_parallel": 50  // 并行 worker 每 N 批提交一次事务（0=默认5）
}
```

### Binlog 位点捕获（仅 ALL 模式）

**ALL 模式**在全量扫描开始前，自动通过短暂的 `FLUSH TABLES WITH READ LOCK` 拿到一个严格领先于
全量读取的 binlog 位点（P0），取到位点后立即 `UNLOCK TABLES`，全过程毫秒级，无需任何
额外配置开关。P0 会被持久化为后续增量同步的起点，保证全量期间的所有变更都能
被增量阶段完整回放。P0 捕获或持久化失败时任务立即终止，防止增量阶段漏数据。

**FULL 模式**不捕获 binlog 位点，不保存增量 checkpoint。FULL 只做一次无缝全表遍历，
同步期间发生的变化不进行追平。如需覆盖同步期间的变化，请使用 ALL 模式。

> 注：历史上提供过 `enable_consistent_snapshot` 任务级开关（用于"严格全局快照
> + 长事务连接池"模式），现已下线。当前实现统一使用"短锁取位点 + 全量短查询"
> 模式，避免长事务长期持有源库 `MDL_SHARED_READ`。

### 并行事务提交间隔（tx_commit_every_n_parallel）

控制并行 worker（range 分片 / sample 采样）每 N 批提交一次事务。

- **串行路径**（keyset / nopk）：固定每 200 批提交一次，减少 fsync 频率提高吞吐。
- **并行路径**（range / sample）：默认每 5 批提交一次，减少锁持有时间避免 lock wait timeout。

对于大表 range/sample 并行同步场景，默认值 5 可能偏保守，fsync 频率偏高。
如果目标库锁等待不严重，可以适当调大此值以提高吞吐：

```json
{
  "tx_commit_every_n_parallel": 50
}
```

| 值 | 行为 |
|---|---|
| 0（默认） | 使用内置默认值 5 |
| 1-5 | 更频繁提交，降低锁等待，适合高并发写入场景 |
| 10-50 | 减少 fsync 频率，提高大表吞吐 |
| 100+ | 长事务，仅适合目标库无并发写入且磁盘性能极好的场景 |

> 注意：此参数仅影响并行路径。串行路径（keyset/nopk）的提交间隔固定为 200，
> 不受此参数影响。

### 全量并行读取路径（与 channel buffer 的关系）

当前 `task_service` 实际使用的全量并行模式如下：

| 模式 | 读模型 | 是否用 `ChannelSync` |
|------|--------|----------------------|
| `range` | 按数值主键分片，每个 worker **独立 reader** 读自己的区间 | 否 |
| `sample` | 按采样边界分片，每个 worker **独立 reader** | 否 |
| `keyset` | 单 goroutine 顺序 keyset 读 | 否 |
| `nopk` | 无主键流式读 | 否 |
| `channel`（预留） | **单 reader** 顺序读 batch，经 channel 分发给多 worker 写 | 是（`ChannelSyncExecutor`，尚未接入主流程） |

因此日常调 `intra_table_worker_count`、`tx_commit_every_n_parallel` 时，影响的是 **range/sample 分片并行**，与 channel buffer **无直接关系**。channel buffer 仅在未来或自定义接入 `ChannelSync` 路径时才有意义。

### Channel 批次缓冲（channel_buffer_batches，内部参数）

`ChannelSync` 用带缓冲 channel 连接「单线程读 batch」与「多 worker 写 batch」。缓冲容量单位是 **批次数**（每个元素是一批行，不是行数）。

**当前状态**：该参数仅在代码层 `NewChannelSync(workerCount, batchSize, channelBufferBatches)` / `ExecuteFullSyncChannel(..., channelBufferBatches)` 中可传，**尚未**加入任务 JSON / Web UI / `application.toml`。

**计算规则**（`EffectiveChannelBufferBatches`）：

| `channelBufferBatches` | 实际 channel 容量 |
|------------------------|-------------------|
| `0` 或未配置（默认） | **`workerCount × 4`**（不是固定 4） |
| `1` … `workerCount × 64` | 使用配置值 |
| `> workerCount × 64` | 截断为 **`workerCount × 64`** |

其中 `workerCount` 即表内并行 worker 数（与 `intraWorkers` / `intra_table_worker_count` 生效值一致）。

示例：`intra_table_worker_count = 8` 且未配置 buffer → channel 容量 = **32** 个 batch；若显式配置 `channel_buffer_batches = 16` → 容量为 **16**。

**内存提示**：每个缓冲槽持有约 `batch_size` 行数据。大表、大行、高 `batch_size` 时不宜把 buffer 设得过大；默认 `worker×4` 用于吸收读/写短暂抖动，不能替代目标库写入能力调优。

**投递语义**：`AddBatch` 为带 `context` 的**阻塞投递**——channel 满时 reader 等待空位，直到 `ctx` 取消；不再使用「满则报错 + sleep 重试」。

### 增量 Sink 配置（sink_configs）

`sink_configs` 是任务级配置，用于控制在增量同步阶段将 binlog 变更事件写入哪些目标。仅在 `mode=INCREMENTAL` 时生效；`FULL`/`ALL` 模式含非 `MYSQL` 类型 Sink 时任务拒绝启动。

**支持的类型：** `MYSQL`、`KAFKA`、`HTTP_WEBHOOK`

**默认行为：** 不传 `sink_configs` 时，自动补为 `[{type: "MYSQL"}]`，保持旧任务兼容。

**多 Sink 语义：** 所有 Sink 写入成功后才推进 binlog checkpoint（At-Least-Once）。任一 Sink 写入失败则任务标记 FAILED，位点不推进。

**密钥回显：** 数据库密码、Kafka `security.sasl_password` 和 Webhook `headers` 在任务 API 响应中显示为 `******`。编辑任务时原样提交占位符会保留已有值；提交新值才会替换。

#### KAFKA options

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

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| brokers | []string | 是 | - | Kafka broker 地址列表 |
| topic | string | 是 | - | Topic 名称（routing_mode=single_topic 时的固定 topic） |
| routing_mode | string | 否 | single_topic | `single_topic` 固定 topic / `per_table` 按 `{topic_prefix}.{schema}.{table}` 命名 |
| topic_prefix | string | 条件必填 | - | `routing_mode=per_table` 时必填的 topic 前缀 |
| key_mode | string | 否 | pk | `pk` 主键拼接 / `none` 使用 `schema.table:binlog_pos` |
| batch_size | int | 否 | 1000 | Kafka 批次大小（条数） |
| batch_timeout_ms | int | 否 | 500 | 批次超时（毫秒） |
| required_acks | int | 否 | 1 | 发送确认级别：0=无确认 / 1=leader 确认 / -1=all ISR 确认 |
| security.sasl_mechanism | string | 否 | - | SASL 机制：`PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512` |
| security.sasl_username | string | 否 | - | SASL 用户名 |
| security.sasl_password | string | 否 | - | SASL 密码（落盘时 AES-256-GCM 加密） |
| security.tls_enabled | bool | 否 | false | 是否启用 TLS |
| security.ca_cert_path | string | 否 | - | CA 证书文件路径 |
| security.client_cert_path | string | 否 | - | 客户端证书路径（mTLS） |
| security.client_key_path | string | 否 | - | 客户端密钥路径（mTLS） |
| security.insecure_skip_verify | bool | 否 | false | 跳过证书校验（仅测试环境） |

> `security` 整段可选，明文 Kafka 连接时省略即可。`routing_mode=per_table` 时需确保 Kafka broker 允许自动创建 topic 或预先创建对应 topic。

#### HTTP_WEBHOOK options

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

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| url | string | 是 | - | Webhook 目标 URL |
| method | string | 否 | POST | HTTP 请求方法 |
| timeout_ms | int | 否 | 3000 | 请求超时（毫秒） |
| headers | map[string]string | 否 | - | 自定义 HTTP 头（落盘时 AES-256-GCM 加密） |
| retry_times | int | 否 | 3 | 失败重试次数（非 2xx 状态码触发重试） |
| retry_backoff_ms | int | 否 | 500 | 重试退避间隔（毫秒） |

> 重试耗尽后任务标记 FAILED，binlog 位点不推进。重启后从上次 checkpoint 重新消费并重试。

#### 消息体格式

所有 Sink 类型共用统一 JSON 格式：

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

## 监控和日志

### 查看同步进度
```bash
# 实时查看日志
tail -f logs/mysql-to-sync.log
```

### 关键日志信息
- 表级别同步：`Table xxx will be split into N chunks, each ~Y rows`
- 分片完成：`Chunk X of table xxx completed (start-end rows)`
- 表完成：`Table xxx completed, processed N rows`
- 任务完成：`Task xxx Full sync completed, total rows: N`

### 性能指标
可以通过API获取任务指标：
```bash
curl http://localhost:8080/api/tasks/:id/metrics
```

返回包含：
- `processed_rows`: 已处理行数
- `total_rows`: 总行数（当前版本未由同步流程填充；ETA 使用 estimated_total_rows）
- `estimated_total_rows`: 估算总行数（information_schema），仅用于 ETA，不用于正确性校验
- `progress_percent`: 进度百分比
- `status`: 任务状态
- `current_position`: 当前位置

## 故障排查

### 问题1：同步速度慢
**可能原因**：
- worker_count 太小
- batch_size 不合适
- 网络延迟高
- 索引过多

**解决方案**：
- 增加 worker_count
- 调整 batch_size
- 启用 optimize_index

### 问题2：内存占用过高
**可能原因**：
- batch_size 过大
- worker_count 过多

**解决方案**：
- 减小 batch_size
- 减少 worker_count

### 问题3：数据库压力大
**可能原因**：
- worker_count 过多
- batch_size 过大

**解决方案**：
- 减少 worker_count
- 减小 batch_size
- 优化数据库配置

## 最佳实践

1. **测试先行**：先在小数据集上测试配置，验证性能
2. **逐步调优**：从保守配置开始，逐步调整
3. **监控观察**：实时监控日志和数据库性能指标
4. **定期备份**：同步前做好数据备份
5. **权限控制**：使用最小必要权限的数据库账号
6. **网络优化**：确保源和目标数据库网络延迟低
7. **资源评估**：根据服务器硬件资源调整配置
8. **启用密码加密**：配置 `[security].encrypt_key` 或环境变量，确保任务密码不以明文存储

## 更多帮助

- 项目文档：`docs/README.md`
- 增量同步指南：`docs/guides/INCREMENTAL_SYNC_GUIDE.md`
- GitHub Issues：提交问题反馈
