# MySQL-to-Async 配置说明

## 配置文件位置

配置文件位于：`etc/application.toml`

首次使用时，请复制示例文件：
```bash
cp etc/application.toml.example etc/application.toml
```

## 配置项详解

### 基础配置

#### [http] - HTTP服务配置

```toml
[http]
  host = "127.0.0.1"    # HTTP服务监听地址
  port = 8081            # HTTP服务端口
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
- 含义：同步前删除非主键索引，同步完成后再重建
- 作用：大幅提升写入性能（特别是有大量索引的表）
- 建议值：
  - 大表同步：`true`
  - 小表同步：`false`
- 注意：同步期间表性能可能下降

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
  "enable_consistent_snapshot": true // 任务级一致性快照开关
}
```

### enable_consistent_snapshot（任务级参数）

`enable_consistent_snapshot` 不是 `etc/application.toml` 的全局配置项，
而是创建/更新任务时的 JSON 字段：

- 创建任务：`POST /api/tasks`
- 更新任务：`PUT /api/tasks/:id`

示例：

```json
{
  "name": "full-sync-orders",
  "mode": "FULL",
  "sync_level": "TABLE",
  "source_schema": "db_src",
  "target_schema": "db_dst",
  "tables": ["orders"],
  "worker_count": 16,
  "intra_table_worker_count": 16,
  "batch_size": 2000,
  "enable_consistent_snapshot": true
}
```

说明：

- 打开后，全量阶段会使用并行一致性快照读取，保证多个 worker 读取同一时点数据视图。
- 快照建立阶段会短暂执行 `FLUSH TABLES WITH READ LOCK`，请在业务低峰执行大表全量同步。
- 源库账号需具备相应权限（至少可执行快照建立所需语句）。

## 监控和日志

### 查看同步进度
```bash
# 实时查看日志
tail -f logs/mysql-to-async.log
```

### 关键日志信息
- 表级别同步：`Table xxx will be split into N chunks, each ~Y rows`
- 分片完成：`Chunk X of table xxx completed (start-end rows)`
- 表完成：`Table xxx completed, processed N rows`
- 任务完成：`Task xxx Full sync completed, total rows: N`

### 性能指标
可以通过API获取任务指标：
```bash
curl http://localhost:8081/api/tasks/:id/metrics
```

返回包含：
- `processed_rows`: 已处理行数
- `total_rows`: 总行数
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

## 更多帮助

- 项目文档：`docs/README.md`
- 增量同步指南：`docs/guides/INCREMENTAL_SYNC_GUIDE.md`
- GitHub Issues：提交问题反馈