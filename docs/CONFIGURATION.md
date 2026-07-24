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

**target_max_open_conns（目标连接池，见 `[sync]` / 连接池调优）**
- V2 全量持有 schema 顾问锁时会占用 1 个目标连接槽，因此开启 `full_load_engine=v2` 时 **`target_max_open_conns` 必须 ≥ 2**（`AcquireSchemaLocks` 前 fail-fast）

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
  "tx_commit_every_n_parallel": 50, // 并行 worker 每 N 批提交一次事务（0=默认5）
  "full_load_engine": "v2"       // 全量引擎 v1/v2，详见「全量 V2 引擎」小节
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
> + 长事务连接池"模式），现已下线。
>
> **V1（默认 `full_load_engine=v1`）**：P0 仍用毫秒级 `FLUSH TABLES WITH READ LOCK` 捕获；
> 全量读取为普通短查询，不长期持有源库 `MDL_SHARED_READ`。
>
> **V2（`full_load_engine=v2`）**：全量读取改为表级 `REPEATABLE READ` +
> `WITH CONSISTENT SNAPSHOT` 长生命周期只读事务（详见下方「全量 V2 引擎」），每张表在读期间
> 持有表级 `MDL_SHARED_READ`；大表并行读前可能短暂 `FLUSH TABLES t WITH READ LOCK`。
> ALL 模式无 PK/UK 表还会在快照窗口内捕获**表级 binlog HWM**（`table_binlog_hwms`），增量启动
> 时对这类表 fail-closed 校验并按 HWM 过滤。运维需关注源库 undo 保留与 MDL 等待。

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

### 全量 V2 引擎（任务级流水线，full_load_engine）

`full_load_engine` 用于选择全量阶段的执行引擎，默认 `v1`（逐表调度，兼容旧行为）。
设为 `v2` 时启用任务级流水线引擎（`internal/sync/fullload`）：读写 worker 池解耦、
按字节限流的有界队列做背压、事务合并提交、可重试锁错误整事务重放、免 map 的固定
结构 `RowBatch`，用于消除逐表调度的长尾并降低 GC 压力。

V2 读取侧按**表级一致性快照（snapshot group）**执行：同一张表的所有 chunk 绑定到同一
（或经短暂表锁对齐的一组）InnoDB `REPEATABLE READ` + `WITH CONSISTENT SNAPSHOT` 只读事务，
避免全量期间源库“换主键重写 / 唯一列改值”导致目标端重建唯一索引报 1062。中小表走单连接
快照（无显式写阻塞锁）；估算行数达到约 100 万且 chunk>1 的超大表，短暂
`FLUSH TABLES t WITH READ LOCK` 对齐多连接 ReadView 后并行读。非 InnoDB 表 fail-closed。
InnoDB 引擎在打开快照前预检一次，并在持有表锁或首次 `SELECT ... LIMIT 1` 取得表级 MDL 后再权威校验一次，避免 DDL 竞态窗口内被改成 MyISAM 却仍继续一致性快照。
对齐取锁失败时默认降级为单连接快照；ALL 模式无主键表捕获表级 binlog HWM 时取锁失败
不降级（fail-closed）。ALL 模式有主键/唯一键表依赖增量幂等收敛；无主键表在快照窗口内
捕获表级 HWM，增量按事务提交位点过滤并仍推进 checkpoint。

#### 写事务提交标记表（`__mts_fl_tx`）

V2 在每个目标 schema 自动创建系统表 `` `{schema}.__mts_fl_tx` ``（`CREATE TABLE IF NOT EXISTS`，InnoDB）。
每个写入事务在 `Commit` 前插入一行唯一 UUID，与业务 `INSERT` **同事务**提交；当客户端收到连接类
Commit 错误、服务端结果未知时，writer 换连后对该 UUID 做 `SELECT ... FOR UPDATE` 锁定当前读：

| 探测结果 | 含义 | 处理 |
|---|---|---|
| 锁定读命中 marker | 原事务已提交 | 只推进进度，**禁止**重放 |
| 锁定读无行（等锁结束后） | 原事务已回滚 | 整事务重放后再提交 |
| 缺少 marker / 探测失败 | 无法判定 | **fail-closed**，任务失败 |

禁止根据业务行是否存在猜测 Commit 结果：无主键/全列策略下，前一事务已提交的完全相同行会误判；
普通一致性读也可能在原 `COMMIT` 仍在服务端处理时先看到“无行”，随后重放造成重复。

**表结构与注释**（新建时写入；已存在的同名表不会被 `IF NOT EXISTS` 改注释或改结构）：

| 列/对象 | 说明 |
|---|---|
| 表注释 | `mysql-to-sync 全量V2写事务提交标记表；与业务INSERT同事务提交，用于Commit结果未知时的锁定探测，请勿手工删除或改名` |
| `id` | 写事务唯一标记 UUID（`PRIMARY KEY`） |
| `run_id` | 本趟全量数据流水线运行 ID；成功收尾时按此删除本任务行 |
| `created_at` | 标记行写入时间（服务端时钟） |
| `idx_run_id` | 非唯一索引，加速按 `run_id` 清理 |

建表后会 **fail-closed** 校验已有 marker 表：必须是 `BASE TABLE`、`ENGINE=InnoDB`；`id` 为可保存 UUID 的 `NOT NULL` 列，并具备 `PRIMARY KEY(id)` 或等价单列唯一索引，且索引须覆盖完整列（`STATISTICS.SUB_PART` 为 `NULL`）或前缀长度 ≥ 36（拒绝 `UNIQUE(id(1))` 等短前缀）；`run_id` 为可保存至少 64 字符的 `NOT NULL` 列。若同名表是 MyISAM 等非事务引擎，marker 会在业务事务 `Commit` 前永久落库，回滚后探测仍会误判已提交并造成静默少数据。早期版本若缺少 `run_id` 列，需手工 `ALTER` 补齐或删表后由下次 V2 重建。

**上线检查清单（目标库账号 / 表映射）**：

- 目标账号对每个目标 schema 具备 `CREATE TABLE`（首次自动建 `__mts_fl_tx`）。
- 目标账号对 `__mts_fl_tx` 具备 `INSERT`（写事务提交前插入 marker）与 `SELECT`（含 `SELECT ... FOR UPDATE` 锁定探测）。
- 目标账号对 `__mts_fl_tx` 具备 `DELETE`：数据流水线成功后会按本趟 `run_id` 删除本任务 marker 行（**不** `DROP` 共享表）；清理使用独立短超时，失败只打告警，不回滚已成功的同步结果。失败/暂停/取消**不**清理本趟行，便于排查。
- 目标账号需能执行 `GET_LOCK`/`RELEASE_LOCK`：任务进入全量 V2 后、**首次目标端 DDL 之前**即对每个目标 schema 获取名为 `mts_fl_v2:{schema}` 的顾问锁，并持有至索引恢复与任务级收尾完成；同一 schema 上并发 V2 会 **fail-closed**。
- 锁连接会占用目标连接池 1 个槽位，因此 **`target_max_open_conns` 必须 ≥ 2**（在 `AcquireSchemaLocks` 前 fail-fast）；写路径与 DDL 另需可用连接。
- 锁会话会读取并必要时抬高 `@@SESSION.wait_timeout`（至少 60s），心跳间隔严格小于该值（默认 15s，取 `wait_timeout/3` 的较小者），避免 MySQL 先关闭空闲会话并隐式释放 `GET_LOCK`。心跳失败以 `ErrSchemaLockLost` 取消派生 context，库重建/建表/删索引等破坏性 DDL 与 `MarkFullSyncCompleted` 前都会检查该 cause。
- 多 schema 任务在同一锁连接上连续 `GET_LOCK`，要求 **MySQL ≥ 5.7.5**（更早版本第二次 `GET_LOCK` 会释放旧锁）。
- 业务 `TargetTable` **不得**占用保留名 `__mts_fl_tx`（大小写不敏感比较，兼容 `lower_case_table_names`）；否则启动 fail-closed。
- 启动 writers 前会 **fail-closed** 校验所有目标业务表为 InnoDB（复用既有表且为 MyISAM 等非事务引擎时直接失败）。
- 请勿手工删除、改名或清空**正在运行**任务使用的 `__mts_fl_tx`。同一目标 schema 上不得并发跑多个 V2 全量（代码以 `GET_LOCK` 强制互斥）。
- 若早期版本已创建无注释或不兼容结构的同名表，应手工修正（`ALTER`/`ENGINE=InnoDB`/`PRIMARY KEY`/补 `run_id`）或删表后由下次 V2 全量重建；不符合结构时任务会直接失败。

注意：表级 HWM 捕获与增量启动时的 `RequireNoPKTableHWM` fail-closed 校验**仅在
`full_load_engine=v2` 生效**。默认 `v1` 保持旧语义：不生成表级 HWM，也不强校验；
因此 V1 的 ALL + 无主键/无唯一键表在增量接管阶段存在重复 INSERT 风险。需要无主键表
正确去重时请使用 `full_load_engine=v2`。

chunk 边界在**该表快照事务内**规划（与读取共享同一 ReadView），改善负载均衡。开启
`optimize_index` 时，每张表数据全部提交后即异步重建该表索引，不必等到全部表灌数结束。

```json
{
  "full_load_engine": "v2",
  "full_load_read_workers": 4,
  "full_load_write_workers": 4,
  "full_load_buffer_mb": 128,
  "full_load_batch_bytes_mb": 4,
  "full_load_commit_rows": 10000,
  "full_load_commit_bytes_mb": 32,
  "full_load_lock_wait_timeout_sec": 10,
  "full_load_degrade_on_align_lock_fail": true
}
```

| 字段 | 含义 | 默认（0/未配置时） |
|------|------|---------------------|
| `full_load_engine` | 全量引擎，`v1` / `v2` | `v1` |
| `full_load_read_workers` | 读取 worker 数 | 4（范围 1–64） |
| `full_load_write_workers` | 写入 worker 数 | 4（范围 1–64） |
| `full_load_buffer_mb` | RowBatch 队列容量（MiB），背压阈值 | 128（范围 1–4096） |
| `full_load_batch_bytes_mb` | 单批目标字节数（MiB），达到即入队 | 4（范围 1–64） |
| `full_load_commit_rows` | 单事务累计提交行数阈值 | 10000（不小于 `batch_size`，上限 10000000） |
| `full_load_commit_bytes_mb` | 单事务累计提交字节阈值（MiB） | 32（不小于单批字节，上限 4096） |
| `full_load_lock_wait_timeout_sec` | 超大表对齐取锁等待超时（秒，双保险：客户端 context + `SESSION lock_wait_timeout`） | 10（范围 1–3600） |
| `full_load_degrade_on_align_lock_fail` | 对齐多连接取锁失败时是否降级为单连接快照；`false` 为 fail-closed | `true`（省略时同样按 true） |

说明：

- 所有 V2 字段为 0 或未配置时使用上表默认值（4C8G 平衡预设）。`full_load_read_workers`
  与 `full_load_write_workers` 会被夹在 `[1, 64]`；其余显式值超过上表上限时也会被夹紧，
  防止异常 API 参数造成整数溢出或无界内存申请。
- 未显式设置 `full_load_commit_rows` 但设置了 `tx_commit_every_n_parallel` 时，
  提交行数按 `batch_size × tx_commit_every_n_parallel` 推导，兼容旧调优习惯。
- V2 沿用 V1 的建表 / 删索引 / 索引回放（`optimize_index`、`index_restore_worker_count`）
  与目标库只读保护、`enable_drop_table_before_ddl`、`enable_skip_binlog` 语义。
- 全量批量写入仍为普通 `INSERT`：请确保目标表为空或开启 `enable_drop_table_before_ddl`。
- V2 运行期指标（读/写/提交行数与字节、队列水位、活跃 worker、事务重放与锁重试次数、
  活跃 snapshot group / 快照事务数、最老快照存活时长、对齐降级次数）通过 Prometheus
  `mysql_sync_full_load_*` 指标与任务详情接口暴露。
- `full_load_read_workers` 同时约束并发表数与单表并行读上限；快照连接信号量会为协调锁
  连接预留槽位，避免取锁自死锁。源库写入频繁时多个长 ReadView 会放大 undo / history list，
  请按源库承受能力保守设置 worker 数。
- `full_load_degrade_on_align_lock_fail=false` 时，超大表对齐取锁失败会直接让该表/任务失败；
  默认 `true` 则降级为单连接一致性快照继续。无论该开关如何，ALL 模式无主键表在捕获表级
  binlog HWM 时取锁失败始终 fail-closed。

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

**多 Sink 语义：** 同一 binlog 事务内全部事件对所有 Sink 写入并 flush 成功后，才统一推进 checkpoint / 任务存档位点（At-Least-Once）。MySQL durable mark 按 **Sink 独立** 判定重放跳过，不会作为全局完成标志；非 durable Sink（Kafka/Webhook）先提交，MySQL durable Sink 最后提交，避免外部 Sink 失败时 MySQL 已持久化 mark 导致事件永久丢失。任一事件或 Sink 失败则任务标记 FAILED，位点不越过该事务。Kafka/Webhook 重放时可能重复投递，接收方应使用 `trace_id`（或 Webhook 的 `Idempotency-Key` 头）去重。

**超大事务缓冲：** 增量订阅在 XID 前会缓冲整事务事件。内存软上限默认为约 `100000` 个 row map（UPDATE 的 before/after 都计入，故约 5 万行 UPDATE 即可能触及）以及约 `256MiB` 估算字节（用于大 BLOB）。超限后溢写到临时目录（默认 `$TMPDIR/mysql-to-sync-txn-spill`），XID 时再回放，避免硬失败形成无法跨越的毒事务。可通过 `SyncConfig.MaxTxnBufferedRows` / `MaxTxnBufferedBytes` / `TxnSpillDir` 调整。

**外部 Sink 事务缓冲硬上限：** Kafka/Webhook 在 `BeginTransaction`/`CommitTransaction` 路径下另有独立内存缓冲（默认 `100000` 事件 / `256MiB`，可通过 `max_txn_buffered_events` / `max_txn_buffered_bytes` 调整）。该缓冲与 subscriber 磁盘 spill **独立**；触顶后事务无法应用、checkpoint 不推进，Prometheus 指标 `mysql_sync_incremental_sink_txn_buffer_limit_total` 会递增。需调大 limit 或缩小源端事务规模。

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

> 每条变更事件会携带稳定 `trace_id`（格式 `{task_id}:{binlog_file}:{binlog_pos}:{row_index}`），Webhook 在未配置自定义 `Idempotency-Key` 头时自动将其写入 HTTP `Idempotency-Key`，供接收方幂等去重。重试耗尽后任务标记 FAILED，binlog 位点不推进。重启后从上次 checkpoint 重新消费并重试。

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
  "trace_id": "task_001:mysql-bin.000001:12345:0",
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
