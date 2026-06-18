这是一个针对 **MySQL-to-MySQL** 同步场景，且深度适配 **无主键表（No-PK Tables）** 的工业级设计文档。

---

# MySQL-to-MySQL 数据同步平台设计文档 (DTS-Go Pro)

## 1. 系统架构综述
本系统采用 DDD（领域驱动设计）架构，旨在解决 MySQL 之间高性能、高可靠的数据迁移与同步需求。特别针对“无主键”这一痛点，通过动态标识识别（Identity Detection）和全列匹配技术，确保数据一致性。

*   **全量阶段**：
    *   **有主键/唯一键**：基于 Range Sharding 的并发分片读取。
    *   **无主键**：基于流式游标（Cursor）的单协程深分页优化读取。
*   **增量阶段**：基于 Binlog Row 模式，通过 Before Image 补全 `WHERE` 条件。
*   **架构理念**：领域模型驱动策略选择（Strategy Pattern），基础设施层屏蔽库表差异。

### 技术栈
*   **后端**: Go 1.21+, Gin, GORM, `go-mysql-org/go-mysql`。
*   **前端**: Vue 3 + Arco Design Vue。
*   **存储**: Redis（保存 Checkpoint 位点）、MySQL（元数据与任务配置）。

---

## 2. DDD 领域模型设计

### 2.1 领域划分
*   **Task 领域**：任务生命周期管理、心跳监控、容错重试。
*   **Metadata 领域**：**核心逻辑**。负责表结构扫描，自动判定同步策略：`PK`（主键）、`UK`（唯一键）、或 `No-PK`（全列匹配）。
*   **Sync 领域**：执行单元。包含 DataReader（源端）、DataWriter（目标端）、Transformer（清洗）。

### 2.2 核心实体 (Entity & VO)
*   **TableIdentity (Value Object)**:
    *   `Strategy`: `PK_STRATEGY` | `UK_STRATEGY` | `FULL_COLUMNS_STRATEGY`。
    *   `IdentifyCols`: 标识一行数据所需的列集合。
*   **SyncTask (Aggregate Root)**:
    *   包含 `TaskConfig` 和 `ProcessContext`（当前位点、进度百分比、错误堆栈）。

---

## 3. 核心功能设计

### 3.1 差异化全量同步 (Full Sync)
*   **分片机制**：
    *   **有主键**：获取 `MAX(id)` 和 `MIN(id)`，按步长切分分片，Worker 协程并发拉取。
    *   **无主键**：无法切片。系统自动降级为 **单协程流式读取**，利用 Go 的 `sql.Rows` 迭代器降低内存占用，但在写入端保持多协程并发写入。
*   **断点续传**（任务存档 `FullSyncResume`，不依赖 Redis）：
    *   **表级**：`done=true` 的表在暂停/重启后直接跳过。
    *   **行级**（keyset / range 路径）：事务提交成功后记录主键游标；range 大表按 worker 分片各自保存 `shard_cursors`。
    *   **阶段级**：`SyncPhase=FULL_STARTED` 表示全量未完成；完成后变为 `FULL_COMPLETED`，ALL 模式可接增量。
    *   **限制**：`enable_drop_table_before_ddl` 时禁用续传；无主键表与 sample 并行路径仅表级续传。
    *   详见 [全量续传指南](../guides/FULL_SYNC_RESUME_GUIDE.md)。

### 3.2 增量同步 (Incremental CDC)
*   **逻辑主键定位**：
    *   **有主键**：生成的 SQL 为 `UPDATE ... WHERE id = ?`。
    *   **无主键**：利用 Binlog 提供的 Before Image，生成全列匹配的 SQL：
        `UPDATE/DELETE ... WHERE col1=? AND col2=? AND col3=? ... LIMIT 1`。
    *   *注意：必须附加 `LIMIT 1` 以防在存在完全重复行时误操作。*
*   **幂等处理**：
    *   所有 `INSERT` 自动转化为 `REPLACE INTO` 或 `INSERT ... ON DUPLICATE KEY UPDATE`。

### 3.3 无主键表专项安全策略
*   **环境预检**：启动前检查 `binlog_row_image` 是否为 `FULL`。若为 `MINIMAL` 且存在无主键表，则强制中止任务。
*   **冲突处理**：无主键表在同步过程中若 `WHERE` 条件匹配不到行，记录为“数据空漂移”异常并打入审计日志，不中断主流程。

---

## 4. 接口设计 (API)

| 模块 | 方法 | 路径 | 描述 |
| :--- | :--- | :--- | :--- |
| 任务 | POST | `/api/tasks` | 创建任务（包含自动表结构检测） |
| 元数据 | GET | `/api/metadata/tables` | 预览待同步表及标识策略（标注无主键表风险） |
| 监控 | GET | `/api/tasks/:id/metrics` | 实时 TPS、延迟、无主键匹配成功率 |
| 容错 | POST | `/api/tasks/:id/skip` | 跳过当前报错的位点 |

---

## 5. 工程目录结构

```text
dts-platform/
├── internal/
│   ├── metadata/                 # 元数据领域
│   │   ├── domain/service/       # IdentityAnalyzer (主键/无主键判定算法)
│   │   └── infrastructure/       # Schema 探测器 (查询 info_schema)
│   ├── sync/                     # 同步领域
│   │   ├── domain/strategy/      # PK/FullColumn 匹配策略实现
│   │   ├── application/          # 协调全量切换增量流程
│   │   └── infrastructure/       
│   │       ├── writer/sql_builder.go # 动态生成有主键/无主键 SQL
│   │       └── reader/cursor_reader.go# 无主键流式读取器
│   └── task/                     # 任务管理
├── pkg/
│   └── binlog/                   # 基于 go-mysql 的原生封装
└── web/                          # Vue 3 应用
```

---

## 6. 前端设计亮点

1.  **风险雷达**：在选择表页面，红色高亮显示“无主键表”，并提示“该表将采用全列匹配模式，同步性能可能受限”。
2.  **实时看板**：
    *   **TPS 曲线**：区分展示有主键表和无主键表的写入速率。
    *   **位点偏移量**：直观展示 Master Position 与当前处理 Position 的 Gap。
3.  **SQL 预览**：支持点击查看当前正在生成的增量 SQL 模板（可见 `WHERE col1=... LIMIT 1`）。

---

## 7. “最快”与“安全”的平衡优化

1.  **批量写入 (Batching)**：
    无论是全量还是增量，数据进入 `Buffer`，满足 1000 条或 500ms 触发一次批量提交，最大化利用 MySQL 吞吐量。
2.  **并发冲突规避**：
    *   **有主键**：按 `Hash(id) % WorkerNum` 分发，保证同一行的操作顺序。
    *   **无主键**：按 `Hash(All_Columns) % WorkerNum` 分发，保证物理完全一致的行进入同一处理器。
3.  **连接池优化**：
    针对全量读取单独设置连接池，防止大数据量拉取占满业务连接。
4.  **无主键安全阈值**：
    支持设置 `Limit 1` 保护开关，默认开启，防止在非严格模式下的目标库出现多行误删。

---

## 8. 实施计划 (Roadmap)

1.  **Phase 1**: 核心元数据扫描逻辑（自动区分 PK/UK/No-PK）。
2.  **Phase 2**: 实现全量分片与无主键流式读取的双重引擎。
3.  **Phase 3**: 实现基于 Before Image 的增量 SQL 构建器（全列匹配逻辑）。
4.  **Phase 4**: 引入 Redis Checkpoint，实现断点续传与监控 API。
5.  **Phase 5**: 前端可视化看板与异常审计后台。

---

**设计总结**：本方案在原有 DTS 基础上，通过在 **Metadata 领域** 引入 `IdentityStrategy`，在 **Infrastructure 层** 引入 `FullColumnMatch` 算法，完美兼容了无主键表的极端场景，是目前 Go 实现 CDC 系统中最严谨的工程实践路径。