# FULL / ALL 语义拆分与 FullLoadV2 去长事务改造方案（草案）

> 状态：Completed - 阶段 0/1/2/3 均已落地
>
> 已完成摘要：
> - 阶段 0：禁用并行失败自动降级；`full_load_degrade_on_align_lock_fail` 仅兼容占位
> - 阶段 1：FULL V2 普通短查询路径
> - 阶段 2：ALL 无锁 P0/P1 + bounded catch-up；索引恢复在 catch-up 之后；`allow_nopk_all`；旧 HWM ALL 要求 fresh run
> - 阶段 3：删除 aligned snapshot/limiter/HWM 运行时与快照指标；废弃配置仅兼容接收
>
> 核心原则：`FULL` 只负责一次性基线拷贝；`ALL` 才负责覆盖全量期间发生的增量变更。
> 并发原则：并发度由用户在任务启动前决定；运行时不得把失败的并行任务自动降级为单线程。

## 1. 背景与问题

当前 FullLoadV2 为了让所有模式都获得“表级一致视图”，引入了：

- 每表 `REPEATABLE READ + WITH CONSISTENT SNAPSHOT` 长事务；
- 大表通过 `FLUSH TABLES db.table WITH READ LOCK` 对齐多个 ReadView；
- 对齐、权限、连接或事务初始化失败后，默认降级为单连接、单事务串行读取全部 chunk；
- ALL + 无 PK/UK 表使用表级 HWM 衔接增量；
- snapshot group、snapshot txn、oldest snapshot 等配套状态和指标。

这套实现把 `FULL` 和 `ALL` 的语义混在了一起，带来以下问题：

1. `FULL` 本来就不承诺捕获执行期间的增量，却承担了长事务、表锁和快照对齐成本。
2. 大表并行路径失败后静默退化为单连接串行，改变了用户选择的执行策略，可能让一张表的
   事务持续数小时，并把明确失败伪装成“仍在运行”的半成功状态。
3. `openAlignedTableSnapshots` 的连接、锁、事务、引擎校验等任意错误都可能被归类成
   `align lock failed`，真实根因被掩盖。
4. 长 ReadView 阻止 InnoDB purge、增加 undo/history list 压力，并长期阻塞相关 DDL。
5. 代码、配置、恢复状态和运维指标复杂度显著增加，但没有对应清晰的模式语义。

本方案删除“所有模式都必须表级一致快照”的前提，重新定义 FULL、ALL 和 INCREMENTAL。
自动降级本身按缺陷处理，不再把它保留为可配置的容错能力。

## 2. 模式语义

| 模式 | 目标 | 全量期间发生的变更 | 一致性承诺 |
|---|---|---|---|
| `FULL` | 尽快生成一次目标端基线 | 不捕获，可能丢失 | 尽力拷贝；源端持续写入时不保证时点一致 |
| `ALL` | 基线拷贝后持续增量同步 | 必须从全量开始前的 binlog 位点回放 | PK/UK 表最终收敛；无 PK/UK 表支持同步，但需用户确认无法严格保证去重和最终一致性 |
| `INCREMENTAL` | 恢复已经建立的增量任务 | 从已有 checkpoint 继续 | 不负责为一个普通 FULL 任务补齐历史空窗 |

### 2.1 FULL 明确接受的风险

FULL 执行期间源表仍有写入时，允许出现：

- 某些并发 INSERT/UPDATE/DELETE 未进入目标；
- 不同 chunk 看到不同时间点的数据；
- 主键迁移、唯一列改值时出现暂态重复或漏行；
- 恢复唯一索引时因源端并发变化而失败。

产品、API 和 UI 必须明确提示：

> FULL 是一次性基线拷贝，不保证执行期间产生的增量数据。需要完整迁移请使用 ALL；
> 需要严格静态副本则应在 FULL 期间暂停源端写入。

代码不再替 FULL 隐式承担一致性保证。

### 2.2 FULL 的时间语义：允许 T1 / T2 / T3 混合

FULL 不承诺一张表的所有行来自同一个数据库时刻。

例如同一张表被拆成三个 chunk：

```text
chunk-1 在 T1 读取
chunk-2 在 T2 读取
chunk-3 在 T3 读取
```

- 如果源表在 T1～T3 期间没有写入，三个时间点对应的数据状态自然相同，无需依赖长事务、
  FTWRL 或多连接 ReadView 对齐。
- 如果源表在 T1～T3 期间持续写入，FULL 允许结果由多个时间点的数据组成；这是用户选择
  “一次性全量”时接受的语义，不得在代码内部擅自提升为严格时点快照。
- 如果用户要求静态副本，应由用户在 FULL 期间暂停源端写入；如果用户要求不停写且最终
  收敛，应选择 ALL。

阿里云 DTS、华为云 DRS、腾讯云 DTS 的公开产品约束均把“仅全量”和“全量+增量”分开：
仅全量要求或建议源端停写；源端持续写入时使用全量+增量。公开文档没有承诺“动态源库下，
仅 FULL 仍能得到单表统一时点快照”。本项目采用这一产品语义，不把云厂商未公开的内部实现
当成必须复制的机制。

公开语义参考：

- [阿里云 DTS：仅全量迁移期间请勿写入，需要不停机时选择全量+增量](https://help.aliyun.com/zh/dts/user-guide/overview-of-data-migration-scenarios/)
- [华为云 DRS：全量模式要求无业务写入，全量+增量允许源库写入](https://support.huaweicloud.com/realtimemig-drs/drs_03_1167.html)
- [腾讯云 DTS：全量适用于无写入场景，全量+增量通过 binlog 回放收敛](https://cloud.tencent.com/document/product/571/59387)

### 2.3 故障语义：失败必须可见，禁止改变执行策略

- 用户配置并行读取，就按该并发策略执行；任一 reader 初始化、连接、权限、查询、规划或
  写入失败，在同策略的有界重试耗尽后立即失败。
- 用户需要单线程时，应在前端/API 显式选择 `read_workers=1`；单线程不是运行时兜底。
- 禁止“并行失败后自动切换单 reader”“锁失败后继续串行”“权限失败后降级运行”等行为。
- 错误必须保留 MySQL 原始错误，并至少包含：任务、库表、chunk（如有）、读取策略、失败阶段。
- 任务失败优先于长时间展示一个已经偏离用户配置、最终结果仍无保证的“运行中”任务。

## 3. 目标架构

### 3.1 FULL：无表级快照、无对齐锁

流程：

1. 初始化目标端 DDL、staging 和写入会话。
2. 按现有 identity 策略规划读取：
   - PK/UK：`keyset`、`range`、`sample`；
   - 无 PK/UK：`nopk` 流式读取。
3. 每个 PK/UK batch 使用普通短查询/autocommit，可跨连接并行。
4. 不执行以下操作：
   - `START TRANSACTION WITH CONSISTENT SNAPSHOT`；
   - 表级 `FLUSH TABLES ... WITH READ LOCK`；
   - aligned snapshots；
   - table HWM 捕获；
   - snapshot group 限流；
   - 任何形式的自动串行降级。
5. 全量数据写完后按现有流程恢复索引、发布表并完成任务。

无 PK/UK 表仍可使用单条流式 `SELECT`，但不再额外包裹显式表级 RR 长事务。
该 SELECT 的实际执行时长仍需 query idle/max-duration 保护。

普通短查询意味着同表不同 chunk 可以分别看到 T1、T2、T3 的已提交状态。实现和测试不得再
把“单表统一时点”作为 FULL 的正确性条件。

### 3.2 ALL：基线扫描 + binlog 收敛

ALL 不通过多个长事务获得统一 ReadView，而是通过 binlog 使不同时间读取的基线最终收敛。

建议流程：

1. 在任何全量 reader 启动前读取当前 binlog position，记为 `P0`。
2. 持久化 `P0`，作为本次 ALL 的增量起点。
3. 不使用 FTWRL；普通读取 `SHOW MASTER STATUS` / 等价位点接口即可。
4. 执行无表级快照的并行基线扫描。
5. 基线扫描结束时读取当前 binlog position，记为 `P1`。
6. 从 `P0` 按事务顺序回放 binlog，至少追到 `P1`。
7. catch-up 到 `P1` 后，目标端应收敛到 `P1` 对应状态。
8. 恢复延迟创建的唯一索引和普通索引。
9. 将状态切换为持续增量，从已提交 checkpoint 继续消费 `P1` 之后的事件。

因为 P0 必须在所有 reader 启动前捕获，所以：

- P0 之前提交的数据应由后续基线查询读取；
- P0 之后提交的数据无论是否已被某个基线 chunk 看到，都会再次通过 binlog 回放；
- PK/UK 表使用 upsert/delete 按事务顺序回放，最终状态由 binlog 收敛。
- 无 PK/UK 表继续回放 INSERT/UPDATE/DELETE，但因缺少稳定身份，基线与增量重叠窗口内只能
  提供 best-effort 语义，具体风险必须在任务创建时由用户确认。

ALL 的基线扫描与 FULL 一样允许同表不同 chunk 读取 T1/T2/T3；ALL 的一致性来自 P0 之后
binlog 的顺序回放和最终追平，不来自长事务或 ReadView 对齐。

### 3.3 ALL 基线写入规则

ALL 与 FULL 的写入语义分开：

- FULL：继续使用普通 `INSERT`。
- ALL：
  - PK 表按 PK upsert；
  - UK-only 表按选定 UK upsert；
  - 无 PK/UK 表继续使用 `FullColumnsStrategy`：
    - INSERT 使用普通 INSERT；
    - UPDATE/DELETE 使用 binlog before image 生成全列 WHERE；
    - 遵守 `enable_limit_one`；
  - DELETE 必须按 identity 删除；
  - 事务提交顺序必须与 binlog 顺序一致。

ALL 期间必须保留用于 identity 的 PK/UK 索引。其他唯一索引可以延迟创建，避免基线的
跨时刻暂态数据在 catch-up 前触发 1062。

若 `optimize_index=false`，ALL 仍应至少延迟非 identity 唯一索引；否则可能在增量尚未收敛前失败。

### 3.4 并发选择与 fail-fast

并发度是用户配置，不是运行时自适应容错项：

- `read_workers=1`：用户明确选择单 reader；
- `read_workers>1`：用户明确选择并行 reader；
- 运行期间不得从 `N` 自动改成 `1`；
- 可配置重试只能重试失败的同一策略/同一 chunk，不得借重试改变并发度或一致性语义；
- 重试耗尽后任务立即失败，并向 API/UI 返回可操作的根因。

典型错误格式：

```text
full read failed: task=<task_id> table=<schema.table> strategy=<range|sample|keyset|nopk>
chunk=<chunk_id> stage=<connect|plan|query|scan|enqueue|write> mysql_code=<code> cause=<original error>
```

前端不得把任务仅展示为笼统的“同步失败”，必须展示上述核心字段和后端原始原因。

## 4. 无 PK/UK 表能力边界

无 PK/UK 表没有稳定行身份，无法可靠判断：

- 基线读到的某行与 binlog INSERT 是否为同一逻辑行；
- 多条完全相同行中的哪一条被 UPDATE/DELETE；
- 基线已经包含的 INSERT 是否应在回放时去重。

删除表级一致快照和表级 HWM 后，ALL 仍然允许同步无 PK/UK 表，但必须把它定义为
“支持执行、不承诺严格一致”，不能因为无法严格去重就直接禁止用户同步。

推荐策略：

1. `ALL` 创建或编辑任务时做 metadata preflight。
2. 如果存在无 PK/UK 表，一次性返回并展示全部表名、估算行数和风险等级。
3. 前端弹出风险确认框，明确说明：
   - 全量扫描已经读到的并发 INSERT，可能在 P0 之后的 binlog 回放中再次插入；
   - 完全重复行没有稳定身份，UPDATE/DELETE 只能依赖 before image + 全列匹配；
   - `enable_limit_one=true` 时只能尽量避免一次影响多条重复行，不能恢复逻辑上的“第几条”；
   - 最终行数和内容可能与某个严格时点不完全一致；
   - 补充 PK/UK 或在迁移期间停写可以提升一致性。
4. 用户可以选择：
   - 继续 ALL，并接受无 PK/UK 表的一致性风险；
   - 返回修改任务，排除这些表；
   - 先为源表补充 PK/UK；
   - 改用停写窗口。
5. 用户选择继续后，前端必须提交显式确认字段，例如：
   - `allow_nopk_all=true`；
   - 后端收到确认后记录 `nopk_all_risk_acknowledged_at`；
   - 登录态可用时由后端记录确认人，不能信任客户端自报时间或身份。
6. 后端只有在请求明确确认后才允许新建/启动该 ALL 任务，避免旧前端或脚本在没有提示的
   情况下静默进入 best-effort；这不是禁止同步，而是确保风险决策确实由用户完成。

运行期间继续使用现有 `FullColumnsStrategy` 和 ROW/FULL before image 处理无 PK/UK 的
UPDATE/DELETE。进度日志、任务详情和最终结果中必须持续标记：

```text
consistency=best_effort reason=no_primary_or_unique_key
```

任务完成状态表示“同步流程完成”，不表示这些表已获得严格一致性保证。

## 5. 索引恢复顺序

当前流程是：

```text
基线扫描 → 恢复索引 → 标记 FULL_COMPLETED → 启动增量
```

ALL 应改成：

```text
捕获 P0
→ 基线扫描
→ 捕获 P1
→ 回放 P0..P1
→ 确认 catch-up 完成
→ 恢复非 identity 索引
→ 标记基线和 catch-up 完成
→ 持续增量
```

原因：非快照基线可能产生暂态唯一键冲突，必须先让 binlog DELETE/UPDATE 收敛，再恢复唯一索引。

FULL 保持现有“扫描结束后立即恢复索引”的流程；恢复失败时明确提示源端写入可能导致
FULL 跨时刻数据不一致，并建议改用 ALL 或停写后重跑。

## 6. 状态机建议

保持 `TaskStatus` 与 `SyncPhase` 分离。

可选方案 A（较小改动）：

- `FULL_STARTED` 覆盖 ALL 的基线扫描和 bounded catch-up；
- catch-up 到 P1、索引恢复完成后才进入 `FULL_COMPLETED`；
- 随后进入现有增量运行状态。

可选方案 B（可观测性更好）：

- `FULL_SCANNING`
- `FULL_CATCHING_UP`
- `FULL_RESTORING_INDEXES`
- `FULL_COMPLETED`
- `INCREMENTAL_RUNNING`

推荐先采用 A，并在 `context` 中新增非生命周期字段：

- `full_sync_end_position`（P1）；
- `full_sync_catchup_position`；
- `full_sync_subphase`。

避免一次改动扩大持久化状态机兼容范围。

## 7. 代码改造范围

### 7.1 `internal/task/application/service`

- `executeFullSync`
  - FULL 不捕获 binlog position；
  - ALL 在 reader 启动前无锁捕获 P0；
  - ALL 基线结束后捕获 P1；
  - ALL 在索引恢复前执行 bounded catch-up。
- `captureFullSyncStartPosition`
  - 拆成普通 `captureBinlogPosition`；
  - 删除全局 FTWRL，只保留查询位点的短超时。
- `syncDatabasePairV2`
  - 将 task mode 传入 FullLoadV2；
  - FULL 和 ALL 使用不同 writer 语义；
  - 不再注入 table snapshot/HWM callback。
- `executeIncrementalSync`
  - 抽出“追到指定 P1 后返回”的 bounded catch-up；
  - 持续增量复用同一事务处理和 checkpoint 逻辑。
- `restorePendingIndexes`
  - FULL：基线后执行；
  - ALL：catch-up 到 P1 后执行。

### 7.2 `internal/sync/fullload`

- 删除读取正确性对以下组件的依赖：
  - `tableSnapshot`；
  - `openAlignedTableSnapshots`；
  - `snapshotLimiter`；
  - `SnapshotOptions`；
  - `CaptureTableHWM`；
  - `OnTableSnapshotReady`；
  - `DegradeOnAlignLockFail`。
- reader 直接使用 `*sql.DB` / 普通短查询。
- PK/UK chunk 继续通过全局 chunk queue 并行调度。
- no-PK 保留流式 reader，但不额外开启显式 RR 快照事务。
- 保留 query open、idle 和 max-duration 保护；错误必须包含表名和读取策略。
- 删除自动降级分支；并发 reader 失败不得再次调用单 reader 路径。
- 保留的有界重试必须使用原并发策略，不得修改 `ReadWorkers`、reader 数或 chunk 规划。

### 7.3 `internal/sync/application`

- 增加 bounded catch-up：
  - 起点 P0；
  - 目标 P1；
  - 仅当完整事务提交位点达到或超过 P1 时返回；
  - checkpoint 必须与目标事务原子顺序保持一致。
- PK/UK 事件继续使用 upsert/delete 收敛。
- 删除 V2 ALL 对 `table_binlog_hwms` 和 `RequireNoPKTableHWM` 的依赖。
- 无 PK/UK 事件继续走 `FullColumnsStrategy`、before image 和 `enable_limit_one`。
- ALL 的 metadata preflight 返回无 PK/UK 风险清单和一致性等级，不直接阻断同步。
- 后端校验用户是否提交 `allow_nopk_all` 风险确认；确认后允许继续。

### 7.4 `internal/task/domain/entity`

- 保留历史 JSON 字段以兼容旧存档，但停止生成：
  - `table_binlog_hwms`；
  - snapshot 相关运行统计。
- 新任务不再写入表级 HWM。
- 旧任务读取时允许历史字段存在，但不得据此启用旧快照路径。

### 7.5 API 与 Web

- FULL 创建页增加醒目说明：
  - 不捕获全量期间的变化；
  - 同表不同 chunk 可能读取不同时间点；
  - 在线迁移请选择 ALL。
- FULL/ALL 创建页明确提供单线程/并行读取选择，并说明运行中不会自动切换：
  - 单线程由用户显式选择；
  - 并行失败将直接报错，不会降级后继续运行。
- ALL 创建页在 metadata 分析后展示无 PK/UK 风险列表：
  - 表名、估算行数、匹配策略；
  - 可能重复 INSERT、无法精确定位重复行、最终状态不保证；
  - “继续同步”与“返回修改”两个明确动作；
  - 用户勾选“我已理解无主键/唯一键表无法保证严格一致性”后才能继续。
- 任务详情和运行结果持续展示每张表的 `strict` / `best_effort` 一致性等级。
- 隐藏/废弃：
  - `full_load_lock_wait_timeout_sec`（表级对齐用途）；
  - `full_load_degrade_on_align_lock_fail`；
  - snapshot group/connection 调优参数。
- 保留一段兼容期：API 接收旧字段但忽略，并返回 deprecated warning。

## 8. 配置迁移

以下配置进入 deprecated：

- `full_load_degrade_on_align_lock_fail`
- 表级对齐用途的 `full_load_lock_wait_timeout_sec`
- `full_load_max_snapshot_groups`
- `full_load_max_snapshot_conns`

以下配置继续保留：

- `full_load_query_timeout_sec`
- `full_load_stream_idle_timeout_sec`
- `full_load_stream_max_duration_sec`
- reader/writer/batch/buffer/commit 调优字段

`full_load_degrade_on_align_lock_fail` 在兼容期内仅允许被 API 读取和回显 deprecated warning，
执行引擎必须忽略该字段，且不得发生任何自动降级。

旧任务迁移规则：

1. 未开始任务：按新语义执行。
2. 已完成 FULL：保持结果，不承诺补齐历史变化。
3. 已进入 INCREMENTAL：仅从已有 checkpoint 恢复。
4. 使用旧 snapshot/HWM 语义但未完成的 ALL：不得混合恢复，要求 fresh run。
5. ALL 包含无 PK/UK 表：
   - 新任务必须有显式风险确认；
   - 确认后允许启动；
   - 旧任务首次按新版本启动时要求用户补一次确认，确认后可继续。

## 9. 测试计划

### 9.1 FULL

- 断言源端不执行：
  - `START TRANSACTION WITH CONSISTENT SNAPSHOT`；
  - 表级/global FTWRL；
  - table HWM 查询。
- PK/UK 多 chunk 使用多个普通连接并行读取。
- 构造 chunk-1/T1、chunk-2/T2、chunk-3/T3 的源端并发变化，断言任务可完成但不要求单表
  时点一致，并且结果/日志不得宣称 snapshot consistency。
- 索引恢复冲突应返回清晰的 FULL 语义提示。
- 无 PK/UK 流读取超时能关闭 Rows，不残留连接/事务。
- 任一并行 reader 的连接、权限、规划或查询失败后，不得调用单 reader 路径。
- 错误必须带表名、chunk、策略、阶段和原始 MySQL 错误。
- 用户配置 `read_workers=1` 时走显式单线程路径；该行为不得与故障降级共用状态或指标。

### 9.2 ALL（PK/UK）

- P0 必须早于所有全量 reader。
- P1 必须晚于所有基线读取完成。
- 并发 INSERT/UPDATE/DELETE/PK move 后，catch-up 到 P1 的目标状态与源端 P1 状态一致。
- 增量追平前不得恢复非 identity 唯一索引。
- checkpoint 保存失败不得标记 catch-up 完成。
- binlog 已清理、无法从 P0 订阅时 fail-closed。
- pause/restart 后从持久化 checkpoint 继续，不重复发布事务。

### 9.3 ALL（无 PK/UK）

- preflight 一次性返回全部风险表，不直接拒绝任务。
- 未提交 `allow_nopk_all` 时，API 返回“需要用户确认”，而不是“不支持”。
- 提交确认后任务可以创建、启动并完成。
- INSERT 继续执行，UPDATE/DELETE 使用 before image + 全列 WHERE。
- `enable_limit_one` 开关在 SQL 和行为测试中保持有效。
- 构造基线与 binlog INSERT 重叠，验证任务可完成，同时结果和日志标记 `best_effort`。
- 构造完全重复行 UPDATE/DELETE，验证不会错误宣称严格一致。
- 前端必须覆盖查看风险、取消、确认继续三条交互路径。

### 9.4 回归

- FULL/ALL 的目标端 DDL、drop-before-DDL、staging、schema lock 不回退。
- 增量 ROW/FULL image 校验保持。
- no-PK 的纯 INCREMENTAL before-image SQL 行为保持，但不得把它解释成可为新 ALL 建立无损基线。
- 原 `DegradeOnAlignLockFail` 开关无论 true/false 都不得触发运行时切换单 reader。
- 权限错误（含 1227）、连接错误、事务/查询初始化错误必须立即进入失败状态并通过 API 可见。

## 10. 可观测性

删除或废弃：

- `snap_groups`
- `snap_txns`
- `oldest_snap`
- `align_degrades`

新增：

- FULL：
  - 当前表、策略、chunk、batch rows/bytes、query duration；
  - 配置并发数与实际并发数（两者不得因错误自动改变）；
  - 明确的 `consistency=best_effort`。
- ALL：
  - P0、P1、当前 catch-up position；
  - binlog lag bytes/events/time；
  - `BASE_SCAN` / `CATCH_UP` / `RESTORE_INDEX` / `STREAMING` 子阶段；
  - 无 PK/UK preflight 风险结果、用户确认状态和每表一致性等级。

每条周期进度日志必须包含当前表，避免只看到任务累计行数而无法定位慢表。

## 11. 分阶段实施

### 阶段 0：立即纠正自动降级缺陷

- FULL、ALL 一律禁用“并行失败后自动切换单 reader”。
- `full_load_degrade_on_align_lock_fail` 立即变为兼容占位字段，执行引擎忽略其值。
- 对齐锁、权限、连接、规划、事务初始化和查询错误在同策略有界重试耗尽后立即失败。
- API/UI 必须展示表名、读取策略、失败阶段、MySQL 错误码和原始原因。
- 增加回归测试：任何错误都不得改变用户配置的 reader 数，不得增加降级计数，不得继续创建
  单 reader 长事务。

### 阶段 1：纠正 FULL

- FULL 绕开全部 snapshot group/alignment/HWM 逻辑。
- 恢复普通并行 chunk 读取。
- 删除并行失败自动切单 reader 的分支；所有失败带完整上下文返回 API/UI。
- 单线程仅由任务配置显式选择。
- 更新 API/UI/文档警告。
- 暂不改变 ALL。

### 阶段 2：重做 ALL（PK/UK）

- 无锁捕获 P0/P1。
- 实现 bounded catch-up。
- 调整索引恢复顺序。
- ALL preflight 返回无 PK/UK 风险清单。
- 增加前端风险确认和后端确认字段校验；确认后继续同步。

### 阶段 3：删除旧架构

- 删除 aligned snapshot、limiter、table HWM，以及阶段 0 已禁用的自动降级兼容代码。
- 清理配置、指标和兼容代码。
- 完成旧任务迁移测试。

## 12. 验收标准

1. FULL 源端不存在由本程序创建的跨 batch/跨 chunk 显式长事务。
2. FULL 大表按配置并行读取，不再经过对齐锁或单连接降级。
3. FULL 文档和 UI 明确声明不保证执行期间的增量变化，也不保证同表所有 chunk 来自统一时点。
4. ALL 的 PK/UK 表在全量期间持续写入时，catch-up 后不漏最终状态。
5. ALL 在追到 P1 前不恢复延迟索引、不标记 FULL_COMPLETED。
6. ALL 遇到无 PK/UK 表时不禁止同步；前端必须展示风险，用户确认后可以继续。
7. 无 PK/UK 表的任务详情、日志和结果始终标记 `best_effort`，不得虚假宣称严格一致。
8. 不再出现 `align_degrades` 和数小时 `oldest_snap`。
9. 并发任务任一 reader 失败后立即返回包含表名和根因的错误，实际并发度不得自动变为 1。
10. 只有用户显式选择单线程时才允许 `read_workers=1`。
11. `go test ./...`、`go vet ./...` 和前端构建通过。

## 13. 待确认决策（已确认）

1. **无 PK/UK 确认**：字段 `allow_nopk_all`；后端记录 `nopk_all_risk_acknowledged_at`（登录态可记确认人）。旧任务首次按新版本启动时，在启动/编辑页补一次确认。
2. **catch-up 第一版**：全量结束后从源 binlog 回放 P0..P1（不先做 durable spool）。
3. **staging**：不强制；允许在已清空目标表上直接基线 + catch-up；`full_load_enable_staging` 仍可选。
4. **FULL 确认**：仅 UI/文档风险提示，不增加额外勾选（阶段 1 已落地）。
5. **旧未完成 V2 ALL**：一律要求 fresh run，禁止混合恢复旧 snapshot/HWM 语义。
