# 全量一致性快照（表级 snapshot group）实施计划（修正版）

## 0. 一句话结论

把 V2 全量从「跨表共享 chunk 队列 + 非一致性快照并行读」重构为「**表级 snapshot group**：同一张表的所有 chunk reader 绑定到同一个（或经短暂表锁对齐的一组）RR 只读快照事务」，从而在**保留表间并行与单表内多连接并行**的同时，消除「同一逻辑行的旧版本、新版本被不同 chunk 在不同时刻分别捞入 → 最终重建唯一索引报 1062」这类问题。

ALL 模式必须额外解决「**快照—binlog 位点原子绑定**」与「**事务级 binlog 位置协议**」两个 P0 问题，否则 snapshot group 做对了，接缝处仍会重复/漏放。

本计划只描述设计与改造点，不含代码实现。

---

## 1. 背景与根因

### 1.1 现象

表级 V2 全量，开启 `OptimizeIndex`（先删非主键索引、灌数、最后重建）。全部数据灌完后，阶段3 重建唯一索引失败：

```
restore indexes for xk-asset_20260721.pv_ele_bill:
create index `uniq_tranfer_channel_bill_key` ...:
Error 1062 (23000): Duplicate entry '光E宝-9dc692fb...' for key 'pv_ele_bill.uniq_tranfer_channel_bill_key'
```

报错是**唯一键**冲突而非主键冲突：目标表存在两条**主键不同、唯一键相同**的行。

### 1.2 根因

不是「先删索引最后重建」这个动作本身有错（云厂商同款优化也这么做），而是 **V2 全量读取是明确的非一致性快照**：

- `internal/sync/fullload/reader.go` 里每个 chunk 用 `r.db.QueryContext` 从连接池取连接、各自独立短查询，跨连接、跨时刻执行。
- `internal/task/application/service/task_service.go` 注释写死 `non-snapshot mode -> cross-worker time skew accepted`。
- 灌数期间 `UNIQUE_CHECKS=0` 且唯一索引已被 drop（`internal/sync/fullload/session.go`）。

于是当源库在全量期间发生「删旧主键 + 新主键重插同一业务记录」（或唯一列被改成另一行的值）时，旧行与新行会被不同 chunk 在不同时刻分别读入目标，主键不撞、唯一键撞，攒到阶段3 重建索引才爆。

### 1.3 修复方向

让**同一张表**的全量数据来自一个**表级一致的快照视图**，使复制出的数据在快照时刻本身自洽，重建唯一索引不再冲突；ALL 模式再由增量从「与该快照原子对齐的位点」精确追平。

---

## 2. 目标与非目标

### 2.1 目标

- 消除非快照并行读导致的唯一键重复（1062）与同类跨时刻不一致。
- 保留表间并行；保留单表内多连接并行（大表）。
- 不引入贯穿整个任务的长锁；显式写阻塞锁控制在「每表初始化 snapshot group」的毫秒~秒级窗口内。
- ALL 模式全量→增量接缝零重复、零漏放（对有 PK/UK 表最终收敛，对无 PK/UK 表严格正确）。

### 2.2 非目标

- 不追求跨表全局一致性快照（本问题是单表内唯一索引，不需要）。
- 不改变目标端「全局 batch/writer 队列」结构（写入侧不按表拆散）。
- 不新增「全量断点续传」能力（保持现有语义，见 §9）。
- 不改变现有行数对比、任务结束等既有功能。

---

## 3. 核心架构：表级 snapshot group

### 3.1 定义

一个 **snapshot group** = 一张源表 + 一组绑定到该表一致快照的读取事务 + 该表的 chunk 集合。约束：

1. **同一张表的所有 chunk reader 必须属于同一个 snapshot group**（见 §4 的正确性证明）。
2. 每个 reader 绑定专属 `*sql.Conn` / `*sql.Tx`，事务隔离级别 `REPEATABLE READ`、`READ ONLY`、`WITH CONSISTENT SNAPSHOT`。
3. group 内 chunk 可在**本表内**工作窃取；worker 不得带着事务跨表窃取。
4. group 内某 reader 读完自己负责的 chunk 即可 `COMMIT`；该表最后一个读取事务结束时，表级 MDL_SHARED_READ 释放。
5. 目标端写入完成后，重建该表索引（沿用现有阶段3 逻辑，仅调整触发时机为「该表所有 group 读取+写入落库完成后」，或维持任务末尾统一重建——见 §9 取舍）。

### 3.2 建立方式（两档，见 §5 决策矩阵）

- **单连接快照（中小表）**：一条连接开 RR 只读一致性快照，顺序读完该表所有 chunk。零显式写阻塞锁。
- **多连接对齐快照（超大表）**：短暂对该表加只读锁，令 N 条连接各自建立已对齐的 ReadView，随后解锁并行读。

多连接对齐流程（超大表）：

```sql
-- 协调连接
FLUSH TABLES `s`.`t` WITH READ LOCK;         -- 仅冻结该表写入与 DDL

-- 每条读取连接（共 N 条），在持锁期间：
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY;
SELECT <pk_col> FROM `s`.`t` LIMIT 1;          -- 必须真正访问该表以持有其 MDL_SHARED_READ

-- 全部 N 条建立完成后，协调连接立即
UNLOCK TABLES;
```

要点：
- `WITH CONSISTENT SNAPSHOT` 的 ReadView 是**各事务独立**的，多个连接不能共享同一个 ReadView。之所以能对齐，是因为建立期间该表被冻结、不可变，所以各 ReadView 看到的**该表数据版本一致**。
- 末尾那条普通 `SELECT` 不能带 `FOR SHARE/FOR UPDATE`；它的作用是让事务真正打开该表、持有表级 MDL_SHARED_READ，使解锁后仍能阻止 DDL 改表结构（MVCC 负责数据一致性，MDL 只负责结构稳定，两者职责分开）。

---

## 4. Planner 与 snapshot group 的关系（修正点 1）

### 4.1 正确表述

> **Planner 可在快照外规划边界；但一张表的所有 chunk reader 必须属于同一个 snapshot group。**

- chunk 边界（`internal/sync/fullload/chunk.go` 现有的 `planIntegerRange` / `planKeysetBoundaries`）继续复用，可继续用池连接计算 `MIN/MAX`、keyset 采样。
- 边界来自稍早/稍晚时刻，只影响**负载均衡**，不影响正确性——前提是这些 chunk 由**同一个表级快照**读取。

### 4.2 为什么「同表同快照」是硬约束（反例）

若边界=100，某行 PK 在两个**独立**快照之间从 90 更新为 110：

- 低区间(`pk<=100`)读较晚快照、高区间(`pk>100`)读较早快照 → 两边都看不到该行（**漏读**）；
- 反过来 → 该行被读两次（**重复**）。

只有当同表所有 reader 共享/对齐到同一表级快照时，无缝无叠的 PK 区间才能保证「不漏不重」。因此：Planner 在哪个连接算边界无所谓，**读取必须在同一 snapshot group 内**。

---

## 5. 分档决策矩阵（修正点 2）

| 场景 | 推荐方式 | 显式写阻塞锁 | binlog 处理 |
|---|---|---|---|
| FULL，中小 InnoDB 表 | 单连接、单 RR 只读一致性快照，顺序读 chunks | 无 | 不涉及 |
| FULL，超大 InnoDB 表 | 短暂表级 FTWRL 对齐 N 个 ReadView，解锁后并行读 | 毫秒~秒级 | 不涉及 |
| ALL，有 PK/UK 的中小表 | 可选「零显式锁 + 全局位点重放」，依赖幂等/最终收敛 | 无 | 依赖幂等收敛 |
| ALL，无 PK/UK 的任意表 | 必须短暂表锁，把 ReadView 与**表级 binlog 位点原子绑定** | 毫秒~秒级 | 精确表级高水位 |
| ALL，超大表 | 表级 FTWRL + snapshot group + 精确表级高水位 | 毫秒~秒级 | 精确表级高水位 |

### 5.1 关键矛盾（必须写进设计约束）

普通一致性快照**不会返回与 ReadView 原子绑定的 binlog 坐标**：

- 「先取位点，再建快照」→ 位点更早 → `[位点, 快照]` 区间事件被**重复应用**（重复窗口）；
- 「先建快照，再取位点」→ 位点更晚 → `[快照, 位点]` 区间事件被**漏放**（漏放窗口）。

因此「ALL 模式所有中小表都零锁」与「严格表级高水位过滤」**不可兼得**：
- 有 PK/UK 表可走「零锁 + 幂等收敛」，容忍重复窗口靠 upsert/delete-by-key 自愈；
- 无 PK/UK 表要严格正确，就必须付出「短暂表锁 + 原子绑定位点」的代价。

### 5.2 其它前提

- `WITH CONSISTENT SNAPSHOT` 仅对 **InnoDB** 且在 **RR** 隔离级别下提供预期语义。计划必须明确**非 InnoDB 表**（MyISAM 等）的策略：拒绝该表 / 降级为「短暂表锁读」/ fail-closed，二选一并写清。
- 文案统一用「**无显式 DML 阻塞锁**」而非「零锁」：长事务仍持有 ReadView、仍会阻塞相关 DDL、并累积 undo。

---

## 6. ALL 模式：快照—位点原子绑定（P0）

### 6.1 现状

现在只在全量任务开始前捕获**一个全局 binlog 位点**（`captureFullSyncStartPosition`，`task_service.go`，短暂全库 FTWRL + `SHOW MASTER STATUS`）。

改成按表、在不同时刻建快照后：每张表快照时间 `T_table` 都**晚于**全局位点 `T_global`，`[T_global, T_table]` 段事件对该表**已含在快照里**，从 `T_global` 重放会对该表重复应用。

### 6.2 方案

- **有 PK/UK 表**：保留全局 `T_global` 作为增量起点（保证不漏），`[T_global, T_table]` 的重复由**增量幂等**（`INSERT ... ON DUPLICATE KEY UPDATE` + `DELETE by key`）自愈。
- **无 PK/UK 表**：增量退化为 `INSERT IGNORE`，无冲突键，重复 INSERT 会真插重行。必须在建立该表快照的**同一把表锁窗口内**捕获该表的 binlog 高水位 `HWM_table`（`SHOW MASTER STATUS`），并对该表增量做高水位过滤（见 §7）。

### 6.3 持久化时序（防「全量完成后、增量启动前崩溃」）

- 每张表的 `HWM_table` 必须在 `FULL_COMPLETED` **之前**持久化到任务存档（随表进度或单独的表级 HWM 映射）。
- 崩溃恢复时若发现全量未完成，按现有「fresh full sync」语义整体重来（§9），此时旧 HWM 作废；若全量已完成，增量必须能读到完整的 `表 → HWM` 映射。

---

## 7. binlog 事务级位置协议（P0）（修正点 3）

### 7.1 现状问题（已核对代码）

- `pkg/binlog/subscriber.go`：`OnRow` 设置 `event.Position = h.subscriber.canal.SyncedPosition()`；canal 只在 **XID（事务提交）** 后推进 synced position（见同文件 `OnXID`）。
- `internal/sync/application/sync_service.go`：`OnEvent` 在每个 row event flush 后 `SavePosition(event.Position)`。

结论：现有 `BinlogEvent.Position` 更接近「**上一个已提交事务**的位置」，**不是当前 row event 的结束位置**。因此高水位过滤**不能**天真实现成 `event.Position < T_table`。

### 7.2 必须先定义的位置协议

1. 明确三个坐标各自含义与来源：**事务起点**、**row event 的 `End_log_pos`**、**事务提交位置（XID）**。
2. 高水位 `HWM_table` 在「所有 ReadView 已建立、表锁仍持有」时捕获（`SHOW MASTER STATUS` 得到的是「下一个事件将写入的位置」语义）。
3. **过滤按事务判断**：一个事务要么整体应用、要么整体跳过，绝不能拆成「部分行跳过、部分行应用」。
4. **被过滤的事件仍要推进全局 checkpoint**，否则重启后位点倒退。
5. 支持 **binlog 文件轮转**后的位置比较（`(file, pos)` 需按文件序 + 偏移比较，处理 `OnRotate`）。
6. `<` 还是 `<=` 由「`HWM_table` 与被比较坐标的实际语义」决定，**不能先写死**；需以 `End_log_pos` 与「起始处理位置」的区别为准。
7. **建议**顺带把 checkpoint 推进对齐到**事务提交边界**（配合按事务过滤），避免「事务中途保存 SyncedPosition」造成的语义漂移。

### 7.3 落地影响

- 需要在订阅层暴露「当前 row event 所属事务的提交位点 / end position」，而不仅是 `SyncedPosition()`。
- `checkpointMgr.SavePosition` 的调用时机建议改到事务提交（XID）边界。
- 该协议是无 PK/UK 表严格正确的前提，也是全量→增量接缝正确的前提，列为 P0。

---

## 8. 并发与资源控制

- **全局信号量**必须同时限制：
  - 活跃 snapshot group 数（并发表数）；
  - 源库快照事务/连接总数（并发表数 × 每表 reader 数）。
- **协调连接（持表锁者）也要计入信号量并预留连接**，避免「协调者占着锁、worker 却拿不到连接」的自死锁。
- undo 压力放大提示：相比串行 dumper，本方案会有「多个长 ReadView 同时存在」，源库写入频繁时 history list length 增长更快；信号量上限需据源库承受能力保守设置，并在文档/日志中给出观测指标（活跃快照事务数、最老事务存活时长）。

---

## 9. 进度与恢复语义（修正点：保持现有语义）

- 仓库**当前**语义即为「**所有全量同步均不可断点续传**」（非仅无主键表）；中断后按状态机需要 fresh full sync，并在允许 DROP 时重建目标（`executeFullSync`，`task_service.go`）。
- 本计划**保持该语义**，不写成「新增限制」。快照无法持久化，天然与「整表重来」一致：任何中断都从头重跑全量，旧快照/旧表级 HWM 作废。
- 索引重建触发时机：可维持现有「任务末尾统一重建」（`restorePendingIndexes`），只要保证「该表数据在其 snapshot group 内读完并落库」即可；是否改为「逐表读完即重建」列为可选优化，不作为本计划必需项。

---

## 10. 锁与超时策略

- 表级 `FLUSH TABLES t WITH READ LOCK` **先申请独占 MDL**，可能等待**所有已打开该表的事务**结束——「持锁时间短」不代表「取锁一定快」。
- 超时必须**双保险**：客户端 `context` 超时 + 锁连接上的 `SESSION lock_wait_timeout`（该变量明确适用于 `FLUSH TABLES ... WITH READ LOCK`）。
- **取锁/建快照失败不得「跳过并继续判定成功」**。策略二选一并写清：
  - 降级为**单 reader** 读该表（放弃单表内并行，但仍在单连接一致性快照下正确）；
  - 或 **fail-closed**：该表/该任务失败并给出明确错误。
  - 无 PK/UK 表在 ALL 模式下**不允许**降级为「无锁 + 全局位点」，因为会插重行。

---

## 11. 代码改造点（清单）

必须重构：

- `internal/task/application/service/full_load_v2.go`（`syncDatabasePairV2`，当前把全部表交给一个任务级引擎）→ 改为「每表一个 snapshot group」的调度：为每张表建立/对齐快照事务、绑定 reader、组内窃取。
- `internal/sync/fullload/engine.go`（`Engine.Run`，当前全局 `chunkCh` 跨表共享 + 单一 reader/writer 协程组）→ 读取侧改为「按表 snapshot group」；写入侧队列可保留全局。
- `internal/sync/fullload/reader.go`（`newChunkReader(*sql.DB)` / `readChunk` 用池连接）→ reader 必须绑定专属 `*sql.Conn`/`*sql.Tx`，不再从 `*sql.DB` 随机取连接。

可复用/保留：

- `internal/sync/fullload/chunk.go`（边界构造）→ 复用，可继续在快照外规划（§4）。
- 目标端 batch/writer 队列与 `internal/sync/fullload/writer.go`、`session.go` → 保留全局写入结构，不按表拆散。

ALL 模式相关：

- `captureFullSyncStartPosition` 及 `task_service.go` 全量编排 → 增加「表级 HWM 捕获与持久化」「表级过滤边界」。
- `pkg/binlog/subscriber.go`、`internal/sync/application/sync_service.go` → 实现 §7 的事务级位置协议、按事务过滤、checkpoint 对齐提交边界。

---

## 12. 测试用例清单

### 12.1 快照正确性（核心回归）

> **状态：已补可选真实 MySQL 集成（`//go:build integration`）**  
> - 单元近似（sqlmock）：`TestReadChunksThroughTableSnapshot_ExcludesPostSnapshotPKSwapVersion`  
> - 真实 MySQL：`internal/sync/fullload/integration_mysql_test.go`（需 `TEST_MYSQL_DSN`；默认 CI/`go test ./...` 不编译）  
>
> 运行示例（复用仓库 `docker-compose.yml` 的 `mysql-source`）：
> ```bash
> export TEST_MYSQL_DSN='root:root_password@tcp(127.0.0.1:3306)/?parseTime=true&multiStatements=true'
> go test -tags=integration -count=1 -timeout=5m ./internal/sync/fullload/ -run TestIntegration
> ```

- 构造「全量进行中，源库将某行从旧 PK 删除 + 新 PK 重插同一唯一键」，验证目标端**不出现重复唯一键**、阶段3 重建唯一索引成功。 *(真实 MySQL：`TestIntegration_EngineConcurrentPKSwap_NoDuplicateUKOnIndexRestore` + `TestIntegration_SnapshotExcludesPostSnapshotPKSwap`)*
- 构造「唯一列被更新为另一行已有值」，验证同上。 *(真实 MySQL：`TestIntegration_SnapshotExcludesUniqueColumnRewrite`)*
- 边界跨越场景：PK 在边界值附近被更新，验证同表同快照下**不漏不重**（§4 反例的正向验证）。 *(真实 MySQL：`TestIntegration_SnapshotBoundaryCrossingPKMove`)*
- **已有单元近似（sqlmock）：** 多 chunk 经同一 `tableSnapshot.conn` 读取时，只看到快照行版本、不混入「快照后新 PK / 同唯一键」行（见 `TestReadChunksThroughTableSnapshot_ExcludesPostSnapshotPKSwapVersion`）。

### 12.2 分档与降级

- FULL 中小表单连接快照路径；FULL 超大表多连接对齐快照路径。
- 取表锁超时 → 按配置降级单 reader / fail-closed，两条路径都要测。
- 非 InnoDB 表按既定策略被拒绝/降级。

### 12.3 ALL 模式接缝

- 有 PK/UK 表：`[T_global, T_table]` 重复窗口经幂等收敛，终态一致。
- 无 PK/UK 表：表级 HWM 过滤后**不插重行**；被过滤事务仍推进全局 checkpoint。
- binlog 文件轮转跨越 HWM 的位置比较正确。
- 「全量完成后、增量启动前」注入崩溃：恢复后按 fresh full sync 语义整体重来，无重复无漏放。

### 12.4 位置协议

- 事务级过滤：同一事务不被拆分为部分应用/部分跳过。
- checkpoint 在事务提交边界推进；重启不回退、不倒退。

### 12.5 资源与并发

- 信号量同时约束并发表数与快照事务/连接总数；协调连接计入且预留，注入「连接紧张」验证无自死锁。

### 12.6 验证命令

```bash
go test ./internal/sync/fullload/...
go test ./internal/task/...
go test ./internal/sync/...
go test ./pkg/binlog/...
go test ./...
go vet ./...

# 可选：真实 MySQL 并发集成（§12.1）
# TEST_MYSQL_DSN='root:root_password@tcp(127.0.0.1:3306)/?parseTime=true&multiStatements=true' \
#   go test -tags=integration -count=1 -timeout=5m ./internal/sync/fullload/ -run TestIntegration
```

---

## 13. 分期实施

- **P0（正确性，先做）**
  1. §7 binlog 事务级位置协议 + checkpoint 对齐提交边界。
  2. §6 ALL 模式快照—位点原子绑定（有 PK/UK 走幂等；无 PK/UK 走表锁 + 表级 HWM）。
  3. §3/§4 表级 snapshot group 的**单连接快照**路径（覆盖中小表，先把正确性闭环）。
- **P1（性能/并行）**
  4. 超大表「短暂 FTWRL 对齐多连接 ReadView」并行读。
  5. §8 全局信号量与资源观测指标。
  6. 取锁失败降级、非 InnoDB 策略等边界完善。
- **P2（可选优化）**
  7. 逐表读完即重建索引；Planner 迁入快照事务以改进负载均衡。

---

## 14. 验收标准

- 全量进行中源库发生「换主键重写 / 唯一列改值」时，目标端不再出现重复唯一键，阶段3 重建唯一索引不再报 1062。  
  **（§12.1：sqlmock 单元近似 + 可选真实 MySQL 集成 `TestIntegration_*`；需 `TEST_MYSQL_DSN` + `-tags=integration`。）**
- 表间并行与（大表）单表内多连接并行均保留。
- 无贯穿全任务的长写阻塞锁；显式写阻塞锁窗口限于每表快照初始化。
- ALL 模式全量→增量接缝：有 PK/UK 表最终一致；无 PK/UK 表严格不重不漏。
- 崩溃/中断遵循现有 fresh full sync 语义，无静默重复或漏放。
- 非 InnoDB / 取锁失败等边界有明确、可预期的行为，绝不「静默跳过判成功」。

---

## 15. 关键约束备忘（P0 复述）

> 「ALL 模式快照—位点原子绑定」与「binlog 事务级位置协议」列为 P0。否则即使 snapshot group 架构做对，全量到增量的接缝处仍会重复/漏放。
