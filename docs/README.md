# MySQL-to-Async 文档中心

本文档中心用于快速定位项目行为、设计边界、运维配置和测试资料。

## 推荐阅读顺序

1. [项目 README](../README.md)：产品能力、快速启动、主要 API。
2. [领域模块边界与调用关系说明](design/DOMAIN_MODULE_BOUNDARIES.md)：各模块职责、入参/出参、调用链和状态流转。
3. [设计文档](design/shejiwendang.md)：DDD 分层、全量/增量同步设计、无主键表策略。
4. [配置说明](CONFIGURATION.md)：TOML、环境变量、存储、Redis、安全和连接池。
5. [全量续传指南](guides/FULL_SYNC_RESUME_GUIDE.md)：全量暂停/失败后的表级和行级续传。
6. [增量同步指南](guides/INCREMENTAL_SYNC_GUIDE.md)：binlog checkpoint 和 ROW 模式要求。
7. [单元测试说明](testing/UNIT_TEST.md)：测试范围、运行方式和覆盖重点。

## 文档导航

### 架构与设计

- [领域模块边界与调用关系说明](design/DOMAIN_MODULE_BOUNDARIES.md)
- [设计文档](design/shejiwendang.md)

### 使用与运维

- [配置说明](CONFIGURATION.md)
- [启动时无数据库配置](STARTUP_WITHOUT_DB.md)
- [Web UI 使用指南](guides/WEB_UI_GUIDE.md)
- [Kubernetes 部署说明](../k8s/README.md)

### 同步机制

- [全量续传指南](guides/FULL_SYNC_RESUME_GUIDE.md)
- [增量同步指南](guides/INCREMENTAL_SYNC_GUIDE.md)
- [任务存储升级 SQL](sql/sys_sync_tasks_upgrade.sql)

### 测试与计划

- [单元测试说明](testing/UNIT_TEST.md)
- [单元测试覆盖计划](../plans/unit-test-coverage-plan.md)
- [增量多目标设计计划](../plans/incremental-multi-sink-design.md)

### 历史优化记录

- [P0 优化报告](optimization/P0_OPTIMIZATION_REPORT.md)
- [P1 优化报告](optimization/P1_OPTIMIZATION_REPORT.md)
- [P2 优化报告](optimization/P2_OPTIMIZATION_REPORT.md)
- [优化清单](optimization/OPTIMIZATION_CHECKLIST.md)
- [最终总结](optimization/FINAL_SUMMARY.md)

## 代码目录速览

```text
mysql-to-sync/
├── main.go                         # 程序入口、配置加载、路由启动、优雅关闭
├── internal/api                    # HTTP handler 和 router
├── internal/task                   # 任务聚合、生命周期、调度、运行时隔离、存储
├── internal/metadata               # 表结构探测、PK/UK/no-PK 策略识别
├── internal/sync                   # 全量/增量同步、reader、writer、readonly、match strategy
├── internal/checkpoint             # 增量 binlog 位点，Redis 或内存
├── internal/config                 # TOML/env 配置、校验、连接池参数
├── internal/audit                  # 审计日志
├── internal/metrics                # Prometheus 指标
├── pkg/binlog                      # go-mysql canal 封装
├── pkg/crypto                      # AES-GCM 密码加密
├── web                             # Vue 3 管理端
├── docs                            # 文档
├── plans                           # 计划和设计草案
├── docker / k8s / etc              # 部署和配置示例
```

## 文档维护规则

- 行为变化必须同步更新对应指南、配置示例和 API 说明。
- 任务生命周期、`SyncPhase`、全量续传、checkpoint、无主键表策略变化时，优先更新 [领域模块边界与调用关系说明](design/DOMAIN_MODULE_BOUNDARIES.md)。
- 配置字段变化时，同时更新 `etc/application.toml.example`、`docs/CONFIGURATION.md` 和部署模板。
- Web API 字段变化时，同时检查 `web/` 调用和展示文案。

最后更新：2026-06-19
