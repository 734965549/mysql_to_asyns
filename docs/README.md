# 📚 MySQL-to-Async 文档中心

欢迎来到 MySQL-to-Async 项目文档中心！

---

## 📖 文档导航

### 🚀 快速开始

- [项目主文档](../README.md) - 项目介绍、快速开始、功能特性

### 📋 优化报告

- [P0优化报告](optimization/P0_OPTIMIZATION_REPORT.md) - 核心功能优化（任务持久化、审计日志、并发控制）
- [P1优化报告](optimization/P1_OPTIMIZATION_REPORT.md) - 重要功能优化（优雅关闭、健康检查、错误处理）
- [P2优化报告](optimization/P2_OPTIMIZATION_REPORT.md) - 功能增强（配置验证、Docker、监控、测试）
- [最终总结](optimization/FINAL_SUMMARY.md) - 项目优化完整总结
- [优化清单](optimization/OPTIMIZATION_CHECKLIST.md) - 优化项检查清单

### 📘 使用指南

- [配置说明](CONFIGURATION.md) - 配置文件详细说明和性能调优指南
- [增量同步指南](guides/INCREMENTAL_SYNC_GUIDE.md) - 增量同步功能使用说明
- [Web UI使用指南](guides/WEB_UI_GUIDE.md) - Web管理界面使用说明和最佳实践

### 🎨 设计文档

- [设计文档](design/shejiwendang.md) - 系统架构和设计说明

### 🧪 测试文档

- [单元测试文档](testing/UNIT_TEST.md) - 单元测试说明和覆盖率

---

## 📂 项目结构

```
mysql-to-async/
├── README.md                    # 项目主文档
├── docs/                        # 文档目录
│   ├── optimization/            # 优化报告
│   ├── guides/                  # 使用指南
│   ├── design/                  # 设计文档
│   └── testing/                 # 测试文档
├── internal/                    # 内部代码
│   ├── api/                     # API层
│   ├── audit/                   # 审计日志
│   ├── checkpoint/              # 位点管理
│   ├── config/                  # 配置管理
│   ├── metadata/                # 元数据
│   ├── metrics/                 # 监控指标
│   ├── sync/                    # 同步逻辑
│   └── task/                    # 任务管理
├── web/                         # 前端代码
├── docker/                      # Docker配置
├── etc/                         # 配置文件
└── plans/                       # 计划文档
```

---

## 🔍 按主题查找

### 功能特性
- **任务管理**: [P0优化报告](optimization/P0_OPTIMIZATION_REPORT.md#1-任务存储持久化-)
- **审计日志**: [P0优化报告](optimization/P0_OPTIMIZATION_REPORT.md#2-审计日志实现-)
- **增量同步**: [增量同步指南](guides/INCREMENTAL_SYNC_GUIDE.md)
- **监控指标**: [P2优化报告](optimization/P2_OPTIMIZATION_REPORT.md#4-prometheus监控指标-)

### 部署运维
- **Docker部署**: [P2优化报告](optimization/P2_OPTIMIZATION_REPORT.md#3-docker支持-)
- **健康检查**: [P1优化报告](optimization/P1_OPTIMIZATION_REPORT.md#2-健康检查接口-)
- **优雅关闭**: [P1优化报告](optimization/P1_OPTIMIZATION_REPORT.md#1-优雅关闭机制-)

### 开发测试
- **单元测试**: [测试文档](testing/UNIT_TEST.md)
- **配置验证**: [P2优化报告](optimization/P2_OPTIMIZATION_REPORT.md#1-配置验证-)
- **错误处理**: [P1优化报告](optimization/P1_OPTIMIZATION_REPORT.md#3-前端错误处理优化-)

---

## 📊 项目状态

| 模块 | 完成度 | 文档 |
|------|--------|------|
| 核心功能 | ✅ 100% | [P0优化报告](optimization/P0_OPTIMIZATION_REPORT.md) |
| 重要功能 | ✅ 100% | [P1优化报告](optimization/P1_OPTIMIZATION_REPORT.md) |
| 功能增强 | ✅ 100% | [P2优化报告](optimization/P2_OPTIMIZATION_REPORT.md) |
| 测试覆盖 | ⚠️ 40-50% | [测试文档](testing/UNIT_TEST.md) |

---

## 🎯 快速链接

### 新手入门
1. 阅读 [项目主文档](../README.md)
2. 查看 [设计文档](design/shejiwendang.md)
3. 运行 [Docker部署](optimization/P2_OPTIMIZATION_REPORT.md#3-docker支持-)

### 开发者
1. 查看 [优化清单](optimization/OPTIMIZATION_CHECKLIST.md)
2. 阅读 [单元测试文档](testing/UNIT_TEST.md)
3. 参考 [P2优化报告](optimization/P2_OPTIMIZATION_REPORT.md)

### 运维人员
1. 部署 [Docker环境](optimization/P2_OPTIMIZATION_REPORT.md#3-docker支持-)
2. 配置 [健康检查](optimization/P1_OPTIMIZATION_REPORT.md#2-健康检查接口-)
3. 集成 [Prometheus监控](optimization/P2_OPTIMIZATION_REPORT.md#4-prometheus监控指标-)

---

## 📝 文档维护

### 更新日志
- 2026-03-25: 添加数据库和表搜索框功能，支持实时过滤和模糊匹配
- 2026-03-25: 添加配置说明文档，包含性能调优指南
- 2026-03-25: 优化单表分片并行处理，性能提升2-4倍
- 2026-03-25: 修复表级别同步UI和目标表自动创建问题
- 2026-03-20: 创建文档索引，整理文档结构
- 2026-03-20: 完成P0/P1/P2优化报告
- 2026-03-20: 添加单元测试文档

### 贡献指南
如需更新文档，请遵循以下规则：
- 主文档 `README.md` 保持在项目根目录
- 优化报告放在 `docs/optimization/`
- 使用指南放在 `docs/guides/`
- 设计文档放在 `docs/design/`
- 测试文档放在 `docs/testing/`

---

## 💬 反馈与支持

如有问题或建议，请查看：
- [优化清单](optimization/OPTIMIZATION_CHECKLIST.md) - 查看待办事项
- [最终总结](optimization/FINAL_SUMMARY.md) - 查看项目完整状态

---

**文档版本**: v1.0  
**最后更新**: 2026-03-20  
**维护者**: MySQL-to-Async Team
