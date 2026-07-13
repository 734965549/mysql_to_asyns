# 增量同步功能使用指南

## 功能概述

增量同步功能通过订阅MySQL Binlog实现实时数据同步，支持以下特性：

- ✅ 实时捕获INSERT、UPDATE、DELETE事件
- ✅ 自动保存和恢复Binlog位点
- ✅ 支持Redis和内存两种位点存储方式
- ✅ 审计日志记录
- ✅ 延迟监控
- ✅ 优雅启动和停止

## 前置要求

### 1. MySQL配置要求

确保源MySQL已启用Binlog，并且配置正确：

```sql
-- 检查Binlog是否启用
SHOW VARIABLES LIKE 'log_bin';

-- 检查Binlog格式（必须为ROW）
SHOW VARIABLES LIKE 'binlog_format';

-- 检查binlog_row_image（建议为FULL）
SHOW VARIABLES LIKE 'binlog_row_image';
```

**MySQL配置文件 (my.cnf/my.ini)：**

```ini
[mysqld]
# 启用Binlog
log-bin=mysql-bin

# Binlog格式必须为ROW
binlog_format=ROW

# 建议设置为FULL，以支持无主键表的增量同步
binlog_row_image=FULL

# Server ID（必须唯一）
server-id=1

# Binlog过期时间（天）
expire_logs_days=7
```

### 2. 数据库权限要求

用于增量同步的数据库用户需要以下权限：

```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'username'@'%';
GRANT SELECT ON *.* TO 'username'@'%';
FLUSH PRIVILEGES;
```

### 3. Redis配置（推荐）

用于持久化保存Binlog位点，防止服务重启后丢失进度。

配置文件 `etc/application.toml`:

```toml
[redis]
  host = "127.0.0.1"
  port = 6379
  password = ""
  db = 0
```

如果未配置Redis，系统将使用内存存储位点（服务重启后位点会丢失）。

## 使用方式

### 1. 创建增量同步任务

通过API创建任务：

```bash
POST /api/tasks
Content-Type: application/json

{
  "name": "增量同步示例",
  "mode": "INCREMENTAL",
  "sync_level": "TABLE",
  "source_schema": "test",
  "target_schema": "test_target",
  "tables": ["users", "orders"],
  "batch_size": 1000
}
```

### 2. 启动任务

```bash
POST /api/tasks/{task_id}/start
```

### 3. 查看任务状态和指标

```bash
GET /api/tasks/{task_id}/metrics
```

响应示例：

```json
{
  "processed_rows": 1234,
  "total_rows": 0,
  "estimated_total_rows": 50000,
  "progress_percent": 0,
  "tables_completed": 0,
  "tables_total": 2,
  "status": "RUNNING",
  "current_position": "mysql-bin.000003:4567",
  "binlog_file": "mysql-bin.000003",
  "binlog_pos": 4567,
  "lag": 0
}
```

### 4. 暂停任务

```bash
POST /api/tasks/{task_id}/pause
```

暂停任务会：
- 停止Binlog订阅
- 保留当前的Binlog位点
- 下次启动时从暂停位置继续

### 5. 全量+增量组合模式

先执行全量同步，完成后自动启动增量同步：

```json
{
  "name": "全量+增量同步",
  "mode": "ALL",
  "sync_level": "TABLE",
  "source_schema": "test",
  "target_schema": "test_target",
  "tables": ["users", "orders"],
  "batch_size": 1000
}
```

## 同步模式对比

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| FULL | 全量同步 | 一次性数据迁移、初始化数据 |
| INCREMENTAL | 增量同步 | 实时数据同步、持续同步 |
| ALL | 全量+增量 | 首次同步+持续同步 |

## 技术细节

### 1. Binlog订阅机制

- 每个任务使用唯一的ServerID（基于任务ID生成）
- 支持从指定Binlog位置开始同步
- 自动重连机制

### 2. 位点管理

位点信息包含：
- Binlog文件名
- Binlog位置
- 最后更新时间

存储方式：
- **Redis**（推荐）：持久化存储，支持服务重启
- **内存**：临时存储，服务重启后从最新位置开始

### 3. 事件处理

支持的事件类型：
- **INSERT**: 直接插入目标表
- **UPDATE**: 根据主键/唯一键更新
- **DELETE**: 根据主键/唯一键删除

### 4. 无主键表处理

对于无主键表：
- 使用全列匹配进行UPDATE和DELETE
- 要求`binlog_row_image=FULL`
- 同步性能可能受影响

> **警告**：无主键表在 ALL 模式下无法保证数据一致性。"短锁位点 + 非快照全量 + binlog 回放"的收敛保证仅适用于有可靠非空 PK/UK 的表。无主键表在全量扫描与 binlog 回放同时写入同一行时，`INSERT IGNORE` 因无冲突键无法去重，会产生重复行。建议给表补充主键或唯一键。

## 监控与告警

### 1. 延迟监控

通过`/api/tasks/{task_id}/metrics`接口获取延迟信息：

- `lag`: 当前延迟字节数
- `binlog_file`: 当前Binlog文件
- `binlog_pos`: 当前Binlog位置

### 2. 审计日志

所有同步失败的操作都会记录审计日志，包含：
- 任务ID
- 表名
- 事件类型
- 错误信息
- 时间戳

## 常见问题

### Q1: 增量同步失败：binlog_row_image不是FULL

**解决方案：**

修改MySQL配置文件：
```ini
binlog_row_image=FULL
```

重启MySQL服务。

### Q2: 增量同步延迟较大

**可能原因：**
- 网络延迟
- 目标数据库性能问题
- 批量写入配置不合理

**解决方案：**
- 调整`batch_size`参数
- 优化目标数据库性能
- 检查网络连接

### Q3: 服务重启后增量同步从头开始

**原因：** 未配置Redis，使用内存存储位点。

**解决方案：** 配置Redis并重启服务。

### Q4: 增量同步无法捕获某些表的变化

**可能原因：**
- 表未被包含在任务的`tables`列表中
- Binlog订阅的表过滤规则问题

**解决方案：**
- 检查任务配置
- 确认表名正确

## 最佳实践

1. **生产环境建议配置Redis**，确保位点持久化
2. **监控延迟指标**，及时发现同步问题
3. **合理设置batch_size**，平衡性能和延迟
4. **定期检查审计日志**，了解同步失败情况
5. **使用ALL模式**完成首次同步+持续同步
6. **确保MySQL配置正确**，特别是binlog_format和binlog_row_image

## 示例：完整的增量同步流程

```bash
# 1. 创建增量同步任务
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "订单表增量同步",
    "mode": "INCREMENTAL",
    "sync_level": "TABLE",
    "source_schema": "production",
    "target_schema": "production_backup",
    "tables": ["orders", "order_items"],
    "batch_size": 1000
  }'

# 响应：{"config": {"id": "task_abc123", ...}}

# 2. 启动任务
curl -X POST http://localhost:8080/api/tasks/task_abc123/start

# 3. 查看状态
curl http://localhost:8080/api/tasks/task_abc123/metrics

# 4. 暂停任务
curl -X POST http://localhost:8080/api/tasks/task_abc123/pause

# 5. 查看所有任务
curl http://localhost:8080/api/tasks
```

## 相关API接口

| 接口 | 方法 | 说明 |
|------|------|------|
| /api/tasks | POST | 创建任务 |
| /api/tasks | GET | 获取所有任务 |
| /api/tasks/:id | GET | 获取任务详情 |
| /api/tasks/:id/start | POST | 启动任务 |
| /api/tasks/:id/pause | POST | 暂停任务 |
| /api/tasks/:id/metrics | GET | 获取任务指标 |
| /api/tasks/:id | DELETE | 删除任务 |
