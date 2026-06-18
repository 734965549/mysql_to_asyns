# 启动时无需数据库配置

## 变更说明

从最新版本开始，**程序启动时不再需要配置数据库连接**。数据库配置可以在创建同步任务时动态指定。

## 变更原因

之前的架构要求：
- ❌ 启动时必须配置 `datasource` 和 `target`
- ❌ 启动时必须能够连接数据库
- ❌ 如果数据库不可用，服务无法启动

现在的架构：
- ✅ 启动时无需任何数据库配置
- ✅ 可以在创建任务时动态指定数据库
- ✅ 每个任务可以使用不同的数据库
- ✅ 更适合云原生和容器化部署

## 使用方式

### 方式一：最小配置启动（推荐）

#### 1. 创建最小配置文件

```toml
# etc/application.toml
[http]
  host = "127.0.0.1"
  port = 8081

[datasource]
  debug = true

[log]
  level = "debug"

[log.console]
  enable = true

[log.file]
  enable = true
```

#### 2. 启动服务

```bash
./mysql-to-sync
```

#### 3. 创建任务时指定数据库

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "用户数据同步",
    "mode": "FULL",
    "sync_level": "TABLE",
    "source_schema": "production",
    "target_schema": "backup",
    "tables": ["users", "orders"],
    "source_db": {
      "host": "192.168.1.100",
      "port": 3306,
      "database": "production",
      "username": "root",
      "password": "password"
    },
    "target_db": {
      "host": "192.168.1.101",
      "port": 3306,
      "database": "backup",
      "username": "root",
      "password": "password"
    }
  }'
```

### 方式二：配置文件提供默认值（开发环境）

#### 1. 创建完整配置文件

```toml
# etc/application.toml
[http]
  host = "0.0.0.0"
  port = 8081

[datasource]
  host = "192.168.1.100"
  port = 3306
  database = "production"
  username = "root"
  password = "password"
  debug = true

[target]
  host = "192.168.1.101"
  port = 3306
  database = "backup"
  username = "root"
  password = "password"

[redis]
  host = "127.0.0.1"
  port = 6379

[log]
  level = "debug"
```

#### 2. 启动服务

```bash
./mysql-to-sync
```

#### 3. 创建任务（使用默认数据库配置）

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "本地测试同步",
    "mode": "FULL",
    "sync_level": "TABLE",
    "source_schema": "test_db",
    "target_schema": "test_backup",
    "tables": ["users"]
  }'
```

## 配置优先级

创建任务时，数据库配置的优先级：

```
任务级别的 source_db/target_db > 配置文件的 datasource/target
```

### 示例

假设配置文件中：
```toml
[datasource]
  host = "192.168.1.100"
  database = "default_db"
```

创建任务时指定：
```json
{
  "source_schema": "my_db",
  "source_db": {
    "host": "192.168.2.100",
    "database": "my_db"
  }
}
```

**实际使用**：
- `host`: `192.168.2.100`（来自任务配置）
- `database`: `my_db`（来自任务配置，优先级更高）

## 元数据接口说明

当没有配置数据库或未启动任何任务时，以下接口会返回错误提示：

- `GET /api/metadata/databases` - 获取数据库列表
- `GET /api/metadata/tables` - 获取表列表
- `GET /api/metadata/identity` - 获取表标识信息

**错误响应示例**：

```json
{
  "error": "Database not connected. Please create a task with database configuration first, or configure the datasource in config file."
}
```

**解决方案**：

1. 方式一：在配置文件中配置 `datasource`
2. 方式二：先创建并启动一个任务，系统会自动连接数据库

## 数据库连接生命周期

### 启动阶段
- 不创建数据库连接
- 不验证数据库配置

### 首次启动任务
- 动态创建源数据库连接
- 动态创建目标数据库连接
- 初始化元数据分析器
- 连接会被复用于后续任务

### 关闭服务
- 保存所有任务状态
- 关闭所有数据库连接

## 适用场景

### ✅ 推荐场景

1. **多租户环境**
   - 不同任务使用不同的数据库
   - 无需为每个数据库启动独立实例

2. **容器化部署**
   - 无需挂载配置文件
   - 通过环境变量或 API 传递配置

3. **云原生架构**
   - 配置与代码分离
   - 动态配置管理

4. **开发测试**
   - 快速切换数据库
   - 无需频繁修改配置文件

### ⚠️ 注意事项

1. **元数据查询**
   - 首次使用需要先创建任务建立连接
   - 或者配置默认数据库

2. **连接管理**
   - 所有任务共享同一个数据库连接
   - 如需独立连接，需要启动多个实例

3. **位点存储**
   - 建议配置 Redis 实现持久化
   - 否则使用内存存储，重启后丢失

## 迁移指南

### 从旧版本迁移

#### 如果有配置文件

无需修改，旧配置文件仍然有效：

```toml
[datasource]
  host = "192.168.1.100"
  port = 3306
  # ...其他配置
```

系统会将其作为默认值使用。

#### 如果没有配置文件

创建最小配置即可：

```toml
[http]
  host = "127.0.0.1"
  port = 8081

[log]
  level = "info"
  [log.console]
    enable = true
```

## API 兼容性

所有 API 保持向后兼容：

- ✅ 创建任务时不指定 `source_db`/`target_db`，使用配置文件默认值
- ✅ 创建任务时指定 `source_db`/`target_db`，覆盖配置文件
- ✅ 旧的任务配置格式仍然有效

## 常见问题

### Q1: 启动后无法获取数据库列表？

**原因**: 没有配置默认数据库，且未启动任何任务。

**解决方案**:
1. 在配置文件中配置 `datasource`
2. 或者先创建一个任务，启动后会建立连接

### Q2: 多个任务可以使用不同的数据库吗？

**可以**。每个任务可以独立配置 `source_db` 和 `target_db`。

### Q3: 配置文件中的数据库配置还有用吗？

**有用**。作为创建任务时的默认值，简化任务配置。

### Q4: 任务之间的数据库连接会互相影响吗？

**不会**。虽然共享连接，但任务之间是隔离的，不会互相影响。

## 技术实现

相关代码变更：

1. **main.go**
   - 移除启动时的数据库连接逻辑
   - 使用 `NewTaskService(cfg)` 替代 `NewTaskServiceWithDBAndConfig(...)`

2. **task_service.go**
   - 新增 `initDatabaseConnections()` 方法
   - 在首次启动任务时动态创建连接

3. **task_handler.go**
   - 元数据接口增加 `nil` 检查
   - 返回友好的错误提示

4. **config/validator.go**
   - 新增 `ValidateConfig()` 方法（不验证数据库）
   - 保留 `ValidateAll()` 方法（完整验证）

## 总结

这次重构使系统更加灵活：

- 🚀 快速启动：无需预先配置数据库
- 🔧 灵活配置：支持任务级别的数据库配置
- ☁️ 云原生友好：适合容器化和动态配置场景
- 🔄 向后兼容：旧配置和 API 仍然有效
