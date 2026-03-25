# 配置说明

## 重要变更

**启动时不再需要配置数据库！**

从最新版本开始，程序启动时不再强制要求配置数据库连接。数据库配置可以通过以下两种方式提供：

### 1. 配置文件方式（开发环境推荐）

在 `etc/application.toml` 中配置默认的数据库连接信息，作为创建任务时的默认值。

### 2. API传参方式（生产环境推荐）

创建同步任务时，通过 API 参数动态指定源和目标数据库，实现更灵活的配置管理。

## 配置文件示例

### 最小配置（无需数据库）

```toml
[http]
  host = "127.0.0.1"
  port = 8081

[datasource]
  debug = true

[log]
  level = "debug"

[log.console]
  enable = true
  no_color = false

[log.file]
  enable = true

# 不配置数据库和Redis，启动时不会验证连接
# 数据库信息将在创建任务时通过API传入
```

### 完整配置（开发环境）

```toml
[http]
  host = "0.0.0.0"
  port = 8081

[datasource]
  # 源数据库配置（可选 - 如果创建任务时不指定，将使用此默认值）
  provider = "mysql"
  host = "192.168.1.100"
  port = 3306
  database = "source_db"
  username = "root"
  password = "password"
  debug = false

[target]
  # 目标数据库配置（可选 - 如果创建任务时不指定，将使用此默认值）
  host = "192.168.1.101"
  port = 3306
  database = "target_db"
  username = "root"
  password = "password"

[redis]
  # Redis配置（可选 - 用于保存Checkpoint位点，如果不配置则使用内存）
  host = "127.0.0.1"
  port = 6379
  password = ""
  db = 0

[log]
  level = "info"
  [log.console]
    enable = true
  [log.file]
    enable = true
```

## 配置优先级

当创建任务时，数据库配置的优先级为：

1. **任务级别配置**（最高优先级）：通过 API 创建任务时传入的 `source_db` 和 `target_db`
2. **配置文件默认值**：`etc/application.toml` 中配置的 `datasource` 和 `target`

## API 创建任务示例

### 使用任务级别数据库配置

```bash
curl -X POST http://localhost:8081/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "跨机房同步",
    "mode": "FULL",
    "sync_level": "TABLE",
    "source_schema": "production",
    "target_schema": "production_backup",
    "tables": ["users", "orders"],
    "source_db": {
      "host": "192.168.1.100",
      "port": 3306,
      "database": "production",
      "username": "reader",
      "password": "password"
    },
    "target_db": {
      "host": "192.168.2.100",
      "port": 3306,
      "database": "production_replica",
      "username": "writer",
      "password": "password"
    }
  }'
```

### 使用配置文件默认值

```bash
curl -X POST http://localhost:8081/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "本地测试同步",
    "mode": "FULL",
    "sync_level": "TABLE",
    "source_schema": "test_db",
    "target_schema": "test_db_backup",
    "tables": ["users"]
  }'
```

## 配置项说明

### HTTP 配置

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| host | string | 是 | HTTP服务监听地址 |
| port | int | 是 | HTTP服务监听端口 |

### Datasource 配置（可选）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| provider | string | 否 | 数据库类型，默认mysql |
| host | string | 否 | 源数据库地址 |
| port | int | 否 | 源数据库端口 |
| database | string | 否 | 源数据库名 |
| username | string | 否 | 源数据库用户名 |
| password | string | 否 | 源数据库密码 |
| debug | bool | 否 | 是否开启调试模式 |

### Target 配置（可选）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| host | string | 否 | 目标数据库地址 |
| port | int | 否 | 目标数据库端口 |
| database | string | 否 | 目标数据库名 |
| username | string | 否 | 目标数据库用户名 |
| password | string | 否 | 目标数据库密码 |

### Redis 配置（可选）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| host | string | 否 | Redis地址 |
| port | int | 否 | Redis端口 |
| password | string | 否 | Redis密码 |
| db | int | 否 | Redis数据库编号 |

## 环境变量支持

即将支持通过环境变量配置，便于容器化部署。

## 验证配置

启动服务时，可以通过日志查看配置是否正确加载：

```bash
2024/01/20 10:00:00 Warning: Failed to load config file: open etc/application.toml: no such file or directory, using empty config
2024/01/20 10:00:00 Using in-memory checkpoint manager
```

- 如果看到 "Warning: Failed to load config file"，说明配置文件不存在，将使用空配置
- 如果看到 "Using Redis checkpoint manager"，说明Redis配置成功
- 如果看到 "Using in-memory checkpoint manager"，说明未配置Redis，使用内存存储
