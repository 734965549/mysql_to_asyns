# MySQL-to-Async 数据同步工具



## 项目简介



MySQL-to-Async 是一个高性能的 MySQL 数据同步工具，支持全量同步、增量同步以及全量+增量组合同步模式。该工具采用领域驱动设计（DDD）架构，提供 RESTful API 接口和 Web 管理界面，支持任务的创建、启动、暂停、监控等全生命周期管理。



## 核心能力



- **全量数据迁移**：支持千万级数据高效迁移

- **实时增量同步**：基于 Binlog 的实时数据捕获，延迟小于1秒

- **断点续传**：Redis 持久化位点，服务重启后自动恢复

- **无主键表同步**：智能识别策略，全列匹配保障数据一致性

- **Web 管理界面**：可视化任务管理，实时监控同步进度

- **只读保护机制**：同步期间自动锁定目标库，防止数据冲突



## 性能指标



| 指标 | 数值 | 说明 |

|------|------|------|

| 全量同步速度 | 50,000+ rows/s | 批量插入，4核8G配置 |

| 增量同步延迟 | < 1s | Binlog实时订阅 |

| 支持数据量 | 1亿+ | 已验证生产环境 |

| 并发任务数 | 10+ | 视服务器配置而定 |



## 功能特性



### 核心功能



- **多种同步模式**：支持全量同步(FULL)、增量同步(INCREMENTAL)、全量+增量(ALL)

- **断点续传**：基于 Redis 的 checkpoint 机制，支持任务中断后继续执行

- **实时监控**：提供任务进度、状态、指标等实时监控

- **Binlog 订阅**：基于 MySQL Binlog 的实时增量数据捕获

- **批量处理**：支持批量数据读写，提高同步效率

- **无主键表支持**：针对无主键表提供全列匹配策略

- **审计日志**：记录所有同步操作的审计日志

- **只读保护**：同步期间自动设置目标库只读模式

- **密码加密存储**：任务配置中的数据库密码使用 AES-256-GCM 加密后持久化，防止明文泄露



### 高级特性



- **智能表识别**：自动识别主键、唯一键，选择最优同步策略

- **数据一致性保障**：支持INSERT、UPDATE、DELETE事件同步

- **自定义数据库配置**：每个任务可配置独立的源和目标数据库

- **任务级 Runtime 隔离**：每个任务独立维护 source/target DB、analyzer、readOnlyManager，支持并发启动

- **库级别同步**：支持整个数据库的批量同步

- **表级别同步**：支持指定表的精确同步



## 快速开始



### 环境要求



- Go 1.24+

- MySQL 5.7+ / 8.0+ (需开启Binlog)

- Redis 6.0+ (推荐)

- Node.js 18+ (前端开发，可选)



### MySQL配置要求



确保源MySQL已启用Binlog：



``sql

-- 检查Binlog是否启用

SHOW VARIABLES LIKE 'log_bin';



-- 检查Binlog格式（必须为ROW）

SHOW VARIABLES LIKE 'binlog_format';



-- 检查binlog_row_image（建议为FULL）

SHOW VARIABLES LIKE 'binlog_row_image';

``



MySQL配置文件 (my.cnf/my.ini)：



`ini

[mysqld]

log-bin=mysql-bin

binlog_format=ROW

binlog_row_image=FULL

server-id=1

expire_logs_days=7

`



### 数据库权限要求



``sql

GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'username'@'%';

GRANT SELECT ON *.* TO 'username'@'%';

# 若创建任务时启用 enable_consistent_snapshot=true（并发一致性快照）

# 建议额外授予 RELOAD 权限（用于快照建立阶段的 FTWRL）

# GRANT RELOAD ON *.* TO 'username'@'%';

FLUSH PRIVILEGES;

``



### 安装与配置



#### 1. 下载源码



``bash

git clone https://github.com/yourusername/mysql-to-async.git

cd mysql-to-async

``



#### 2. 配置文件



编辑 etc/application.toml：



``toml

[http]

  host = "0.0.0.0"

  port = 8081



[datasource]

  provider = "mysql"

  host = "192.168.1.100"

  port = 3306

  database = "source_db"

  username = "root"

  password = "password"

  debug = false



[target]

  host = "192.168.1.101"

  port = 3306

  database = "target_db"

  username = "root"

  password = "password"



[redis]

  host = "127.0.0.1"

  port = 6379

  password = ""

  db = 0



[security]
  # 任务密码加密密钥（AES-256），建议 32 字节；留空则不加密
  # 也可通过环境变量 MYSQL_TO_ASYNC_SECURITY_ENCRYPT_KEY 设置
  encrypt_key = "your-32-byte-secret-key-here!!!!"

[log]

  level = "info"

  [log.console]

    enable = true

  [log.file]

    enable = true

``



#### 3. 启动服务



**方式一：直接运行**



``bash

go run main.go

``



**方式二：编译后运行**



``bash

# Linux/macOS

go build -o mysql-to-async

./mysql-to-async



# Windows

go build -o mysql-to-async.exe

.\mysql-to-async.exe

``



#### 4. 启动Web界面（可选）



``bash

cd web

npm install

npm run dev

``



访问 http://localhost:5173 打开Web管理界面。



#### 5. Docker Compose 一键启动（前后端 + 依赖）



`Dockerfile` 已支持多阶段多目标构建：



- `backend`：Go 后端服务（监听 `8080`）；

- `frontend`：Nginx 托管前端 `npm run build` 产物，并反向代理 `/api` 到后端。



启动命令：



```bash

docker compose up -d --build

```



访问地址：



- 前端页面：`http://localhost`

- API 入口（通过 Nginx 代理）：`http://localhost/api`

- 后端容器内部端口：`app:8080`（compose 网络内）



## API 接口文档



服务启动后，默认监听端口 8080。API基础路径：http://localhost:8080/api



### 任务管理



#### 创建任务



```bash
POST /api/tasks
Content-Type: application/json
```

**请求示例：**

```json
{
  "name": "用户表同步",
  "mode": "ALL",
  "sync_level": "TABLE",
  "source_schema": "production",
  "target_schema": "production_backup",
  "tables": ["users", "orders"],
  "target_tables": ["users_archive", "orders_archive"],
  "batch_size": 1000,
  "worker_count": 4,
  "intra_table_worker_count": 8,
  "enable_limit_one": false,
  "optimize_index": true,
  "enable_read_only": true,
  "enable_consistent_snapshot": true,
  "enable_drop_table_before_ddl": true,
  "source_db": {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "production",
    "username": "root",
    "password": "root_password"
  },
  "target_db": {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "production_backup",
    "username": "root",
    "password": "root_password"
  }
}
```



**请求参数说明：**



| 参数 | 类型 | 必填 | 说明 |

|------|------|------|------|

| name | string | 是 | 任务名称 |

| mode | string | 是 | 同步模式：FULL/INCREMENTAL/ALL |

| sync_level | string | 是 | 同步级别：DATABASE(库级别)/TABLE(表级别) |

| source_schema | string | 是 | 源数据库名 |

| target_schema | string | 是 | 目标数据库名 |

| tables | array | 表级别必填 | 要同步的表列表 |

| batch_size | int | 否 | 批量大小，默认1000 |

| worker_count | int | 否 | 工作线程数，默认4 |

| enable_limit_one | bool | 否 | 无主键表LIMIT 1保护，默认false |

| enable_consistent_snapshot | bool | 否 | 全量阶段启用并发一致性快照（任务级参数，默认false） |
| enable_drop_table_before_ddl | bool | 否 | 同步 DDL 前先执行 `DROP TABLE IF EXISTS`，适用于库级别/表级别同步，默认false |



**`enable_consistent_snapshot` 说明：**

- 该参数属于任务请求体字段（`POST /api/tasks`、`PUT /api/tasks/:id`），不是 `application.toml` 全局配置。

- 开启后，全量阶段会让多个 worker 在同一时点快照上并发读取。

- 快照建立时会短暂执行 FTWRL（`FLUSH TABLES WITH READ LOCK`），建议在低峰时段执行。

**`enable_drop_table_before_ddl` 说明：**

- 该参数属于任务请求体字段（`POST /api/tasks`、`PUT /api/tasks/:id`）。

- 当目标库中已存在同名表时，如果开启该开关，任务会在执行 `CREATE TABLE ... LIKE ...` 或回放 `SHOW CREATE TABLE` 之前，先执行：`DROP TABLE IF EXISTS \`目标库\`.\`目标表\``。

- 该行为会在库级别同步和表级别同步中生效，适合“每次都以源表结构重建目标表”的场景。

- 注意：开启后会删除目标表及其数据，请确认目标库允许覆盖。



#### 更新任务



```bash
PUT /api/tasks/:id
Content-Type: application/json
```

**请求示例：**

```json
{
  "name": "用户表同步-调整版",
  "mode": "ALL",
  "sync_level": "TABLE",
  "source_schema": "production",
  "target_schema": "production_backup",
  "tables": ["users", "orders"],
  "target_tables": ["users_archive", "orders_archive"],
  "batch_size": 2000,
  "worker_count": 8,
  "intra_table_worker_count": 12,
  "enable_limit_one": false,
  "optimize_index": true,
  "enable_read_only": true,
  "enable_consistent_snapshot": true,
  "enable_drop_table_before_ddl": true,
  "source_db": {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "production",
    "username": "root",
    "password": "root_password"
  },
  "target_db": {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "production_backup",
    "username": "root",
    "password": "root_password"
  }
}
```

**说明：**

- 更新任务时，请把需要保留的字段一并传入，后端按请求体覆盖对应配置。
- `enable_drop_table_before_ddl=true` 时，任务在同步表结构前会先执行 `DROP TABLE IF EXISTS`。
- 如果只想切换该开关，可以只传该字段和任务标识相关的必要字段，但建议前端保存完整配置，避免遗漏。



#### 获取所有任务



```bash

GET /api/tasks

```



#### 获取任务详情



```bash

GET /api/tasks/:id

```



#### 启动任务



```bash
POST /api/tasks/:id/start
```

**请求体说明：**

- 不传请求体：立即启动任务。
- 传入 `scheduled_at`：设置单次定时启动。
- 传入 `repeat_count` 和 `repeat_interval_sec`：设置重复定时启动。

**请求示例 1：立即启动**

```bash
curl -X POST http://localhost:8080/api/tasks/任务ID/start
```

**请求示例 2：单次定时启动**

```json
{
  "scheduled_at": "2026-06-04T18:30:00+08:00"
}
```

**请求示例 3：重复定时启动**

```json
{
  "scheduled_at": "2026-06-04T18:30:00+08:00",
  "repeat_count": 3,
  "repeat_interval_sec": 60
}
```

**参数说明：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| scheduled_at | string | 定时启动时必填 | 首次启动时间，RFC3339 格式 |
| repeat_count | int | 否 | 重复启动总次数，包含首次执行；不传或传 0 表示仅执行一次 |
| repeat_interval_sec | int | 否 | 每次执行完成后到下一次启动的间隔秒数，默认 0 |

**说明：**

- `scheduled_at` 不能早于当前时间。
- `repeat_count` 必须大于等于 0。
- `repeat_interval_sec` 必须大于等于 0。
- 重复启动会在每次任务执行完成后，自动安排下一次启动，直到达到 `repeat_count`。

#### 暂停任务



```bash

POST /api/tasks/:id/pause

```



#### 删除任务



```bash

DELETE /api/tasks/:id

```



#### 获取任务指标



```bash

GET /api/tasks/:id/metrics

```



**响应示例：**



```json

{

  "processed_rows": 12345,

  "total_rows": 50000,

  "progress_percent": 24.69,

  "status": "RUNNING",

  "current_position": "users:15000",

  "binlog_file": "mysql-bin.000003",

  "binlog_pos": 4567,

  "lag": 0

}

```



### 元数据接口



#### 获取数据库列表



```bash

GET /api/metadata/databases

```



#### 获取表列表



```bash

GET /api/metadata/tables?schema=database_name

```



#### 获取表标识信息



```bash

GET /api/metadata/identity?schema=database_name&table=table_name

```



#### 刷新元数据



```bash

POST /api/metadata/refresh

```



### 配置接口



#### 获取默认配置



```bash

GET /api/config/default

```



## 使用示例



### 示例1：全量同步



```bash

curl -X POST http://localhost:8080/api/tasks \

  -H "Content-Type: application/json" \

  -d '{

    "name": "全量迁移订单数据",

    "mode": "FULL",

    "sync_level": "TABLE",

    "source_schema": "production",

    "target_schema": "backup",

    "tables": ["orders", "order_items"],

    "batch_size": 2000,

    "worker_count": 16,

    "intra_table_worker_count": 16,

    "enable_consistent_snapshot": true

  }'

```



### 示例2：增量同步



```bash

curl -X POST http://localhost:8080/api/tasks \

  -H "Content-Type: application/json" \

  -d '{

    "name": "实时同步用户表",

    "mode": "INCREMENTAL",

    "sync_level": "TABLE",

    "source_schema": "production",

    "target_schema": "production_slave",

    "tables": ["users", "user_profiles"],

    "batch_size": 1000

  }'

``



### 示例3：全量+增量组合



``bash

curl -X POST http://localhost:8080/api/tasks \

  -H "Content-Type: application/json" \

  -d '{

    "name": "完整数据同步",

    "mode": "ALL",

    "sync_level": "DATABASE",

    "source_schema": "production",

    "target_schema": "production_replica",

    "source_databases": ["production"],

    "target_database": "production_replica",

    "batch_size": 1000

  }'

``



## 架构设计



### 项目结构



``tree

mysql-to-async/

├── main.go                      # 程序入口

├── etc/

│   └── application.toml         # 配置文件

├── internal/                    # 内部模块（DDD架构）

│   ├── api/                     # API层

│   │   ├── handler/             # 请求处理器

│   │   └── router/              # 路由配置

│   ├── checkpoint/              # 位点管理

│   │   ├── redis_checkpoint.go  # Redis位点存储

│   │   └── memory_checkpoint.go # 内存位点存储

│   ├── config/                  # 配置管理

│   ├── metadata/                # 元数据管理

│   │   ├── domain/

│   │   │   ├── entity/          # 表实体

│   │   │   └── service/         # 元数据分析服务

│   │   └── infrastructure/      # Schema检测器

│   ├── sync/                    # 同步核心模块

│   │   ├── application/         # 增量同步服务

│   │   ├── domain/

│   │   │   └── strategy/        # 匹配策略

│   │   └── infrastructure/

│   │       ├── reader/          # 数据读取器

│   │       ├── writer/          # 数据写入器

│   │       └── readonly/        # 只读管理器

│   └── task/                    # 任务管理

│       ├── application/service/ # 任务服务

│       └── domain/entity/       # 任务实体

├── pkg/                         # 公共包

│   ├── binlog/                  # Binlog订阅器

│   └── crypto/                  # AES-GCM 密码加密工具

└── web/                         # 前端界面(Vue3)

``



### 核心组件



#### 1. 元数据分析器 (Metadata Analyzer)

- 自动识别表结构（主键、唯一键、索引）

- 分析表的标识策略（PK Strategy / UK Strategy / Full Columns Strategy）

- 提供数据库和表的元数据查询



#### 2. 数据读取器 (Data Reader)

- **CursorReader**: 无主键表的分页读取器

- **RangeShardingReader**: 有主键表的范围分片读取器

- 支持批量读取，提高读取效率



#### 3. 数据写入器 (Data Writer)

- **BatchWriter**: 批量写入器，支持INSERT/UPDATE/DELETE

- **BufferedWriter**: 带缓冲的写入器，定时刷新

- 自动构建SQL，支持ON DUPLICATE KEY UPDATE



#### 4. Binlog订阅器 (Binlog Subscriber)

- 基于go-mysql-org/go-mysql实现

- 实时捕获INSERT/UPDATE/DELETE事件

- 自动保存和恢复Binlog位点



#### 5. 位点管理器 (Checkpoint Manager)

- Redis持久化存储（推荐）

- 内存存储（备用）

- 支持断点续传



## 同步模式详解



### 1. 全量同步 (FULL)



**适用场景**：

- 一次性数据迁移

- 数据库初始化

- 数据备份



**工作流程**：

1. 分析源表结构

2. 在目标库创建表结构

3. 分批读取源表数据

4. 批量写入目标表

5. 更新进度



**性能优化**：

- 有主键表：使用ID范围分片，并行读取

- 无主键表：使用OFFSET分页，顺序读取

- 批量插入：使用批量INSERT语句



### 2. 增量同步 (INCREMENTAL)



**适用场景**：

- 实时数据同步

- 主从复制

- 数据分发



**前置要求**：

- MySQL开启Binlog (ROW格式)

- binlog_row_image=FULL（推荐）

- 用户具有REPLICATION权限



**工作流程**：

1. 连接到MySQL Master

2. 订阅Binlog事件

3. 解析INSERT/UPDATE/DELETE事件

4. 应用变更到目标库

5. 保存Binlog位点



**特性**：

- 实时捕获数据变更

- 延迟 < 1秒

- 自动重连机制

- 审计日志记录



### 3. 全量+增量 (ALL)



**适用场景**：

- 首次部署数据同步

- 数据迁移后需要持续同步



**工作流程**：

1. 执行全量同步

2. 全量完成后自动启动增量同步

3. 持续实时同步



**优势**：

- 一站式解决数据同步需求

- 无缝切换，数据不丢失



## 高级功能



### 1. 无主键表同步



对于没有主键或唯一键的表，系统自动采用**全列匹配策略**：



- INSERT: 直接插入

- UPDATE: WHERE条件使用所有列

- DELETE: WHERE条件使用所有列

- 性能影响：同步速度较慢，建议添加主键



### 2. 只读保护机制



同步期间，系统自动：

- 设置目标库普通用户为只读模式

- 防止其他应用修改目标数据

- 同步完成后自动恢复权限



### 3. 自定义数据库配置



每个任务可以配置独立的源和目标数据库：



``json

{

  "name": "跨机房同步",

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

}

``



### 4. 任务监控



实时监控任务状态和指标：



- **任务状态**: PENDING/RUNNING/PAUSED/COMPLETED/FAILED

- **进度百分比**: 已处理行数/总行数

- **当前位置**: 正在处理的表和位置

- **Binlog位点**: 当前同步的Binlog文件和位置

- **延迟监控**: 与主库的位置差



## 最佳实践



### 1. 生产环境部署建议



**服务器配置**：

- CPU: 4核+

- 内存: 8GB+

- 磁盘: SSD，足够存储日志

- 网络: 低延迟，高带宽



**数据库配置**：

- 使用独立的同步账号

- 合理设置批量大小（建议1000-5000）

- 监控数据库负载



**Redis配置**：

- 部署Redis高可用集群

- 设置合理的过期策略

- 监控内存使用



### 2. 性能优化建议



**全量同步**：

- 增大batch_size（2000-5000）

- 使用多线程并行同步

- 避开业务高峰期



**增量同步**：

- 使用Redis持久化位点

- 监控Binlog延迟

- 定期清理历史Binlog



**数据库优化**：

- 目标表禁用索引和约束

- 同步完成后再启用

- 使用批量INSERT



### 3. 数据一致性保障



**同步前检查**：

- 确认源表有主键或唯一键

- 检查binlog配置

- 验证网络连接



**同步中监控**：

- 监控任务状态

- 检查错误日志

- 验证数据样本



**同步后验证**：

- 对比源表和目标表行数

- 抽样验证数据一致性

- 检查审计日志



### 4. 容灾方案



**服务高可用**：

- 部署多个实例，使用负载均衡

- 定期备份Redis位点数据

- 监控服务健康状态



**数据恢复**：

- 保留Binlog足够时间

- 定期全量备份

- 制定数据恢复预案



## 常见问题



### Q1: 增量同步失败：binlog_row_image不是FULL



**解决方案**：



修改MySQL配置：



``ini

[mysqld]

binlog_row_image=FULL

``



重启MySQL服务。



### Q2: 增量同步延迟较大



**可能原因**：

- 网络延迟

- 目标数据库性能瓶颈

- 批量写入配置不合理



**解决方案**：

- 调整batch_size参数

- 优化目标数据库性能

- 检查网络连接

- 增加worker_count



### Q3: 服务重启后增量同步从头开始



**原因**： 未配置Redis，使用内存存储位点。



**解决方案**： 配置Redis并重启服务。



### Q4: 无主键表同步性能差



**原因**： 使用全列匹配，WHERE条件包含所有列。



**解决方案**：

- 为表添加主键或唯一键

- 调小batch_size

- 使用enable_limit_one保护



### Q5: 同步过程中目标库被其他应用修改



**解决方案**：

- 启用只读保护机制（默认开启）

- 同步期间锁定目标库

- 使用独立的目标库



### Q6: 如何查看同步失败的具体错误？



**解决方案**：



1. 查看任务状态：

``bash

GET /api/tasks/:id

``



2. 查看错误栈：

``json

{

  "context": {

    "error_stack": "具体错误信息"

  }

}

``



3. 查看审计日志（增量同步）



## 故障排查



### 日志查看



**应用日志**：

- 控制台输出

- 日志文件（如果配置）



**MySQL日志**：

- Error Log: 错误信息

- Slow Query Log: 慢查询



**Redis日志**：

- 连接错误

- 权限错误



### 常见错误码



| 错误信息 | 原因 | 解决方案 |

|---------|------|---------|

| connect refused | 数据库未启动 | 启动MySQL服务 |

| access denied | 权限不足 | 授予必要权限 |

| binlog not enabled | 未开启Binlog | 修改配置启用Binlog |

| table not found | 表不存在 | 检查表名和schema |

| connection timeout | 网络问题 | 检查网络连接 |



## 开发指南



### 本地开发



``bash

# 克隆项目

git clone https://github.com/yourusername/mysql-to-async.git

cd mysql-to-async



# 安装依赖

go mod download



# 运行服务

go run main.go



# 运行前端

cd web && npm run dev

``



### 运行测试



``bash

# 运行所有测试

go test ./...



# 运行测试并显示覆盖率

go test -cover ./...



# 生成覆盖率报告

go test -coverprofile=coverage.out ./...

go tool cover -html=coverage.out

``



### 代码规范



- 遵循 Go 标准代码规范

- 使用 gofmt 格式化代码

- 使用 go vet 检查代码

- 添加必要的注释



## 更新日志



### v1.2.0 (2026-04-02)

- 🔒 **密码加密存储**：任务中 source_db / target_db 的密码在持久化到数据库或文件时，使用 AES-256-GCM 加密，防止明文泄露

- 🔒 **安全配置**：新增 `[security]` 配置节，支持通过配置文件或环境变量 `MYSQL_TO_ASYNC_SECURITY_ENCRYPT_KEY` 设置加密密钥

- 🔒 **向后兼容**：未设置密钥时行为不变（明文存储）；已有明文旧数据在加载时自动兼容

- 🐛 修复前端更新配置时覆盖 `security` / `sync` 等后端专用字段的问题



### v1.1.1 (2026-03-30)

- ✨ **并发能力增强**: `TaskService` 改为任务级 runtime 隔离，移除单任务运行限制

- ✨ **可测性优化**: `StartTask` 增加测试注入点，支持并发启动场景稳定单测

- ✅ **新增测试**: 增加并发启动双任务隔离测试 `TestStartTask_ConcurrentRuntimeIsolation`



### v1.1.0 (2026-03-25)

- ✨ **性能优化**: 单表分片并行处理，大表同步速度提升2-4倍

- ✨ **UI优化**: 修复表级别同步选择问题，支持自定义数据库配置

- ✨ **搜索功能**: 添加数据库和表搜索框，支持实时过滤和模糊匹配

- ✨ **智能分片**: 自动根据表大小计算最佳分片数量和大小

- ✨ **兼容性增强**: 支持有主键、无主键、复合主键等多种表类型的并行处理

- ✨ **容错机制**: 分片失败自动回退到串行处理

- ✨ **跨库复制**: 支持同服务器和跨服务器的表结构自动复制

- ✨ **配置文档**: 新增详细的配置说明和性能调优指南

- 🐛 修复目标表不存在时继续写入的问题

- 🐛 修复跨库复制表结构的兼容性问题



### v1.0.0 (2026-03-20)

- ✨ 初始版本发布

- ✅ 支持全量同步、增量同步、全量+增量同步

- ✅ 集成Redis位点管理

- ✅ Web管理界面

- ✅ RESTful API

- ✅ 审计日志



## 贡献指南



欢迎提交 Issue 和 Pull Request！



1. Fork 本仓库

2. 创建特性分支 (git checkout -b feature/AmazingFeature)

3. 提交更改 (git commit -m 'Add some AmazingFeature')

4. 推送到分支 (git push origin feature/AmazingFeature)

5. 提交 Pull Request



## 技术支持



- **文档**: [INCREMENTAL_SYNC_GUIDE.md](INCREMENTAL_SYNC_GUIDE.md)

- **Issues**: [GitHub Issues](https://github.com/yourusername/mysql-to-async/issues)



## 许可证



本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件



---



<div align="center">



**⭐ 如果这个项目对你有帮助，请给一个Star支持一下！⭐**



</div>

