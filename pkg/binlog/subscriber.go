package binlog // 声明当前文件属于binlog包，用于MySQL binlog订阅功能

import ( // 导入外部包
	"context" // 导入context包，用于处理请求超时和取消
	"fmt" // 导入fmt包，用于格式化输入输出
	"mysql-to-async/pkg/logger" // 导入log包，用于日志输出
	"sync" // 导入sync包，用于并发控制
	"time" // 导入time包，用于时间处理

	"github.com/go-mysql-org/go-mysql/canal" // 导入canal包，用于MySQL binlog订阅
	"github.com/go-mysql-org/go-mysql/mysql" // 导入mysql包，用于MySQL相关功能
	"github.com/go-mysql-org/go-mysql/replication" // 导入replication包，用于MySQL复制协议
)

// BinlogEvent Binlog事件类型定义
type BinlogEventType string // 定义binlog事件类型为字符串

const ( // 常量定义
	EventTypeInsert BinlogEventType = "INSERT" // 插入事件类型
	EventTypeUpdate BinlogEventType = "UPDATE" // 更新事件类型
	EventTypeDelete BinlogEventType = "DELETE" // 删除事件类型
)

// BinlogEvent Binlog事件结构体
type BinlogEvent struct { // 定义binlog事件结构体
	Table       string                   `json:"table"` // 表名
	Schema      string                   `json:"schema"` // 数据库名
	EventType   BinlogEventType          `json:"event_type"` // 事件类型
	Rows        []map[string]interface{} `json:"rows"` // 行数据
	BeforeImage []map[string]interface{} `json:"before_image"` // 更新前的镜像数据，用于无主键表的WHERE条件
	Timestamp   time.Time                `json:"timestamp"` // 事件时间戳
	Position    mysql.Position           `json:"position"` // binlog位置
}

// EventHandler 事件处理器接口定义
type EventHandler interface { // 定义事件处理器接口
	OnEvent(event *BinlogEvent) error // 处理事件方法
}

// Subscriber Binlog订阅器结构体
type Subscriber struct { // 定义binlog订阅器结构体
	canal    *canal.Canal // canal实例
	config   *SubscriberConfig // 订阅器配置
	handlers []EventHandler // 事件处理器列表
	position mysql.Position // 当前binlog位置
	mu       sync.RWMutex // 读写互斥锁，用于保证线程安全
	ctx      context.Context // 上下文
	cancel   context.CancelFunc // 取消函数
	running  bool // 运行状态标志
}

// SubscriberConfig 订阅器配置结构体
type SubscriberConfig struct { // 定义订阅器配置结构体
	Host      string // MySQL主机地址
	Port      int // MySQL端口
	Username  string // MySQL用户名
	Password  string // MySQL密码
	Database  string // 数据库名（已废弃，请使用Databases）
	Databases []string // 要订阅的数据库列表
	Tables    []string // 要订阅的表列表（如果为空，订阅Databases中的所有表）
	ServerID  uint32 // 服务器ID，用于标识订阅者
}

// NewSubscriber 创建Binlog订阅器函数
func NewSubscriber(config *SubscriberConfig) *Subscriber { // 创建新的binlog订阅器
	return &Subscriber{ // 返回订阅器实例
		config:   config, // 设置配置
		handlers: make([]EventHandler, 0), // 初始化处理器列表
	}
}

// AddHandler 添加事件处理器方法
func (s *Subscriber) AddHandler(handler EventHandler) { // 添加事件处理器
	s.handlers = append(s.handlers, handler) // 将处理器添加到列表
}

// Start 启动订阅方法
func (s *Subscriber) Start(ctx context.Context, position mysql.Position) error { // 启动binlog订阅
	s.mu.Lock() // 获取锁
	if s.running { // 检查是否已在运行
		s.mu.Unlock() // 释放锁
		return fmt.Errorf("subscriber already running") // 返回错误
	}
	s.ctx, s.cancel = context.WithCancel(ctx) // 创建可取消的上下文
	s.position = position // 设置位置
	s.running = true // 设置运行状态
	s.mu.Unlock() // 释放锁

	// 配置canal
	cfg := canal.NewDefaultConfig() // 创建默认配置
	cfg.Addr = fmt.Sprintf("%s:%d", s.config.Host, s.config.Port) // 设置MySQL地址
	cfg.User = s.config.Username // 设置用户名
	cfg.Password = s.config.Password // 设置密码
	cfg.Flavor = "mysql" // 设置数据库类型
	cfg.ServerID = s.config.ServerID // 设置服务器ID
	cfg.Dump.ExecutionPath = "mysqldump" // 设置mysqldump路径

	// 确定要订阅的数据库列表
	dbs := s.config.Databases // 获取数据库列表
	if len(dbs) == 0 && s.config.Database != "" { // 如果数据库列表为空但Database不为空
		dbs = []string{s.config.Database} // 使用Database作为单元素列表
	}

	// 构建 IncludeTableRegex
	var regexes []string // 定义正则表达式列表
	if len(s.config.Tables) > 0 { // 如果指定了表
		// 指定了表 (通常用于单库模式)
		// 如果指定了 Databases，则假设 Tables 属于这些库 (或者只取第一个库)
		// 为了兼容性，如果 Database 字段存在，使用它
		db := s.config.Database // 获取数据库名
		if db == "" && len(dbs) > 0 { // 如果Database为空但有数据库列表
			db = dbs[0] // 使用第一个数据库
		}
		if db != "" { // 如果数据库名不为空
			for _, table := range s.config.Tables { // 遍历表列表
				regexes = append(regexes, fmt.Sprintf("%s\\.%s", db, table)) // 构建正则表达式
			}
		}
	} else { // 如果未指定表
		// 未指定表，订阅库下所有表
		for _, db := range dbs { // 遍历数据库列表
			regexes = append(regexes, fmt.Sprintf("%s\\..*", db)) // 构建匹配所有表的正则表达式
		}
	}

	cfg.IncludeTableRegex = regexes // 设置表匹配正则表达式

	c, err := canal.NewCanal(cfg) // 创建canal实例
	if err != nil { // 如果创建失败
		return fmt.Errorf("failed to create canal: %w", err) // 返回错误
	}
	s.canal = c // 设置canal实例

	// 设置事件处理器
	s.canal.SetEventHandler(&binlogHandler{subscriber: s}) // 设置binlog事件处理器

	// 从指定位置开始同步
	if position.Name != "" { // 如果指定了binlog文件名
		s.canal.RunFrom(position) // 从指定位置开始同步
	} else { // 如果未指定位置
		// 从最新位置开始
		pos, err := s.canal.GetMasterPos() // 获取主库当前位置
		if err != nil { // 如果获取失败
			return fmt.Errorf("failed to get master position: %w", err) // 返回错误
		}
		go s.canal.RunFrom(pos) // 在goroutine中从最新位置开始同步
	}

	logger.Info("Binlog subscriber started from position: %v", position) // 输出启动日志
	return nil // 返回nil表示成功
}

// Stop 停止订阅方法
func (s *Subscriber) Stop() { // 停止binlog订阅
	s.mu.Lock() // 获取锁
	defer s.mu.Unlock() // 延迟释放锁

	if !s.running { // 如果未在运行
		return // 直接返回
	}

	if s.cancel != nil { // 如果取消函数存在
		s.cancel() // 取消上下文
	}
	if s.canal != nil { // 如果canal实例存在
		s.canal.Close() // 关闭canal
	}
	s.running = false // 设置运行状态为false
	logger.Info("Binlog subscriber stopped") // 输出停止日志
}

// GetPosition 获取当前位置方法
func (s *Subscriber) GetPosition() mysql.Position { // 获取当前binlog位置
	s.mu.RLock() // 获取读锁
	defer s.mu.RUnlock() // 延迟释放锁
	return s.position // 返回位置
}

// dispatchEvent 分发事件到处理器方法
func (s *Subscriber) dispatchEvent(event *BinlogEvent) error { // 分发事件到所有处理器
	s.mu.Lock() // 获取锁
	s.position = event.Position // 更新位置
	s.mu.Unlock() // 释放锁

	for _, handler := range s.handlers { // 遍历所有处理器
		if err := handler.OnEvent(event); err != nil { // 调用处理器处理事件
			logger.Error("Handler error: %v", err) // 输出错误日志
			return err // 返回错误
		}
	}
	return nil // 返回nil表示成功
}

// binlogHandler canal事件处理器结构体
type binlogHandler struct { // 定义canal事件处理器结构体
	canal.DummyEventHandler // 嵌入DummyEventHandler
	subscriber *Subscriber // 订阅器引用
}

func (h *binlogHandler) OnRow(e *canal.RowsEvent) error { // 处理行事件
	var eventType BinlogEventType // 定义事件类型变量
	switch e.Action { // 根据动作类型确定事件类型
	case canal.InsertAction: // 如果是插入动作
		eventType = EventTypeInsert // 设置为插入事件
	case canal.UpdateAction: // 如果是更新动作
		eventType = EventTypeUpdate // 设置为更新事件
	case canal.DeleteAction: // 如果是删除动作
		eventType = EventTypeDelete // 设置为删除事件
	}

	// 转换行数据
	rows := make([]map[string]interface{}, 0, len(e.Rows)) // 创建行数据列表
	beforeImage := make([]map[string]interface{}, 0) // 创建更新前镜像列表

	for i := 0; i < len(e.Rows); i++ { // 遍历所有行
		rowMap := make(map[string]interface{}) // 创建行映射表
		for j, col := range e.Table.Columns { // 遍历所有列
			if j < len(e.Rows[i]) { // 检查列索引是否有效
				rowMap[col.Name] = e.Rows[i][j] // 将列值存入映射表
			}
		}

		if e.Action == canal.UpdateAction { // 如果是更新事件
			if i%2 == 0 { // 如果是偶数索引
				beforeImage = append(beforeImage, rowMap) // 添加到更新前镜像
			} else { // 如果是奇数索引
				rows = append(rows, rowMap) // 添加到行数据
			}
		} else if e.Action == canal.DeleteAction { // 如果是删除事件
			beforeImage = append(beforeImage, rowMap) // 添加到更新前镜像
		} else { // 其他情况（插入事件）
			rows = append(rows, rowMap) // 添加到行数据
		}
	}

	event := &BinlogEvent{ // 创建binlog事件
		Table:       e.Table.Name, // 设置表名
		Schema:      e.Table.Schema, // 设置数据库名
		EventType:   eventType, // 设置事件类型
		Rows:        rows, // 设置行数据
		BeforeImage: beforeImage, // 设置更新前镜像
		Timestamp:   time.Now(), // 设置时间戳
		Position:    h.subscriber.canal.SyncedPosition(), // 设置binlog位置
	}

	return h.subscriber.dispatchEvent(event) // 分发事件到处理器
}

func (h *binlogHandler) String() string { // 获取处理器名称
	return "binlogHandler" // 返回处理器名称
}

// OnRotate 处理binlog文件切换方法
func (h *binlogHandler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error { // 处理binlog文件切换事件
	logger.Info("Binlog rotate: %s:%d", string(rotateEvent.NextLogName), rotateEvent.Position) // 输出binlog切换日志
	return nil // 返回nil
}

// OnXID 处理XID事件（事务提交）方法
func (h *binlogHandler) OnXID(header *replication.EventHeader, pos mysql.Position) error { // 处理XID事件
	h.subscriber.mu.Lock() // 获取锁
	h.subscriber.position = pos // 更新位置
	h.subscriber.mu.Unlock() // 释放锁
	return nil // 返回nil
}

// GetMasterPosition 获取主库当前位置方法
func (s *Subscriber) GetMasterPosition() (mysql.Position, error) { // 获取主库当前binlog位置
	if s.canal == nil { // 如果canal未初始化
		return mysql.Position{}, fmt.Errorf("canal not initialized") // 返回错误
	}
	return s.canal.GetMasterPos() // 返回主库位置
}
