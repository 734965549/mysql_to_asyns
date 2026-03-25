package binlog

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// BinlogEvent Binlog事件类型
type BinlogEventType string

const (
	EventTypeInsert BinlogEventType = "INSERT"
	EventTypeUpdate BinlogEventType = "UPDATE"
	EventTypeDelete BinlogEventType = "DELETE"
)

// BinlogEvent Binlog事件
type BinlogEvent struct {
	Table       string                   `json:"table"`
	Schema      string                   `json:"schema"`
	EventType   BinlogEventType          `json:"event_type"`
	Rows        []map[string]interface{} `json:"rows"`
	BeforeImage []map[string]interface{} `json:"before_image"` // 用于无主键表的WHERE条件
	Timestamp   time.Time                `json:"timestamp"`
	Position    mysql.Position           `json:"position"`
}

// EventHandler 事件处理器接口
type EventHandler interface {
	OnEvent(event *BinlogEvent) error
}

// Subscriber Binlog订阅器
type Subscriber struct {
	canal    *canal.Canal
	config   *SubscriberConfig
	handlers []EventHandler
	position mysql.Position
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	running  bool
}

// SubscriberConfig 订阅器配置
type SubscriberConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
	Tables   []string
	ServerID uint32
}

// NewSubscriber 创建Binlog订阅器
func NewSubscriber(config *SubscriberConfig) *Subscriber {
	return &Subscriber{
		config:   config,
		handlers: make([]EventHandler, 0),
	}
}

// AddHandler 添加事件处理器
func (s *Subscriber) AddHandler(handler EventHandler) {
	s.handlers = append(s.handlers, handler)
}

// Start 启动订阅
func (s *Subscriber) Start(ctx context.Context, position mysql.Position) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("subscriber already running")
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.position = position
	s.running = true
	s.mu.Unlock()

	// 配置canal
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	cfg.User = s.config.Username
	cfg.Password = s.config.Password
	cfg.Flavor = "mysql"
	cfg.ServerID = s.config.ServerID
	cfg.Dump.ExecutionPath = "mysqldump"

	// 只订阅指定的数据库和表
	if len(s.config.Tables) > 0 {
		cfg.IncludeTableRegex = make([]string, len(s.config.Tables))
		for i, table := range s.config.Tables {
			cfg.IncludeTableRegex[i] = fmt.Sprintf("%s\\.%s", s.config.Database, table)
		}
	} else {
		cfg.IncludeTableRegex = []string{fmt.Sprintf("%s\\..*", s.config.Database)}
	}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("failed to create canal: %w", err)
	}
	s.canal = c

	// 设置事件处理器
	s.canal.SetEventHandler(&binlogHandler{subscriber: s})

	// 从指定位置开始同步
	if position.Name != "" {
		s.canal.RunFrom(position)
	} else {
		// 从最新位置开始
		pos, err := s.canal.GetMasterPos()
		if err != nil {
			return fmt.Errorf("failed to get master position: %w", err)
		}
		go s.canal.RunFrom(pos)
	}

	log.Printf("Binlog subscriber started from position: %v", position)
	return nil
}

// Stop 停止订阅
func (s *Subscriber) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}

	if s.cancel != nil {
		s.cancel()
	}
	if s.canal != nil {
		s.canal.Close()
	}
	s.running = false
	log.Println("Binlog subscriber stopped")
}

// GetPosition 获取当前位置
func (s *Subscriber) GetPosition() mysql.Position {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.position
}

// dispatchEvent 分发事件到处理器
func (s *Subscriber) dispatchEvent(event *BinlogEvent) error {
	s.mu.Lock()
	s.position = event.Position
	s.mu.Unlock()

	for _, handler := range s.handlers {
		if err := handler.OnEvent(event); err != nil {
			log.Printf("Handler error: %v", err)
			return err
		}
	}
	return nil
}

// binlogHandler canal事件处理器
type binlogHandler struct {
	canal.DummyEventHandler
	subscriber *Subscriber
}

func (h *binlogHandler) OnRow(e *canal.RowsEvent) error {
	var eventType BinlogEventType
	switch e.Action {
	case canal.InsertAction:
		eventType = EventTypeInsert
	case canal.UpdateAction:
		eventType = EventTypeUpdate
	case canal.DeleteAction:
		eventType = EventTypeDelete
	}

	// 转换行数据
	rows := make([]map[string]interface{}, 0, len(e.Rows))
	beforeImage := make([]map[string]interface{}, 0)

	for i := 0; i < len(e.Rows); i++ {
		rowMap := make(map[string]interface{})
		for j, col := range e.Table.Columns {
			if j < len(e.Rows[i]) {
				rowMap[col.Name] = e.Rows[i][j]
			}
		}

		if e.Action == canal.UpdateAction {
			if i%2 == 0 {
				beforeImage = append(beforeImage, rowMap)
			} else {
				rows = append(rows, rowMap)
			}
		} else if e.Action == canal.DeleteAction {
			beforeImage = append(beforeImage, rowMap)
		} else {
			rows = append(rows, rowMap)
		}
	}

	event := &BinlogEvent{
		Table:       e.Table.Name,
		Schema:      e.Table.Schema,
		EventType:   eventType,
		Rows:        rows,
		BeforeImage: beforeImage,
		Timestamp:   time.Now(),
		Position:    h.subscriber.canal.SyncedPosition(),
	}

	return h.subscriber.dispatchEvent(event)
}

func (h *binlogHandler) String() string {
	return "binlogHandler"
}

// OnRotate 处理binlog文件切换
func (h *binlogHandler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	log.Printf("Binlog rotate: %s:%d", string(rotateEvent.NextLogName), rotateEvent.Position)
	return nil
}

// OnXID 处理XID事件（事务提交）
func (h *binlogHandler) OnXID(header *replication.EventHeader, pos mysql.Position) error {
	h.subscriber.mu.Lock()
	h.subscriber.position = pos
	h.subscriber.mu.Unlock()
	return nil
}

// GetMasterPosition 获取主库当前位置
func (s *Subscriber) GetMasterPosition() (mysql.Position, error) {
	if s.canal == nil {
		return mysql.Position{}, fmt.Errorf("canal not initialized")
	}
	return s.canal.GetMasterPos()
}
