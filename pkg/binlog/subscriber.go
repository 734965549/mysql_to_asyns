package binlog // 声明当前文件属于binlog包，用于MySQL binlog订阅功能

import ( // 导入外部包
	"context" // 导入context包，用于处理请求超时和取消
	"errors"
	"fmt"                      // 导入fmt包，用于格式化输入输出
	"mysql-to-sync/pkg/logger" // 导入log包，用于日志输出
	"regexp"
	"sync" // 导入sync包，用于并发控制
	"time" // 导入time包，用于时间处理

	"github.com/go-mysql-org/go-mysql/canal"       // 导入canal包，用于MySQL binlog订阅
	"github.com/go-mysql-org/go-mysql/mysql"       // 导入mysql包，用于MySQL相关功能
	"github.com/go-mysql-org/go-mysql/replication" // 导入replication包，用于MySQL复制协议
)

// BinlogEvent Binlog事件类型定义
type BinlogEventType string // 定义binlog事件类型为字符串

const ( // 常量定义
	EventTypeInsert BinlogEventType = "INSERT" // 插入事件类型
	EventTypeUpdate BinlogEventType = "UPDATE" // 更新事件类型
	EventTypeDelete BinlogEventType = "DELETE" // 删除事件类型

	// defaultMaxTxnBufferedRows 单个 binlog 事务在内存中可缓冲的 row map 软上限
	//（Rows + BeforeImage；UPDATE 会同时计入 before/after）。超限后溢写磁盘，避免硬失败毒事务。
	defaultMaxTxnBufferedRows = 100_000
	// defaultMaxTxnBufferedBytes 内存缓冲字节软上限（估算值）。少量大 BLOB 也会触发溢写。
	defaultMaxTxnBufferedBytes = 256 << 20 // 256 MiB
)

// BinlogEvent Binlog事件结构体
type BinlogEvent struct { // 定义binlog事件结构体
	Table          string                   `json:"table"`           // 表名
	Schema         string                   `json:"schema"`          // 数据库名
	EventType      BinlogEventType          `json:"event_type"`      // 事件类型
	Rows           []map[string]interface{} `json:"rows"`            // 行数据
	BeforeImage    []map[string]interface{} `json:"before_image"`    // 更新前的镜像数据，用于无主键表的WHERE条件
	Timestamp      time.Time                `json:"timestamp"`       // 事件时间戳
	Position       mysql.Position           `json:"position"`        // 事务提交位点，与 CommitPosition 一致
	EventEndPos    mysql.Position           `json:"event_end_pos"`   // 当前 row event 结束位置
	CommitPosition mysql.Position           `json:"commit_position"` // XID 后的下一事件起始位点
}

// EventHandler 事件处理器接口定义。
// OnEvent 只负责应用单条事件；位点推进必须在 OnTransactionCommit 中、整事务成功后完成。
type EventHandler interface {
	OnEvent(event *BinlogEvent) error
	// OnTransactionCommit 在同一事务内全部 OnEvent 成功后调用一次，传入 XID 提交位点。
	OnTransactionCommit(pos mysql.Position) error
}

// Subscriber Binlog订阅器结构体
type Subscriber struct { // 定义binlog订阅器结构体
	canal    *canal.Canal       // canal实例
	config   *SubscriberConfig  // 订阅器配置
	handlers []EventHandler     // 事件处理器列表
	position mysql.Position     // 当前binlog位置
	mu       sync.RWMutex       // 读写互斥锁，用于保证线程安全
	ctx      context.Context    // 上下文
	cancel   context.CancelFunc // 取消函数
	running  bool               // 运行状态标志

	// eventHandler 持有当前 canal handler，便于 Stop/退出时关闭并删除 spill 临时文件。
	eventHandler *binlogHandler
}

// SubscriberConfig 订阅器配置结构体
type SubscriberConfig struct { // 定义订阅器配置结构体
	Host      string   // MySQL主机地址
	Port      int      // MySQL端口
	Username  string   // MySQL用户名
	Password  string   // MySQL密码
	Database  string   // 数据库名（已废弃，请使用Databases）
	Databases []string // 要订阅的数据库列表
	Tables    []string // 要订阅的表列表（如果为空，订阅Databases中的所有表）
	ServerID  uint32   // 服务器ID，用于标识订阅者

	// MaxTxnBufferedRows 单事务内存缓冲 row map 软上限；0=默认 100000。超限溢写磁盘。
	MaxTxnBufferedRows int
	// MaxTxnBufferedBytes 单事务内存缓冲估算字节软上限；0=默认 256MiB。超限溢写磁盘。
	MaxTxnBufferedBytes int64
	// TxnSpillDir 事务溢写目录；空则使用系统临时目录下 mysql-to-sync-txn-spill。
	TxnSpillDir string
}

// NewSubscriber 创建Binlog订阅器函数
func NewSubscriber(config *SubscriberConfig) *Subscriber { // 创建新的binlog订阅器
	return &Subscriber{ // 返回订阅器实例
		config:   config,                  // 设置配置
		handlers: make([]EventHandler, 0), // 初始化处理器列表
	}
}

// AddHandler 添加事件处理器方法
func (s *Subscriber) AddHandler(handler EventHandler) { // 添加事件处理器
	s.handlers = append(s.handlers, handler) // 将处理器添加到列表
}

// Start 启动订阅方法
func (s *Subscriber) Start(ctx context.Context, position mysql.Position) error { // 启动binlog订阅
	s.mu.Lock()    // 获取锁
	if s.running { // 检查是否已在运行
		s.mu.Unlock()                                   // 释放锁
		return fmt.Errorf("subscriber already running") // 返回错误
	}
	s.ctx, s.cancel = context.WithCancel(ctx) // 创建可取消的上下文
	s.running = true                          // 设置运行状态
	s.mu.Unlock()                             // 释放锁

	// 配置canal
	cfg := canal.NewDefaultConfig()                               // 创建默认配置
	cfg.Addr = fmt.Sprintf("%s:%d", s.config.Host, s.config.Port) // 设置MySQL地址
	cfg.User = s.config.Username                                  // 设置用户名
	cfg.Password = s.config.Password                              // 设置密码
	cfg.Flavor = "mysql"                                          // 设置数据库类型
	cfg.ServerID = s.config.ServerID                              // 设置服务器ID
	cfg.Dump.ExecutionPath = "mysqldump"                          // 设置mysqldump路径

	// 确定要订阅的数据库列表
	dbs := s.config.Databases                     // 获取数据库列表
	if len(dbs) == 0 && s.config.Database != "" { // 如果数据库列表为空但Database不为空
		dbs = []string{s.config.Database} // 使用Database作为单元素列表
	}

	cfg.IncludeTableRegex = buildIncludeTableRegex(dbs, s.config.Tables)

	c, err := canal.NewCanal(cfg) // 创建canal实例
	if err != nil {               // 如果创建失败
		return fmt.Errorf("failed to create canal: %w", err) // 返回错误
	}
	s.canal = c // 设置canal实例

	startPos := position
	if startPos.Name == "" { // 如果未指定位置
		// 从最新位置开始
		startPos, err = s.canal.GetMasterPos() // 获取主库当前位置
		if err != nil {                        // 如果获取失败
			return fmt.Errorf("failed to get master position: %w", err) // 返回错误
		}
	}

	// 设置事件处理器（事务缓冲 + OnXID 派发；超内存软上限时溢写磁盘）
	cfgRows := 0
	var cfgBytes int64
	spillDir := ""
	if s.config != nil {
		cfgRows = s.config.MaxTxnBufferedRows
		cfgBytes = s.config.MaxTxnBufferedBytes
		spillDir = s.config.TxnSpillDir
	}
	handler := &binlogHandler{
		subscriber:  s,
		currentFile: startPos.Name,
		txnBuf:      newTxnEventBuffer(cfgRows, cfgBytes, spillDir),
	}
	s.mu.Lock()
	s.eventHandler = handler
	s.position = startPos
	s.mu.Unlock()
	s.canal.SetEventHandler(handler)
	defer s.closeTxnSpill()

	logger.Info("Binlog subscriber started from position: %v", startPos) // 输出启动日志
	err = s.canal.RunFrom(startPos)
	if err != nil && !errors.Is(err, context.Canceled) && (s.ctx == nil || s.ctx.Err() == nil) {
		return fmt.Errorf("binlog subscriber stopped unexpectedly: %w", err)
	}
	return nil // 返回nil表示成功
}

func buildIncludeTableRegex(databases, tables []string) []string {
	regexes := make([]string, 0)
	for _, database := range databases {
		quotedDatabase := regexp.QuoteMeta(database)
		if len(tables) == 0 {
			regexes = append(regexes, fmt.Sprintf("^%s\\..*$", quotedDatabase))
			continue
		}
		for _, table := range tables {
			regexes = append(regexes, fmt.Sprintf("^%s\\.%s$", quotedDatabase, regexp.QuoteMeta(table)))
		}
	}
	return regexes
}

// Stop 停止订阅方法
func (s *Subscriber) Stop() { // 停止binlog订阅
	s.mu.Lock()         // 获取锁
	defer s.mu.Unlock() // 延迟释放锁

	if !s.running { // 如果未在运行
		return // 直接返回
	}

	if s.cancel != nil { // 如果取消函数存在
		s.cancel() // 取消上下文
	}
	if s.canal != nil { // 如果canal实例存在
		s.canal.Close() // 关闭canal；RunFrom 返回后由 Start defer 清理事务缓冲
	}
	s.running = false                        // 设置运行状态为false
	logger.Info("Binlog subscriber stopped") // 输出停止日志
}

// closeTxnSpill 关闭并删除当前事务 spill 临时文件（Start 退出路径）。
func (s *Subscriber) closeTxnSpill() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeTxnSpillLocked()
}

func (s *Subscriber) closeTxnSpillLocked() {
	if s.eventHandler == nil {
		return
	}
	if s.eventHandler.txnBuf != nil {
		s.eventHandler.txnBuf.reset()
	}
}

// GetPosition 获取当前位置方法
func (s *Subscriber) GetPosition() mysql.Position { // 获取当前binlog位置
	s.mu.RLock()         // 获取读锁
	defer s.mu.RUnlock() // 延迟释放锁
	return s.position    // 返回位置
}

// dispatchEvent 分发事件到处理器方法
func (s *Subscriber) dispatchEvent(event *BinlogEvent) error { // 分发事件到所有处理器
	s.mu.Lock()                 // 获取锁
	s.position = event.Position // 更新位置
	s.mu.Unlock()               // 释放锁

	for _, handler := range s.handlers { // 遍历所有处理器
		if err := handler.OnEvent(event); err != nil { // 调用处理器处理事件
			logger.Error("Handler error: %v", err) // 输出错误日志
			return err                             // 返回错误
		}
	}
	return nil // 返回nil表示成功
}

// binlogHandler canal事件处理器结构体
type binlogHandler struct { // 定义canal事件处理器结构体
	canal.DummyEventHandler             // 嵌入DummyEventHandler
	subscriber              *Subscriber // 订阅器引用
	currentFile             string      // 当前 binlog 文件名，由 OnRotate 维护
	txnBuf                  *txnEventBuffer
}

func (h *binlogHandler) buildRowEvent(e *canal.RowsEvent) *BinlogEvent {
	var eventType BinlogEventType
	switch e.Action {
	case canal.InsertAction:
		eventType = EventTypeInsert
	case canal.UpdateAction:
		eventType = EventTypeUpdate
	case canal.DeleteAction:
		eventType = EventTypeDelete
	}

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

	eventEndPos := mysql.Position{Name: h.currentFile}
	if e.Header != nil {
		eventEndPos.Pos = e.Header.LogPos
	}

	return &BinlogEvent{
		Table:       e.Table.Name,
		Schema:      e.Table.Schema,
		EventType:   eventType,
		Rows:        rows,
		BeforeImage: beforeImage,
		Timestamp:   time.Now(),
		EventEndPos: eventEndPos,
	}
}

func (h *binlogHandler) buffer() *txnEventBuffer {
	if h.txnBuf == nil {
		h.txnBuf = newTxnEventBuffer(0, 0, "")
	}
	return h.txnBuf
}

func (h *binlogHandler) hasBuffered() bool {
	b := h.buffer()
	return b.hasBuffered()
}

func (h *binlogHandler) finishTransactionBoundary(commitPos mysql.Position) error {
	if h.hasBuffered() {
		err := h.buffer().flush(func(event *BinlogEvent) error {
			event.CommitPosition = commitPos
			event.Position = commitPos
			return h.subscriber.dispatchEvent(event)
		})
		if err != nil {
			return err
		}
	}
	return h.subscriber.dispatchTransactionCommit(commitPos)
}

func (h *binlogHandler) flushTransaction(commitPos mysql.Position) error {
	return h.finishTransactionBoundary(commitPos)
}

// dispatchTransactionCommit 通知所有 handler 事务已完整应用，可安全推进位点。
func (s *Subscriber) dispatchTransactionCommit(pos mysql.Position) error {
	s.mu.Lock()
	s.position = pos
	s.mu.Unlock()

	for _, handler := range s.handlers {
		if err := handler.OnTransactionCommit(pos); err != nil {
			logger.Error("Transaction commit handler error: %v", err)
			return err
		}
	}
	return nil
}

func (h *binlogHandler) OnRow(e *canal.RowsEvent) error {
	event := h.buildRowEvent(e)
	if err := h.buffer().append(event); err != nil {
		return fmt.Errorf("buffer binlog row event until XID: %w", err)
	}
	return nil
}

func (h *binlogHandler) String() string { // 获取处理器名称
	return "binlogHandler" // 返回处理器名称
}

// OnRotate 处理binlog文件切换方法
func (h *binlogHandler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	h.currentFile = string(rotateEvent.NextLogName)
	logger.Info("Binlog rotate: %s:%d", h.currentFile, rotateEvent.Position)
	return nil
}

// OnDDL 在表结构变更前刷新未提交的 row 事件；纯 DDL 位点由紧随其后的 OnPosSynced(force) 推进。
func (h *binlogHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	if h.hasBuffered() {
		return h.finishTransactionBoundary(nextPos)
	}
	return nil
}

// OnPosSynced 覆盖非 XID 提交边界：MyISAM 等引擎的 COMMIT Query、Rotate、canal 关闭。
func (h *binlogHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, _ mysql.GTIDSet, force bool) error {
	if h.hasBuffered() {
		return h.finishTransactionBoundary(pos)
	}
	if force {
		return h.subscriber.dispatchTransactionCommit(pos)
	}
	return nil
}

// OnXID 处理XID事件（事务提交）方法
func (h *binlogHandler) OnXID(header *replication.EventHeader, pos mysql.Position) error {
	return h.flushTransaction(pos)
}

// GetMasterPosition 获取主库当前位置方法
func (s *Subscriber) GetMasterPosition() (mysql.Position, error) { // 获取主库当前binlog位置
	if s.canal == nil { // 如果canal未初始化
		return mysql.Position{}, fmt.Errorf("canal not initialized") // 返回错误
	}
	return s.canal.GetMasterPos() // 返回主库位置
}
