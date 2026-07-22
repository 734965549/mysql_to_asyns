package binlog

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"mysql-to-sync/pkg/logger"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func init() {
	// gob 编码 map[string]interface{} 时，interface{} 内的具体类型必须注册。
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register([]byte{})
	gob.Register(time.Time{})
	gob.Register(int8(0))
	gob.Register(int16(0))
	gob.Register(int32(0))
	gob.Register(int64(0))
	gob.Register(uint8(0))
	gob.Register(uint16(0))
	gob.Register(uint32(0))
	gob.Register(uint64(0))
	gob.Register(float32(0))
	gob.Register(float64(0))
	gob.Register("")
	gob.Register(false)
	gob.Register(mysql.Position{})
}

// txnEventBuffer 在 XID 前缓冲事务事件：先占内存，超限后溢写到临时文件，避免硬失败毒事务。
type txnEventBuffer struct {
	mu sync.Mutex

	events []*BinlogEvent
	rows   int
	bytes  int64

	maxRows  int
	maxBytes int64
	spillDir string

	spillFile  *os.File
	spillEnc   *gob.Encoder
	spillCount int
}

func newTxnEventBuffer(maxRows int, maxBytes int64, spillDir string) *txnEventBuffer {
	if maxRows <= 0 {
		maxRows = defaultMaxTxnBufferedRows
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxTxnBufferedBytes
	}
	return &txnEventBuffer{
		events:   make([]*BinlogEvent, 0),
		maxRows:  maxRows,
		maxBytes: maxBytes,
		spillDir: spillDir,
	}
}

func (b *txnEventBuffer) append(event *BinlogEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if event == nil {
		return nil
	}
	n := eventBufferedRowCount(event)
	sz := estimateEventBytes(event)

	// 内存已满时先溢写，再接收新事件，避免超大事务无法推进。
	if len(b.events) > 0 && (b.rows+n > b.maxRows || b.bytes+sz > b.maxBytes) {
		if err := b.spillToDisk(); err != nil {
			return err
		}
	}

	b.events = append(b.events, event)
	b.rows += n
	b.bytes += sz

	// 单事件本身已超内存上限：立刻落盘，只在 canal 已解码的瞬时峰值之外尽量释放堆占用。
	if b.rows > b.maxRows || b.bytes > b.maxBytes {
		if err := b.spillToDisk(); err != nil {
			return err
		}
	}
	return nil
}

func (b *txnEventBuffer) spillToDisk() error {
	if len(b.events) == 0 {
		return nil
	}
	if err := b.ensureSpillFile(); err != nil {
		return err
	}
	for _, event := range b.events {
		if err := b.spillEnc.Encode(event); err != nil {
			return fmt.Errorf("spill binlog transaction event to disk: %w", err)
		}
		b.spillCount++
	}
	logger.Warn("binlog transaction buffer spilled %d events to disk (rows=%d bytes≈%d file=%s); large transactions will be replayed from spill on XID",
		len(b.events), b.rows, b.bytes, b.spillFile.Name())
	b.events = b.events[:0]
	b.rows = 0
	b.bytes = 0
	return nil
}

func (b *txnEventBuffer) ensureSpillFile() error {
	if b.spillFile != nil {
		return nil
	}
	dir := b.spillDir
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "mysql-to-sync-txn-spill")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create txn spill dir %s: %w", dir, err)
	}
	f, err := os.CreateTemp(dir, "txn-*.gob")
	if err != nil {
		return fmt.Errorf("create txn spill file: %w", err)
	}
	b.spillFile = f
	b.spillEnc = gob.NewEncoder(f)
	return nil
}

// flush 按 spill→内存顺序回调每个事件；全部成功后清空缓冲。
// 若 apply 中途失败，已应用的 spill 事件不会回滚到内存，调用方应停止订阅并由 checkpoint 语义重放整事务。
func (b *txnEventBuffer) flush(apply func(*BinlogEvent) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.spillFile != nil {
		if _, err := b.spillFile.Seek(0, 0); err != nil {
			return fmt.Errorf("seek txn spill file: %w", err)
		}
		dec := gob.NewDecoder(b.spillFile)
		for i := 0; i < b.spillCount; i++ {
			var event BinlogEvent
			if err := dec.Decode(&event); err != nil {
				return fmt.Errorf("decode spilled binlog event[%d]: %w", i, err)
			}
			ev := event
			if err := apply(&ev); err != nil {
				return err
			}
		}
	}
	for i, event := range b.events {
		if err := apply(event); err != nil {
			b.events = b.events[i:]
			b.rows = countTxnBufferedRows(b.events)
			b.bytes = estimateEventsBytes(b.events)
			return err
		}
	}
	b.resetLocked()
	return nil
}

func (b *txnEventBuffer) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resetLocked()
}

func (b *txnEventBuffer) resetLocked() {
	b.events = b.events[:0]
	b.rows = 0
	b.bytes = 0
	b.spillCount = 0
	b.spillEnc = nil
	if b.spillFile != nil {
		name := b.spillFile.Name()
		_ = b.spillFile.Close()
		_ = os.Remove(name)
		b.spillFile = nil
	}
}

func (b *txnEventBuffer) bufferedRows() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rows
}

func (b *txnEventBuffer) inMemoryLen() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}

func (b *txnEventBuffer) hasBuffered() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events) > 0 || b.spillCount > 0
}

func eventBufferedRowCount(event *BinlogEvent) int {
	if event == nil {
		return 0
	}
	return len(event.Rows) + len(event.BeforeImage)
}

func countTxnBufferedRows(events []*BinlogEvent) int {
	total := 0
	for _, event := range events {
		total += eventBufferedRowCount(event)
	}
	return total
}

func estimateEventsBytes(events []*BinlogEvent) int64 {
	var total int64
	for _, event := range events {
		total += estimateEventBytes(event)
	}
	return total
}

func estimateEventBytes(event *BinlogEvent) int64 {
	if event == nil {
		return 0
	}
	var total int64
	total += int64(len(event.Schema) + len(event.Table) + len(event.EventType))
	for _, row := range event.Rows {
		total += estimateRowBytes(row)
	}
	for _, row := range event.BeforeImage {
		total += estimateRowBytes(row)
	}
	return total
}

func estimateRowBytes(row map[string]interface{}) int64 {
	var total int64
	for k, v := range row {
		total += int64(len(k))
		total += estimateValueBytes(v)
	}
	return total
}

func estimateValueBytes(v interface{}) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case []byte:
		return int64(len(x))
	case string:
		return int64(len(x))
	case bool:
		return 1
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return 8
	case time.Time:
		return 24
	default:
		// 未知类型按粗粒度估算，偏保守以更早触发溢写。
		return 64
	}
}
