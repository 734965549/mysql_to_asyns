package fullload

import (
	"context"
	"sync"
	"time"
)

const tableSoftLimitCapBytes = 32 * 1024 * 1024 // 32 MiB

// batchQueue 是按字节数限流的有界 RowBatch 队列，支持多表公平调度。
//
// 源读取速度高于目标写入速度时提供短期缓冲；缓冲字节数达到上限后 Put 阻塞，
// 形成对读取 worker 的背压。有多表等待时每表 soft limit = min(global/2, 32MiB)。
type batchQueue struct {
	mu       sync.Mutex
	notFull  *sync.Cond
	notEmpty *sync.Cond

	globalMax int64
	softCap   int64

	tables   map[string]*tableSubQueue
	pollKeys []string
	pollIdx  int

	curBytes int64
	closed   bool
	stats    *Stats
}

type tableSubQueue struct {
	key      string
	items    []*RowBatch
	curBytes int64
}

func newBatchQueue(maxBytes int64, stats *Stats) *batchQueue {
	if maxBytes < 1 {
		maxBytes = defaultBufferBytes
	}
	q := &batchQueue{
		globalMax: maxBytes,
		softCap:   tableSoftLimitCapBytes,
		tables:    make(map[string]*tableSubQueue),
		stats:     stats,
	}
	q.notFull = sync.NewCond(&q.mu)
	q.notEmpty = sync.NewCond(&q.mu)
	if stats != nil {
		stats.setQueue(0, maxBytes)
	}
	return q
}

func (q *batchQueue) subQueue(key string) *tableSubQueue {
	if sq, ok := q.tables[key]; ok {
		return sq
	}
	sq := &tableSubQueue{key: key}
	q.tables[key] = sq
	q.ensurePollKeyLocked(key)
	return sq
}

func (q *batchQueue) ensurePollKeyLocked(key string) {
	for _, k := range q.pollKeys {
		if k == key {
			return
		}
	}
	q.pollKeys = append(q.pollKeys, key)
}

func (q *batchQueue) waitingTableCount() int {
	n := 0
	for _, sq := range q.tables {
		if len(sq.items) > 0 {
			n++
		}
	}
	return n
}

func (q *batchQueue) effectiveTableLimit() int64 {
	if q.waitingTableCount() <= 1 {
		return q.globalMax
	}
	soft := q.globalMax / 2
	if soft > q.softCap {
		soft = q.softCap
	}
	if soft < 1 {
		soft = 1
	}
	return soft
}

func (q *batchQueue) refreshStatsLocked() {
	if q.stats != nil {
		q.stats.setQueue(q.curBytes, q.globalMax)
	}
}

func (q *batchQueue) removePollKeyLocked(key string) {
	for i, k := range q.pollKeys {
		if k == key {
			q.pollKeys = append(q.pollKeys[:i], q.pollKeys[i+1:]...)
			if q.pollIdx >= len(q.pollKeys) {
				q.pollIdx = 0
			}
			break
		}
	}
}

// watchContext 在 ctx 取消时唤醒所有阻塞的 Put/Get，使其检查 ctx 后退出。
func (q *batchQueue) watchContext(ctx context.Context) {
	go func() {
		<-ctx.Done()
		q.mu.Lock()
		q.notFull.Broadcast()
		q.notEmpty.Broadcast()
		q.mu.Unlock()
	}()
}

// Put 投递一个批次；全局或单表 soft limit 达到上限时阻塞。
func (q *batchQueue) Put(ctx context.Context, b *RowBatch) error {
	if b == nil {
		return nil
	}
	key := tableQueueKey(b.Schema, b.Table)
	q.mu.Lock()
	defer q.mu.Unlock()
	sq := q.subQueue(key)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if q.closed {
			return context.Canceled
		}
		tableLimit := q.effectiveTableLimit()
		globalOK := q.curBytes == 0 || q.curBytes+b.ApproxBytes <= q.globalMax
		tableOK := len(sq.items) == 0 || sq.curBytes+b.ApproxBytes <= tableLimit
		if globalOK && tableOK {
			if len(sq.items) == 0 {
				q.ensurePollKeyLocked(key)
			}
			sq.items = append(sq.items, b)
			sq.curBytes += b.ApproxBytes
			q.curBytes += b.ApproxBytes
			q.refreshStatsLocked()
			q.notEmpty.Signal()
			return nil
		}
		q.notFull.Wait()
	}
}

// Get 取出一个批次；队列为空时阻塞，直到有数据、队列关闭或 ctx 取消。
func (q *batchQueue) Get(ctx context.Context) (*RowBatch, bool) {
	b, ok, _ := q.GetUntil(ctx, time.Time{}, "")
	return b, ok
}

// GetUntil 与 Get 相同，但 deadline 到达且队列仍为空时返回 timedOut=true。
// tableKey 非空时仅从该表子队列取批（writer 单事务单表约束）。
func (q *batchQueue) GetUntil(ctx context.Context, deadline time.Time, tableKey string) (*RowBatch, bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var timer *time.Timer
	if !deadline.IsZero() {
		wait := time.Until(deadline)
		if wait <= 0 {
			return nil, true, true
		}
		timer = time.AfterFunc(wait, func() {
			q.mu.Lock()
			q.notEmpty.Broadcast()
			q.mu.Unlock()
		})
		defer timer.Stop()
	}
	for {
		if b, ok := q.dequeueFairLocked(tableKey); ok {
			return b, true, false
		}
		if q.closed {
			return nil, false, false
		}
		if ctx.Err() != nil {
			return nil, false, false
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return nil, true, true
		}
		q.notEmpty.Wait()
	}
}

func (q *batchQueue) dequeueFairLocked(tableKey string) (*RowBatch, bool) {
	if tableKey != "" {
		sq := q.tables[tableKey]
		if sq == nil || len(sq.items) == 0 {
			return nil, false
		}
		return q.popFromSubQueueLocked(sq), true
	}
	if b, ok := q.dequeueFromPollKeysLocked(); ok {
		return b, true
	}
	// pollKeys 可能与 tables 短暂不一致（子队列排空后重新 Put）；扫描 tables 兜底。
	for _, sq := range q.tables {
		if sq != nil && len(sq.items) > 0 {
			return q.popFromSubQueueLocked(sq), true
		}
	}
	return nil, false
}

func (q *batchQueue) dequeueFromPollKeysLocked() (*RowBatch, bool) {
	if len(q.pollKeys) == 0 {
		return nil, false
	}
	start := q.pollIdx
	for i := 0; i < len(q.pollKeys); i++ {
		idx := (start + i) % len(q.pollKeys)
		key := q.pollKeys[idx]
		sq := q.tables[key]
		if sq == nil || len(sq.items) == 0 {
			continue
		}
		q.pollIdx = (idx + 1) % len(q.pollKeys)
		return q.popFromSubQueueLocked(sq), true
	}
	return nil, false
}

func (q *batchQueue) popFromSubQueueLocked(sq *tableSubQueue) *RowBatch {
	b := sq.items[0]
	sq.items[0] = nil
	sq.items = sq.items[1:]
	sq.curBytes -= b.ApproxBytes
	if sq.curBytes < 0 {
		sq.curBytes = 0
	}
	q.curBytes -= b.ApproxBytes
	if q.curBytes < 0 {
		q.curBytes = 0
	}
	if len(sq.items) == 0 {
		q.removePollKeyLocked(sq.key)
	}
	q.refreshStatsLocked()
	q.notFull.Broadcast()
	return b
}

func (q *batchQueue) hasBatchForKey(key string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.hasBatchForKeyLocked(key)
}

func (q *batchQueue) hasOtherTableBatches(key string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.hasOtherTableBatchesLocked(key)
}

// Close 关闭队列，唤醒所有等待者。已入队的批次仍可被 Get 消费。
func (q *batchQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.notFull.Broadcast()
	q.notEmpty.Broadcast()
}

// HasBatchForTable 报告指定表是否仍有排队批次（writer 切换表前检查）。
func (q *batchQueue) HasBatchForTable(schema, table string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	sq := q.tables[tableQueueKey(schema, table)]
	return sq != nil && len(sq.items) > 0
}

func (q *batchQueue) hasBatchForKeyLocked(key string) bool {
	sq := q.tables[key]
	return sq != nil && len(sq.items) > 0
}

func (q *batchQueue) hasOtherTableBatchesLocked(key string) bool {
	for k, sq := range q.tables {
		if k == key {
			continue
		}
		if sq != nil && len(sq.items) > 0 {
			return true
		}
	}
	return false
}
