package fullload

import (
	"context"
	"sync"
	"time"
)

// batchQueue 是按字节数限流的有界 RowBatch 队列。
//
// 源读取速度高于目标写入速度时提供短期缓冲；缓冲字节数达到上限后 Put 阻塞，
// 形成对读取 worker 的背压。目标写入恢复后 Get 消费腾出空间，Put 自动继续。
type batchQueue struct {
	mu       sync.Mutex
	notFull  *sync.Cond
	notEmpty *sync.Cond

	items    []*RowBatch
	curBytes int64
	maxBytes int64

	closed bool
	stats  *Stats
}

func newBatchQueue(maxBytes int64, stats *Stats) *batchQueue {
	if maxBytes < 1 {
		maxBytes = defaultBufferBytes
	}
	q := &batchQueue{maxBytes: maxBytes, stats: stats}
	q.notFull = sync.NewCond(&q.mu)
	q.notEmpty = sync.NewCond(&q.mu)
	if stats != nil {
		stats.setQueue(0, maxBytes)
	}
	return q
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

// Put 投递一个批次；队列字节数达到上限时阻塞，直到有空间、队列关闭或 ctx 取消。
// 为避免单个超大批次永久阻塞，队列为空时总是允许放入。
func (q *batchQueue) Put(ctx context.Context, b *RowBatch) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if q.closed {
			return context.Canceled
		}
		if len(q.items) == 0 || q.curBytes+b.ApproxBytes <= q.maxBytes {
			q.items = append(q.items, b)
			q.curBytes += b.ApproxBytes
			if q.stats != nil {
				q.stats.setQueue(q.curBytes, q.maxBytes)
			}
			q.notEmpty.Signal()
			return nil
		}
		q.notFull.Wait()
	}
}

// Get 取出一个批次；队列为空时阻塞，直到有数据、队列关闭或 ctx 取消。
// 队列关闭且清空后返回 (nil, false)。
func (q *batchQueue) Get(ctx context.Context) (*RowBatch, bool) {
	b, ok, _ := q.GetUntil(ctx, time.Time{})
	return b, ok
}

// GetUntil 与 Get 相同，但 deadline 到达且队列仍为空时返回 timedOut=true。
// writer 用它确保低流量时事务也会按 CommitInterval 提交，而不是无限等待下一批。
func (q *batchQueue) GetUntil(ctx context.Context, deadline time.Time) (*RowBatch, bool, bool) {
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
		if len(q.items) > 0 {
			b := q.items[0]
			q.items[0] = nil
			q.items = q.items[1:]
			q.curBytes -= b.ApproxBytes
			if q.curBytes < 0 {
				q.curBytes = 0
			}
			if q.stats != nil {
				q.stats.setQueue(q.curBytes, q.maxBytes)
			}
			q.notFull.Signal()
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

// Close 关闭队列，唤醒所有等待者。已入队的批次仍可被 Get 消费。
func (q *batchQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.notFull.Broadcast()
	q.notEmpty.Broadcast()
}
