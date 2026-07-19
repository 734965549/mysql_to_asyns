package fullload

import (
	"sync/atomic"
	"time"
)

// Stats 汇总 V2 引擎运行期的读、写、提交、队列、连接与重放观测数据。
// 所有字段用原子操作访问，Snapshot 返回一致快照供任务详情接口与日志使用。
type Stats struct {
	ReadRows    int64
	ReadBytes   int64
	ReadBatches int64
	ReadNanos   int64

	WrittenRows    int64
	WrittenBytes   int64
	WrittenBatches int64
	WriteNanos     int64

	CommittedRows  int64
	CommittedBytes int64
	Commits        int64
	CommitNanos    int64

	EnqueueWaitNanos int64 // 生产者（读取 worker）等待队列腾出空间的累计时间

	TxReplays   int64 // 整事务重放次数（锁冲突回滚后重放全部未提交批次）
	LockRetries int64 // 可重试锁错误命中次数

	ActiveReaders int64
	ActiveWriters int64

	QueueBytes int64 // 队列当前字节数（瞬时）
	QueueCap   int64 // 队列容量字节数

	ChunksTotal int64
	ChunksDone  int64
}

func (s *Stats) addReadBatch(rows, bytes int64, dur time.Duration) {
	atomic.AddInt64(&s.ReadRows, rows)
	atomic.AddInt64(&s.ReadBytes, bytes)
	atomic.AddInt64(&s.ReadBatches, 1)
	atomic.AddInt64(&s.ReadNanos, int64(dur))
}

func (s *Stats) addWriteBatch(rows, bytes int64, dur time.Duration) {
	atomic.AddInt64(&s.WrittenRows, rows)
	atomic.AddInt64(&s.WrittenBytes, bytes)
	atomic.AddInt64(&s.WrittenBatches, 1)
	atomic.AddInt64(&s.WriteNanos, int64(dur))
}

func (s *Stats) addCommit(rows, bytes int64, dur time.Duration) {
	atomic.AddInt64(&s.CommittedRows, rows)
	atomic.AddInt64(&s.CommittedBytes, bytes)
	atomic.AddInt64(&s.Commits, 1)
	atomic.AddInt64(&s.CommitNanos, int64(dur))
}

func (s *Stats) addEnqueueWait(dur time.Duration) { atomic.AddInt64(&s.EnqueueWaitNanos, int64(dur)) }
func (s *Stats) incTxReplays()                    { atomic.AddInt64(&s.TxReplays, 1) }
func (s *Stats) incLockRetries()                  { atomic.AddInt64(&s.LockRetries, 1) }
func (s *Stats) incChunkDone()                    { atomic.AddInt64(&s.ChunksDone, 1) }
func (s *Stats) setQueue(bytes, cap int64) {
	atomic.StoreInt64(&s.QueueBytes, bytes)
	atomic.StoreInt64(&s.QueueCap, cap)
}

// StatsSnapshot 是 Stats 的只读快照，可直接 JSON 序列化返回给任务详情接口。
type StatsSnapshot struct {
	ReadRows       int64 `json:"read_rows"`
	ReadBytes      int64 `json:"read_bytes"`
	ReadBatches    int64 `json:"read_batches"`
	ReadMillis     int64 `json:"read_millis"`
	WrittenRows    int64 `json:"written_rows"`
	WrittenBytes   int64 `json:"written_bytes"`
	WrittenBatches int64 `json:"written_batches"`
	WriteMillis    int64 `json:"write_millis"`
	CommittedRows  int64 `json:"committed_rows"`
	CommittedBytes int64 `json:"committed_bytes"`
	Commits        int64 `json:"commits"`
	CommitMillis   int64 `json:"commit_millis"`
	EnqueueWaitMs  int64 `json:"enqueue_wait_millis"`
	TxReplays      int64 `json:"tx_replays"`
	LockRetries    int64 `json:"lock_retries"`
	ActiveReaders  int64 `json:"active_readers"`
	ActiveWriters  int64 `json:"active_writers"`
	QueueBytes     int64 `json:"queue_bytes"`
	QueueCap       int64 `json:"queue_cap_bytes"`
	ChunksTotal    int64 `json:"chunks_total"`
	ChunksDone     int64 `json:"chunks_done"`
}

// Snapshot 返回当前统计的一致快照。
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		ReadRows:       atomic.LoadInt64(&s.ReadRows),
		ReadBytes:      atomic.LoadInt64(&s.ReadBytes),
		ReadBatches:    atomic.LoadInt64(&s.ReadBatches),
		ReadMillis:     atomic.LoadInt64(&s.ReadNanos) / int64(time.Millisecond),
		WrittenRows:    atomic.LoadInt64(&s.WrittenRows),
		WrittenBytes:   atomic.LoadInt64(&s.WrittenBytes),
		WrittenBatches: atomic.LoadInt64(&s.WrittenBatches),
		WriteMillis:    atomic.LoadInt64(&s.WriteNanos) / int64(time.Millisecond),
		CommittedRows:  atomic.LoadInt64(&s.CommittedRows),
		CommittedBytes: atomic.LoadInt64(&s.CommittedBytes),
		Commits:        atomic.LoadInt64(&s.Commits),
		CommitMillis:   atomic.LoadInt64(&s.CommitNanos) / int64(time.Millisecond),
		EnqueueWaitMs:  atomic.LoadInt64(&s.EnqueueWaitNanos) / int64(time.Millisecond),
		TxReplays:      atomic.LoadInt64(&s.TxReplays),
		LockRetries:    atomic.LoadInt64(&s.LockRetries),
		ActiveReaders:  atomic.LoadInt64(&s.ActiveReaders),
		ActiveWriters:  atomic.LoadInt64(&s.ActiveWriters),
		QueueBytes:     atomic.LoadInt64(&s.QueueBytes),
		QueueCap:       atomic.LoadInt64(&s.QueueCap),
		ChunksTotal:    atomic.LoadInt64(&s.ChunksTotal),
		ChunksDone:     atomic.LoadInt64(&s.ChunksDone),
	}
}
