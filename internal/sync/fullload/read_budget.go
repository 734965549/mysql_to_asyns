package fullload

import (
	"context"
	"sync"
	"time"
)

// ComputeReservedConns 为源库连接池保留的非读取连接数（元数据、规划等）。
func ComputeReservedConns(sourcePoolMax int) int {
	if sourcePoolMax <= 0 {
		return 2
	}
	reserved := (sourcePoolMax + 9) / 10 // ceil(10%)
	if reserved < 2 {
		return 2
	}
	return reserved
}

// ComputeGlobalReadBudget 计算任务级源库读取总预算。
func ComputeGlobalReadBudget(configured, sourcePoolMax int) int {
	budget := configured
	if budget < 1 {
		budget = defaultReadWorkers
	}
	if sourcePoolMax > 0 {
		avail := sourcePoolMax - ComputeReservedConns(sourcePoolMax)
		if avail < 1 {
			avail = 1
		}
		if budget > avail {
			budget = avail
		}
	}
	return budget
}

// PerTableEffectiveLimit 在有其他表等待时限制单表占用。
func PerTableEffectiveLimit(perTableReaders, globalBudget, waitingTables int) int {
	if perTableReaders < 1 {
		perTableReaders = 1
	}
	if waitingTables >= 2 && globalBudget >= 2 {
		half := globalBudget / 2
		if half < 1 {
			half = 1
		}
		if perTableReaders > half {
			perTableReaders = half
		}
	}
	return perTableReaders
}

// ReadBudget 全局源库读取令牌池。
type ReadBudget struct {
	total int

	mu       sync.Mutex
	inUse    int
	perTable map[string]int
}

// NewReadBudget 创建读取预算池。
func NewReadBudget(total int) *ReadBudget {
	if total < 1 {
		total = 1
	}
	return &ReadBudget{
		total:    total,
		perTable: make(map[string]int),
	}
}

func (b *ReadBudget) Total() int {
	if b == nil {
		return 0
	}
	return b.total
}

func (b *ReadBudget) InUse() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inUse
}

func (b *ReadBudget) tryAcquireLocked(tableKey string, perTableLimit int) bool {
	tableUsed := b.perTable[tableKey]
	if b.inUse >= b.total {
		return false
	}
	if perTableLimit > 0 && tableUsed >= perTableLimit {
		return false
	}
	b.inUse++
	b.perTable[tableKey] = tableUsed + 1
	return true
}

// Acquire 获取读取令牌；perTableLimit<=0 时不限制单表。
func (b *ReadBudget) Acquire(ctx context.Context, tableKey string, perTableLimit int) error {
	if b == nil {
		return nil
	}
	for {
		b.mu.Lock()
		if b.tryAcquireLocked(tableKey, perTableLimit) {
			b.mu.Unlock()
			return nil
		}
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// Release 归还令牌。
func (b *ReadBudget) Release(tableKey string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	if b.inUse > 0 {
		b.inUse--
	}
	if b.perTable[tableKey] > 0 {
		b.perTable[tableKey]--
	}
	b.mu.Unlock()
}
