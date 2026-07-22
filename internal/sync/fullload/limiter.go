package fullload

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// snapshotLimiter 同时限制活跃 snapshot group 数与快照连接总数。
// 协调锁连接也必须占用 conn 槽位，避免「持锁者等连接 / 连接者等锁」自死锁。
//
// conns 使用加权全有或全无获取：一次请求 n 个槽必须原子拿到，避免两个 group
// 各占一部分后互相等待剩余槽位形成永久互等。
type snapshotLimiter struct {
	groups chan struct{}

	mu           sync.Mutex
	connCond     *sync.Cond
	maxConns     int
	connAvail    int
	activeGroups int64
	activeConns  int64

	oldestOpenAt time.Time // 当前最老活跃 group 的打开时间；无活跃时为零值
	groupOpened  map[uint64]time.Time
	nextGroupID  uint64
}

func newSnapshotLimiter(maxGroups, maxConns int) *snapshotLimiter {
	if maxGroups < 1 {
		maxGroups = 1
	}
	if maxConns < 1 {
		maxConns = 1
	}
	l := &snapshotLimiter{
		groups:      make(chan struct{}, maxGroups),
		maxConns:    maxConns,
		connAvail:   maxConns,
		groupOpened: make(map[uint64]time.Time),
	}
	l.connCond = sync.NewCond(&l.mu)
	return l
}

type groupLease struct {
	id     uint64
	lim    *snapshotLimiter
	opened time.Time
}

func (l *snapshotLimiter) acquireGroup(ctx context.Context) (*groupLease, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case l.groups <- struct{}{}:
	}
	now := time.Now()
	l.mu.Lock()
	l.nextGroupID++
	id := l.nextGroupID
	l.groupOpened[id] = now
	if l.oldestOpenAt.IsZero() || now.Before(l.oldestOpenAt) {
		l.oldestOpenAt = now
	}
	l.mu.Unlock()
	atomic.AddInt64(&l.activeGroups, 1)
	return &groupLease{id: id, lim: l, opened: now}, nil
}

func (g *groupLease) release() {
	if g == nil || g.lim == nil {
		return
	}
	l := g.lim
	l.mu.Lock()
	delete(l.groupOpened, g.id)
	var oldest time.Time
	for _, t := range l.groupOpened {
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	l.oldestOpenAt = oldest
	l.mu.Unlock()
	atomic.AddInt64(&l.activeGroups, -1)
	select {
	case <-l.groups:
	default:
	}
	g.lim = nil
}

func (l *snapshotLimiter) acquireConns(ctx context.Context, n int) error {
	if n < 1 {
		return nil
	}
	if n > l.maxConns {
		return fmt.Errorf("requested %d snapshot conns exceeds limiter capacity %d", n, l.maxConns)
	}

	// 在独立 goroutine 里监听取消并 Broadcast，避免 Wait 永久挂起。
	stopWake := make(chan struct{})
	defer close(stopWake)
	go func() {
		select {
		case <-ctx.Done():
			l.mu.Lock()
			l.connCond.Broadcast()
			l.mu.Unlock()
		case <-stopWake:
		}
	}()

	l.mu.Lock()
	defer l.mu.Unlock()
	for l.connAvail < n {
		if err := ctx.Err(); err != nil {
			return err
		}
		l.connCond.Wait()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	l.connAvail -= n
	atomic.AddInt64(&l.activeConns, int64(n))
	return nil
}

func (l *snapshotLimiter) releaseConns(n int) {
	if n < 1 {
		return
	}
	l.mu.Lock()
	l.connAvail += n
	if l.connAvail > l.maxConns {
		l.connAvail = l.maxConns
	}
	active := atomic.AddInt64(&l.activeConns, -int64(n))
	if active < 0 {
		atomic.StoreInt64(&l.activeConns, 0)
	}
	l.connCond.Broadcast()
	l.mu.Unlock()
}

func (l *snapshotLimiter) snapshot() (groups, conns int64, oldestAge time.Duration) {
	groups = atomic.LoadInt64(&l.activeGroups)
	conns = atomic.LoadInt64(&l.activeConns)
	l.mu.Lock()
	oldest := l.oldestOpenAt
	l.mu.Unlock()
	if !oldest.IsZero() {
		oldestAge = time.Since(oldest)
	}
	return groups, conns, oldestAge
}

func (l *snapshotLimiter) String() string {
	g, c, age := l.snapshot()
	return fmt.Sprintf("groups=%d conns=%d oldest_age=%s", g, c, age.Round(time.Millisecond))
}
