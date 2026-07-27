package fullload

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	poolWatchInterval       = 5 * time.Second
	poolWaitHighMinInterval = 30 * time.Second
	poolWaitHighInUseRatio  = 0.85
)

// startSourcePoolWatcher 周期性观测源库连接池等待，高压力时上报 SOURCE_POOL_WAIT_HIGH。
func startSourcePoolWatcher(ctx context.Context, db *sql.DB, sink EventSink) func() {
	if db == nil || sink == nil {
		return func() {}
	}
	stop := make(chan struct{})
	var lastWaitCount int64
	var lastWarnAt time.Time
	go func() {
		ticker := time.NewTicker(poolWatchInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				st := db.Stats()
				if st.MaxOpenConnections <= 0 {
					continue
				}
				curWait := st.WaitCount
				delta := curWait - lastWaitCount
				lastWaitCount = curWait
				inUseRatio := float64(st.InUse) / float64(st.MaxOpenConnections)
				if delta == 0 && inUseRatio < poolWaitHighInUseRatio {
					continue
				}
				now := time.Now()
				if !lastWarnAt.IsZero() && now.Sub(lastWarnAt) < poolWaitHighMinInterval {
					continue
				}
				if delta > 0 || inUseRatio >= poolWaitHighInUseRatio {
					lastWarnAt = now
					poolEvent(sink, EventCodeSourcePoolWaitHigh,
						fmt.Sprintf("source pool under pressure in_use=%d max=%d wait_delta=%d",
							st.InUse, st.MaxOpenConnections, delta),
						EventSeverityWarn,
						map[string]interface{}{
							"in_use":              st.InUse,
							"max_open":            st.MaxOpenConnections,
							"wait_count_delta":    delta,
							"wait_duration_total": st.WaitDuration.String(),
							"in_use_ratio":        inUseRatio,
						})
				}
			}
		}
	}()
	return func() { close(stop) }
}

// queueBackpressureState 跟踪背压高/恢复状态，避免重复 emit。
type queueBackpressureState struct {
	high atomic.Bool
}

func (s *queueBackpressureState) observe(sink EventSink, queueBytes, queueCap int64) {
	if sink == nil || queueCap <= 0 {
		return
	}
	ratio := float64(queueBytes) / float64(queueCap)
	if ratio >= 0.80 && !s.high.Load() {
		s.high.Store(true)
		queueEvent(sink, EventCodeQueueBackpressureHigh,
			fmt.Sprintf("write queue high watermark queue=%d/%d bytes (%.0f%%)",
				queueBytes, queueCap, ratio*100),
			map[string]interface{}{
				"queue_bytes": queueBytes,
				"queue_cap":   queueCap,
				"ratio":       ratio,
			})
		return
	}
	if ratio < 0.50 && s.high.Load() {
		s.high.Store(false)
		Emit(sink, FullLoadEvent{
			Severity:   EventSeverityInfo,
			Category:   EventCategoryQueue,
			Code:       EventCodeQueueBackpressureRecovered,
			Message:    fmt.Sprintf("write queue recovered queue=%d/%d bytes (%.0f%%)", queueBytes, queueCap, ratio*100),
			Details:    map[string]interface{}{"queue_bytes": queueBytes, "queue_cap": queueCap, "ratio": ratio},
			Visibility: EventVisibilityKey,
		})
	}
}
