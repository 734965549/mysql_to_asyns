package fullload

import (
	"sync"
	"time"
)

const (
	EventCodeTableNoProgress        = "TABLE_NO_PROGRESS"
	EventCodeTableProgressRecovered = "TABLE_PROGRESS_RECOVERED"
)

// tableProgressWatch 检测表级读取长时间无进展。
type tableProgressWatch struct {
	threshold time.Duration
	sink      EventSink

	mu        sync.Mutex
	rows      map[string]int64
	stalled   map[string]bool
	lastCheck map[string]time.Time
}

func newTableProgressWatch(thresholdSec int, sink EventSink) *tableProgressWatch {
	if thresholdSec <= 0 {
		return nil
	}
	return &tableProgressWatch{
		threshold: time.Duration(thresholdSec) * time.Second,
		sink:      sink,
		rows:      make(map[string]int64),
		stalled:   make(map[string]bool),
		lastCheck: make(map[string]time.Time),
	}
}

func (w *tableProgressWatch) seed(specs []*TableSpec) {
	if w == nil {
		return
	}
	now := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		key := tableKey(spec.SourceSchema, spec.SourceTable)
		w.rows[key] = 0
		w.stalled[key] = false
		w.lastCheck[key] = now
	}
}

// tick 由 engine.reportLoop 周期调用，比较各表读行数增量。
func (w *tableProgressWatch) tick(tableRows map[string]int64) {
	if w == nil || w.threshold <= 0 {
		return
	}
	now := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()
	for key, rows := range tableRows {
		prev, ok := w.rows[key]
		if !ok {
			w.rows[key] = rows
			w.lastCheck[key] = now
			continue
		}
		if rows > prev {
			w.rows[key] = rows
			w.lastCheck[key] = now
			if w.stalled[key] {
				w.stalled[key] = false
				schema, table := splitTableKey(key)
				tableEvent(w.sink, schema, table, EventCodeTableProgressRecovered, EventCategoryTable,
					EventSeverityInfo,
					"table read progress resumed",
					map[string]interface{}{"read_rows": rows})
			}
			continue
		}
		if rows != prev {
			continue
		}
		last := w.lastCheck[key]
		if last.IsZero() {
			w.lastCheck[key] = now
			continue
		}
		if !w.stalled[key] && now.Sub(last) >= w.threshold {
			w.stalled[key] = true
			schema, table := splitTableKey(key)
			tableEvent(w.sink, schema, table, EventCodeTableNoProgress, EventCategoryTable,
				EventSeverityWarn,
				"table has no read progress",
				map[string]interface{}{
					"read_rows":        rows,
					"threshold_sec":    int(w.threshold.Seconds()),
					"since_last_check": now.Sub(last).Seconds(),
				})
		}
	}
}

func splitTableKey(key string) (schema, table string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '.' {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}
