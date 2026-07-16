package sink

import (
	"database/sql"
	"fmt"

	"mysql-to-sync/internal/metadata/domain/service"
	"mysql-to-sync/internal/sync/domain/sink"
	"mysql-to-sync/internal/sync/infrastructure/sink/kafka"
	"mysql-to-sync/internal/sync/infrastructure/sink/mysql"
	"mysql-to-sync/internal/sync/infrastructure/sink/webhook"
)

type SinkDeps struct {
	TargetDB  *sql.DB
	Analyzer  service.IdentityAnalyzer
	BatchSize int
}

func NewSinks(configs []sink.SinkConfig, deps SinkDeps) ([]sink.Sink, error) {
	if len(configs) == 0 {
		if deps.TargetDB == nil {
			return nil, fmt.Errorf("default MYSQL sink requires target database")
		}
		if deps.Analyzer == nil {
			return nil, fmt.Errorf("default MYSQL sink requires identity analyzer")
		}
		ms := mysql.NewMySQLSink(deps.TargetDB, deps.Analyzer, deps.BatchSize)
		return []sink.Sink{ms}, nil
	}

	sinks := make([]sink.Sink, 0, len(configs))
	for i, cfg := range configs {
		switch cfg.Type {
		case sink.SinkTypeMYSQL:
			if deps.TargetDB == nil {
				return nil, fmt.Errorf("sink_configs[%d] MYSQL requires target database", i)
			}
			if deps.Analyzer == nil {
				return nil, fmt.Errorf("sink_configs[%d] MYSQL requires identity analyzer", i)
			}
			ms := mysql.NewMySQLSink(deps.TargetDB, deps.Analyzer, deps.BatchSize)
			sinks = append(sinks, ms)
		case sink.SinkTypeKAFKA:
			ks := kafka.NewKafkaSink(cfg.Options)
			if err := ks.Validate(); err != nil {
				return nil, fmt.Errorf("sink_configs[%d] KAFKA: %w", i, err)
			}
			sinks = append(sinks, ks)
		case sink.SinkTypeHTTPWebhook:
			ws := webhook.NewWebhookSink(cfg.Options)
			if err := ws.Validate(); err != nil {
				return nil, fmt.Errorf("sink_configs[%d] HTTP_WEBHOOK: %w", i, err)
			}
			sinks = append(sinks, ws)
		default:
			return nil, fmt.Errorf("sink_configs[%d]: unknown sink type %q", i, cfg.Type)
		}
	}
	return sinks, nil
}
