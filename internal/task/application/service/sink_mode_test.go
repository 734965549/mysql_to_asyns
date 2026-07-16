package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sinkDomain "mysql-to-sync/internal/sync/domain/sink"
	taskEntity "mysql-to-sync/internal/task/domain/entity"
)

func TestValidateSinkMode(t *testing.T) {
	tests := []struct {
		name    string
		mode    taskEntity.SyncMode
		sinks   []sinkDomain.SinkConfig
		wantErr bool
	}{
		{name: "legacy full without configs", mode: taskEntity.SyncModeFull},
		{name: "full mysql", mode: taskEntity.SyncModeFull, sinks: []sinkDomain.SinkConfig{{Type: sinkDomain.SinkTypeMYSQL}}},
		{name: "all mysql", mode: taskEntity.SyncModeAll, sinks: []sinkDomain.SinkConfig{{Type: sinkDomain.SinkTypeMYSQL}}},
		{name: "incremental kafka", mode: taskEntity.SyncModeIncremental, sinks: []sinkDomain.SinkConfig{{Type: sinkDomain.SinkTypeKAFKA}}},
		{name: "incremental webhook", mode: taskEntity.SyncModeIncremental, sinks: []sinkDomain.SinkConfig{{Type: sinkDomain.SinkTypeHTTPWebhook}}},
		{name: "full kafka rejected", mode: taskEntity.SyncModeFull, sinks: []sinkDomain.SinkConfig{{Type: sinkDomain.SinkTypeKAFKA}}, wantErr: true},
		{name: "all webhook rejected", mode: taskEntity.SyncModeAll, sinks: []sinkDomain.SinkConfig{{Type: sinkDomain.SinkTypeHTTPWebhook}}, wantErr: true},
		{name: "all mixed rejected", mode: taskEntity.SyncModeAll, sinks: []sinkDomain.SinkConfig{{Type: sinkDomain.SinkTypeMYSQL}, {Type: sinkDomain.SinkTypeKAFKA}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := taskEntity.NewSyncTask(taskEntity.TaskConfig{Mode: tt.mode, SinkConfigs: tt.sinks})
			err := validateSinkMode(task)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "仅支持 INCREMENTAL")
				return
			}
			require.NoError(t, err)
		})
	}
}
