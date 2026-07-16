package sink

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metadataEntity "mysql-to-sync/internal/metadata/domain/entity"
	sinkDomain "mysql-to-sync/internal/sync/domain/sink"
)

type factoryAnalyzer struct{}

func (factoryAnalyzer) AnalyzeTable(string, string) (*metadataEntity.TableIdentity, error) {
	return &metadataEntity.TableIdentity{}, nil
}
func (factoryAnalyzer) GetAllTables(string) ([]metadataEntity.TableInfo, error) { return nil, nil }
func (factoryAnalyzer) GetAllDatabases() ([]string, error)                      { return nil, nil }

func TestNewSinks_DefaultsToMySQL(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sinks, err := NewSinks(nil, SinkDeps{TargetDB: db, Analyzer: factoryAnalyzer{}, BatchSize: 10})
	require.NoError(t, err)
	require.Len(t, sinks, 1)
	assert.Equal(t, sinkDomain.SinkTypeMYSQL, sinks[0].Type())
}

func TestNewSinks_CreatesConfiguredTypes(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	configs := []sinkDomain.SinkConfig{
		{Type: sinkDomain.SinkTypeMYSQL},
		{Type: sinkDomain.SinkTypeKAFKA, Options: map[string]interface{}{
			"brokers": []string{"broker:9092"}, "topic": "cdc",
		}},
		{Type: sinkDomain.SinkTypeHTTPWebhook, Options: map[string]interface{}{
			"url": "https://example.com/hook",
		}},
	}
	sinks, err := NewSinks(configs, SinkDeps{TargetDB: db, Analyzer: factoryAnalyzer{}, BatchSize: 10})
	require.NoError(t, err)
	require.Len(t, sinks, 3)
	assert.Equal(t, sinkDomain.SinkTypeMYSQL, sinks[0].Type())
	assert.Equal(t, sinkDomain.SinkTypeKAFKA, sinks[1].Type())
	assert.Equal(t, sinkDomain.SinkTypeHTTPWebhook, sinks[2].Type())
}

func TestNewSinks_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		configs []sinkDomain.SinkConfig
		deps    SinkDeps
		want    string
	}{
		{name: "default mysql missing deps", want: "target database"},
		{name: "configured mysql missing deps", configs: []sinkDomain.SinkConfig{{Type: sinkDomain.SinkTypeMYSQL}}, want: "target database"},
		{name: "invalid kafka", configs: []sinkDomain.SinkConfig{{Type: sinkDomain.SinkTypeKAFKA}}, want: "brokers"},
		{name: "invalid webhook", configs: []sinkDomain.SinkConfig{{Type: sinkDomain.SinkTypeHTTPWebhook}}, want: "url"},
		{name: "unknown type", configs: []sinkDomain.SinkConfig{{Type: "S3"}}, want: "unknown sink type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSinks(tt.configs, tt.deps)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}
