package sink

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mysql-to-sync/pkg/crypto"
)

func TestCloneConfigsDeepCopy(t *testing.T) {
	original := []SinkConfig{{
		Type: SinkTypeKAFKA,
		Options: map[string]interface{}{
			"brokers":  []string{"one:9092"},
			"security": map[string]interface{}{"sasl_password": "secret"},
		},
	}}
	cloned := CloneConfigs(original)
	cloned[0].Options["brokers"].([]string)[0] = "two:9092"
	cloned[0].Options["security"].(map[string]interface{})["sasl_password"] = "changed"

	assert.Equal(t, "one:9092", original[0].Options["brokers"].([]string)[0])
	assert.Equal(t, "secret", original[0].Options["security"].(map[string]interface{})["sasl_password"])
}

func TestEncryptDecryptSinkSecretsRoundTrip(t *testing.T) {
	configs := []SinkConfig{
		{
			Type: SinkTypeKAFKA,
			Options: map[string]interface{}{
				"security": map[string]interface{}{"sasl_password": "kafka-secret"},
			},
		},
		{
			Type: SinkTypeHTTPWebhook,
			Options: map[string]interface{}{
				"headers": map[string]interface{}{
					"Authorization": "Bearer token",
					"X-Signature":   "signature",
				},
			},
		},
	}

	require.NoError(t, EncryptSinkSecrets(configs, "encryption-key"))
	kafkaCipher := configs[0].Options["security"].(map[string]interface{})["sasl_password"].(string)
	webhookCipher := configs[1].Options["headers"].(string)
	assert.True(t, crypto.IsEncrypted(kafkaCipher))
	assert.True(t, crypto.IsEncrypted(webhookCipher))

	// Encryption is idempotent for already encrypted values.
	require.NoError(t, EncryptSinkSecrets(configs, "encryption-key"))
	assert.Equal(t, kafkaCipher, configs[0].Options["security"].(map[string]interface{})["sasl_password"])
	assert.Equal(t, webhookCipher, configs[1].Options["headers"])

	require.NoError(t, DecryptSinkSecrets(configs, "encryption-key"))
	assert.Equal(t, "kafka-secret", configs[0].Options["security"].(map[string]interface{})["sasl_password"])
	headers := configs[1].Options["headers"].(map[string]interface{})
	assert.Equal(t, "Bearer token", headers["Authorization"])
	assert.Equal(t, "signature", headers["X-Signature"])
}

func TestDecryptSinkSecretsLegacyPlaintext(t *testing.T) {
	configs := []SinkConfig{
		{Type: SinkTypeKAFKA, Options: map[string]interface{}{
			"security": map[string]interface{}{"sasl_password": "plain-password"},
		}},
		{Type: SinkTypeHTTPWebhook, Options: map[string]interface{}{
			"headers": map[string]interface{}{"Authorization": "plain-token"},
		}},
	}
	require.NoError(t, DecryptSinkSecrets(configs, "key"))
	assert.Equal(t, "plain-password", configs[0].Options["security"].(map[string]interface{})["sasl_password"])
	assert.Equal(t, "plain-token", configs[1].Options["headers"].(map[string]interface{})["Authorization"])
}

func TestDecryptSinkSecretsRejectsInvalidCiphertext(t *testing.T) {
	configs := []SinkConfig{{Type: SinkTypeKAFKA, Options: map[string]interface{}{
		"security": map[string]interface{}{"sasl_password": "ENC~not-base64"},
	}}}
	err := DecryptSinkSecrets(configs, "key")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "security.sasl_password")
}
