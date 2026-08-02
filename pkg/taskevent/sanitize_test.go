package taskevent

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeString(t *testing.T) {
	in := "connect dsn=user:secret@tcp(localhost:3306)/db password=abc123"
	out := SanitizeString(in)
	assert.NotContains(t, out, "secret")
	assert.NotContains(t, out, "abc123")
	assert.Contains(t, out, "[REDACTED]")
}

func TestSanitizeDetails(t *testing.T) {
	out := SanitizeDetails(map[string]interface{}{
		"password": "p@ss",
		"nested": map[string]interface{}{
			"token": "tok",
		},
	})
	assert.Equal(t, "[REDACTED]", out["password"])
	nested := out["nested"].(map[string]interface{})
	assert.Equal(t, "[REDACTED]", nested["token"])
}

func TestSanitizeString_AuthorizationAndDSN(t *testing.T) {
	in := "failed connect dsn=root:secret@tcp(127.0.0.1:3306)/db Authorization=Bearer abc.def"
	out := SanitizeString(in)
	assert.NotContains(t, out, "secret")
	assert.NotContains(t, out, "abc.def")
	assert.Contains(t, out, "[REDACTED]")
}

func TestSanitizeDetails_RowPayloadNotCopiedRaw(t *testing.T) {
	out := SanitizeDetails(map[string]interface{}{
		"message": "row password=leak",
		"rows": []interface{}{
			map[string]interface{}{"password": "row-secret"},
		},
	})
	rows := out["rows"].([]interface{})
	row0 := rows[0].(map[string]interface{})
	assert.Equal(t, "[REDACTED]", row0["password"])
}
