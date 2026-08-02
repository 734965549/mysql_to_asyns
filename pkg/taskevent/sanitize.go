package taskevent

import (
	"encoding/json"
	"regexp"
	"strings"
)

var (
	passwordFieldRe = regexp.MustCompile(`(?i)(password|passwd|pwd|secret|token|dsn)\s*[:=]\s*[^\s,;'"]+`)
	authBearerRe    = regexp.MustCompile(`(?i)(authorization)\s*[:=]\s*Bearer\s+\S+`)
	bearerTokenRe   = regexp.MustCompile(`(?i)Bearer\s+\S+`)
	encPrefixRe     = regexp.MustCompile(`ENC~[A-Za-z0-9+/=_-]+`)
)

var sensitiveKeys = map[string]struct{}{
	"password":      {},
	"passwd":        {},
	"pwd":           {},
	"secret":        {},
	"token":         {},
	"authorization": {},
	"dsn":           {},
	"connection_string": {},
}

// SanitizeString 对自由文本做二次脱敏。
func SanitizeString(s string) string {
	if s == "" {
		return s
	}
	out := authBearerRe.ReplaceAllString(s, `$1=Bearer [REDACTED]`)
	out = passwordFieldRe.ReplaceAllString(out, `$1=[REDACTED]`)
	out = bearerTokenRe.ReplaceAllString(out, "Bearer [REDACTED]")
	out = encPrefixRe.ReplaceAllString(out, "ENC~[REDACTED]")
	return out
}

// SanitizeDetails 深拷贝并脱敏 details map。
func SanitizeDetails(details map[string]interface{}) map[string]interface{} {
	if len(details) == 0 {
		return nil
	}
	raw, err := json.Marshal(details)
	if err != nil {
		return map[string]interface{}{"error": "details_unserializable"}
	}
	var cloned map[string]interface{}
	if err := json.Unmarshal(raw, &cloned); err != nil {
		return map[string]interface{}{"error": "details_unserializable"}
	}
	redactMap(cloned)
	return cloned
}

func redactMap(m map[string]interface{}) {
	for k, v := range m {
		lk := strings.ToLower(k)
		if _, ok := sensitiveKeys[lk]; ok {
			m[k] = "[REDACTED]"
			continue
		}
		switch tv := v.(type) {
		case map[string]interface{}:
			redactMap(tv)
		case []interface{}:
			for i, item := range tv {
				if sub, ok := item.(map[string]interface{}); ok {
					redactMap(sub)
					tv[i] = sub
				} else if str, ok := item.(string); ok {
					tv[i] = SanitizeString(str)
				}
			}
			m[k] = tv
		case string:
			m[k] = SanitizeString(tv)
		}
	}
}

// SanitizeTaskEventFields 脱敏 message 与 details，返回副本字段。
func SanitizeTaskEventFields(message string, details map[string]interface{}) (string, map[string]interface{}) {
	return SanitizeString(message), SanitizeDetails(details)
}
