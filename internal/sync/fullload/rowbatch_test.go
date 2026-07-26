package fullload

import "testing"

func TestEstimateValueBytes(t *testing.T) {
	if estimateValueBytes(nil) != 1 {
		t.Error("nil")
	}
	if estimateValueBytes([]byte("abc")) != 5 {
		t.Error("bytes")
	}
	if estimateValueBytes("hello") != 7 {
		t.Error("string")
	}
	if estimateValueBytes(int64(123)) != 8 {
		t.Error("int")
	}
}
