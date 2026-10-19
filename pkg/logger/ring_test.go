package logger

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRingRecentFiltersAndBoundsResults(t *testing.T) {
	ring := NewRing(2)
	_, _ = ring.Write([]byte(`{"time":"` + time.Now().UTC().Format(time.RFC3339Nano) + `","level":"info","trace_id":"abc"}`))
	_, _ = ring.Write([]byte(`{"time":"` + time.Now().UTC().Format(time.RFC3339Nano) + `","level":"error","trace_id":"def"}`))
	_, _ = ring.Write([]byte(`{"time":"` + time.Now().UTC().Format(time.RFC3339Nano) + `","level":"warn","trace_id":"abc"}`))
	logs := ring.Recent("warn", time.Minute, 10, "abc")
	if len(logs) != 1 || !strings.Contains(string(logs[0]), `"level":"warn"`) {
		t.Fatalf("unexpected filtered logs: %s", logs)
	}
	if got := len(ring.Recent("", 0, 1, "")); got != 1 {
		t.Fatalf("limit was not enforced: got %d", got)
	}
}

func TestLogsHandlerRequiresBearerToken(t *testing.T) {
	handler := LogsHandler("secret")
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest("GET", "/internal/diagnostic/logs", nil))
	if unauthorized.Code != 401 {
		t.Fatalf("status=%d want 401", unauthorized.Code)
	}
	allowed := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/internal/diagnostic/logs?limit=5", nil)
	request.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(allowed, request)
	if allowed.Code != 200 || !strings.Contains(allowed.Body.String(), `"logs"`) {
		t.Fatalf("status=%d body=%s", allowed.Code, allowed.Body.String())
	}
}
