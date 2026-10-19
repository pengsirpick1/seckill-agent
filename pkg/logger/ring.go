package logger

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Ring 在进程内保留有界的结构化日志副本，供只读诊断使用。
type Ring struct {
	mu       sync.RWMutex
	lines    [][]byte
	capacity int
	next     int
	count    int
}

func NewRing(capacity int) *Ring {
	if capacity < 1 {
		capacity = 1
	}
	return &Ring{lines: make([][]byte, capacity), capacity: capacity}
}

func (r *Ring) Write(line []byte) (int, error) {
	r.mu.Lock()
	r.lines[r.next] = bytes.TrimSpace(bytes.Clone(line))
	r.next = (r.next + 1) % r.capacity
	if r.count < r.capacity {
		r.count++
	}
	r.mu.Unlock()
	return len(line), nil
}

func (r *Ring) Recent(level string, since time.Duration, limit int, traceID string) []json.RawMessage {
	if limit < 1 || limit > 100 {
		limit = 100
	}
	cutoff := time.Now().Add(-since)
	r.mu.RLock()
	ordered := make([][]byte, 0, r.count)
	start := (r.next - r.count + r.capacity) % r.capacity
	for index := 0; index < r.count; index++ {
		line := r.lines[(start+index)%r.capacity]
		ordered = append(ordered, bytes.Clone(line))
	}
	r.mu.RUnlock()

	filtered := make([]json.RawMessage, 0, limit)
	for index := len(ordered) - 1; index >= 0 && len(filtered) < limit; index-- {
		var fields map[string]any
		if json.Unmarshal(ordered[index], &fields) != nil {
			continue
		}
		if level != "" && !strings.EqualFold(stringValue(fields["level"]), level) {
			continue
		}
		if traceID != "" && !strings.EqualFold(stringValue(fields["trace_id"]), traceID) {
			continue
		}
		if since > 0 {
			timestamp, err := time.Parse(time.RFC3339Nano, stringValue(fields["time"]))
			if err != nil || timestamp.Before(cutoff) {
				continue
			}
		}
		filtered = append(filtered, json.RawMessage(ordered[index]))
	}
	return filtered
}

func LogsHandler(token string) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		authorized := request.Header.Get("Authorization")
		expected := "Bearer " + token
		if token == "" || len(authorized) != len(expected) || subtle.ConstantTimeCompare([]byte(authorized), []byte(expected)) != 1 {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		limit, _ := strconv.Atoi(request.URL.Query().Get("limit"))
		seconds, _ := strconv.Atoi(request.URL.Query().Get("since_seconds"))
		response := struct {
			Logs []json.RawMessage `json:"logs"`
		}{Logs: processLogs.Recent(request.URL.Query().Get("level"), time.Duration(seconds)*time.Second, limit, request.URL.Query().Get("trace_id"))}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(response)
	})
}

func stringValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}
