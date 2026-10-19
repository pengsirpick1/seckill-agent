package logger

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// lokiWriter 将结构化日志放入队列，不让请求协程等待 Loki。
// 收集器持续不可用时，有界队列会主动丢弃日志。
type lokiWriter struct {
	endpoint string
	queue    chan []byte
	client   *http.Client
}

func newLokiWriter(endpoint string) *lokiWriter {
	w := &lokiWriter{endpoint: endpoint, queue: make(chan []byte, 4096), client: &http.Client{Timeout: 2 * time.Second}}
	go w.run()
	return w
}

func (w *lokiWriter) Write(line []byte) (int, error) {
	select {
	case w.queue <- bytes.Clone(line):
	default:
		// 可观测性链路背压时绝不能阻塞业务路径。
	}
	return len(line), nil
}

func (w *lokiWriter) run() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	batch := make([][]byte, 0, 100)
	for {
		select {
		case line := <-w.queue:
			batch = append(batch, line)
			if len(batch) >= 100 {
				w.send(batch)
				batch = make([][]byte, 0, 100)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				w.send(batch)
				batch = make([][]byte, 0, 100)
			}
		}
	}
}

func (w *lokiWriter) send(lines [][]byte) {
	type stream struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"`
	}
	streams := make(map[string]*stream)
	for _, raw := range lines {
		var fields map[string]any
		if json.Unmarshal(bytes.TrimSpace(raw), &fields) != nil {
			continue
		}
		eventTime, _ := fields["time"].(string)
		if eventTime == "" {
			eventTime = time.Now().UTC().Format(time.RFC3339Nano)
		}
		fields["timestamp"] = eventTime
		fields["event_time"] = eventTime
		fields["ingest_time"] = time.Now().UTC().Format(time.RFC3339Nano)
		line, err := json.Marshal(fields)
		if err != nil {
			continue
		}
		service, _ := fields["service"].(string)
		if service == "" {
			service = "unknown"
		}
		level, _ := fields["level"].(string)
		key := service + "\x00" + strings.ToLower(level)
		st := streams[key]
		if st == nil {
			st = &stream{Stream: map[string]string{"service": service}}
			if level != "" {
				st.Stream["level"] = strings.ToLower(level)
			}
			streams[key] = st
		}
		eventTime, _ = fields["event_time"].(string)
		parsed, err := time.Parse(time.RFC3339Nano, eventTime)
		if err != nil {
			parsed = time.Now().UTC()
		}
		st.Values = append(st.Values, [2]string{strconvNano(parsed), string(line)})
	}
	payload := struct {
		Streams []*stream `json:"streams"`
	}{}
	for _, st := range streams {
		payload.Streams = append(payload.Streams, st)
	}
	body, err := json.Marshal(payload)
	if err != nil || len(payload.Streams) == 0 {
		return
	}
	req, err := http.NewRequest(http.MethodPost, w.endpoint, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

func strconvNano(value time.Time) string {
	return strconv.FormatInt(value.UnixNano(), 10)
}
