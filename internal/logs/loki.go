package logs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type LokiClient struct {
	baseURL string
	client  *http.Client
}
type Entry struct {
	Timestamp time.Time `json:"timestamp"`
	Line      string    `json:"line"`
	Service   string    `json:"service,omitempty"`
	TraceID   string    `json:"trace_id,omitempty"`
}
type queryResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Values [][]string `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func NewLokiClient(baseURL string, client *http.Client) *LokiClient {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &LokiClient{baseURL: baseURL, client: client}
}

// Query 最多返回 100 条结构化日志；trace_id 在 JSON 解析后过滤。
func (c *LokiClient) Query(ctx context.Context, service, traceID string, start, end time.Time, limit int) ([]Entry, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	selector := "{}"
	if service != "" {
		selector = `{service="` + escapeLabel(service) + `"}`
	}
	query := selector + " | json"
	if traceID != "" {
		query += ` | trace_id="` + escapeLogQL(traceID) + `"`
	}
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {strconv.Itoa(limit)}, "direction": {"backward"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Loki query returned HTTP %d", resp.StatusCode)
	}
	var decoded queryResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, limit)
	for _, stream := range decoded.Data.Result {
		for _, pair := range stream.Values {
			if len(pair) != 2 {
				continue
			}
			ns, _ := strconv.ParseInt(pair[0], 10, 64)
			entry := Entry{Timestamp: time.Unix(0, ns).UTC(), Line: pair[1]}
			var fields map[string]any
			if json.Unmarshal([]byte(entry.Line), &fields) == nil {
				entry.Service, _ = fields["service"].(string)
				entry.TraceID, _ = fields["trace_id"].(string)
			}
			entries = append(entries, entry)
			if len(entries) >= limit {
				return entries, nil
			}
		}
	}
	return entries, nil
}

func escapeLabel(v string) string { b, _ := json.Marshal(v); return string(b[1 : len(b)-1]) }
func escapeLogQL(v string) string { b, _ := json.Marshal(v); return string(b[1 : len(b)-1]) }
