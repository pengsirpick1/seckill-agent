package logs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLokiQueryFiltersServiceAndTraceID(t *testing.T) {
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){query:=r.URL.Query().Get("query");if !strings.Contains(query,`{service="gateway"}`)||!strings.Contains(query,`trace_id="trace-1"`){t.Errorf("unexpected LogQL query: %s",query)};w.Header().Set("Content-Type","application/json");_,_=w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[{"values":[["1791421200000000000","{\"service\":\"gateway\",\"trace_id\":\"trace-1\"}"]]}]}}`))}));defer server.Close()
	client:=NewLokiClient(server.URL,server.Client());entries,err:=client.Query(context.Background(),"gateway","trace-1",time.Now().Add(-time.Minute),time.Now(),10);if err!=nil{t.Fatal(err)};if len(entries)!=1||entries[0].TraceID!="trace-1"||entries[0].Service!="gateway"{t.Fatalf("unexpected entries: %#v",entries)}
}
