//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pengtor/seckill-agent/internal/logs"
)

func TestAlloyIngestsAndLokiQueriesByServiceAndTrace(t *testing.T) {
	pushURL:=os.Getenv("LOG_SHIPPER_URL");if pushURL==""{pushURL="http://localhost:3101/loki/api/v1/push"}
	lokiURL:=os.Getenv("LOKI_URL");if lokiURL==""{lokiURL="http://localhost:3100"}
	traceID:=fmt.Sprintf("m8b-%d",time.Now().UnixNano());eventTime:=time.Now().UTC();line,_:=json.Marshal(map[string]string{"timestamp":eventTime.Format(time.RFC3339Nano),"event_time":eventTime.Format(time.RFC3339Nano),"ingest_time":eventTime.Format(time.RFC3339Nano),"service":"m8b-integration","level":"info","trace_id":traceID,"message":"alloy loki integration"})
	payload,_:=json.Marshal(map[string]any{"streams":[]any{map[string]any{"stream":map[string]string{"service":"m8b-integration","level":"info"},"values":[][2]string{{fmt.Sprint(eventTime.UnixNano()),string(line)}}}}})
	ctx,cancel:=context.WithTimeout(context.Background(),20*time.Second);defer cancel();request,_:=http.NewRequestWithContext(ctx,http.MethodPost,pushURL,strings.NewReader(string(payload)));request.Header.Set("Content-Type","application/json")
	response,err:=http.DefaultClient.Do(request);if err!=nil{t.Skipf("Alloy integration dependency unavailable: %v",err)};response.Body.Close();if response.StatusCode<200||response.StatusCode>=300{t.Fatalf("Alloy push returned HTTP %d",response.StatusCode)}
	client:=logs.NewLokiClient(lokiURL,&http.Client{Timeout:3*time.Second});deadline:=time.Now().Add(15*time.Second)
	for time.Now().Before(deadline){entries,err:=client.Query(ctx,"m8b-integration",traceID,eventTime.Add(-time.Minute),time.Now().Add(time.Minute),10);if err==nil&&len(entries)>0{if entries[0].Service!="m8b-integration"||entries[0].TraceID!=traceID{t.Fatalf("query returned wrong log: %+v",entries[0])};return};time.Sleep(250*time.Millisecond)}
	t.Fatal("Loki did not return the pushed service/trace_id log before timeout")
}
