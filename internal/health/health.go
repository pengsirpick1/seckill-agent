// health 包负责存活检查和依赖就绪检查。
package health

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

type Probe interface {
	Name() string
	Check(context.Context) error
}

type TCPProbe struct {
	Dependency string
	Address    string
}

func (p TCPProbe) Name() string { return p.Dependency }

func (p TCPProbe) Check(ctx context.Context) error {
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.Address)
	if err != nil {
		return fmt.Errorf("dial %s: %w", p.Address, err)
	}
	return connection.Close()
}

type Result struct {
	Status       string            `json:"status"`
	Dependencies map[string]string `json:"dependencies,omitempty"`
}

func CheckAll(ctx context.Context, probes []Probe) Result {
	result := Result{Status: "ok", Dependencies: make(map[string]string, len(probes))}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, probe := range probes {
		probe := probe
		wg.Add(1)
		go func() {
			defer wg.Done()
			status := "ok"
			if err := probe.Check(ctx); err != nil {
				status = "unavailable"
			}
			mu.Lock()
			result.Dependencies[probe.Name()] = status
			if status != "ok" {
				result.Status = "not_ready"
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return result
}

func NewTCPProbes(mysql, redis string, kafka, etcd []string) []Probe {
	probes := []Probe{
		TCPProbe{Dependency: "mysql", Address: mysql},
		TCPProbe{Dependency: "redis", Address: redis},
	}
	for index, address := range kafka {
		probes = append(probes, TCPProbe{Dependency: fmt.Sprintf("kafka-%d", index), Address: address})
	}
	for index, address := range etcd {
		probes = append(probes, TCPProbe{Dependency: fmt.Sprintf("etcd-%d", index), Address: address})
	}
	sort.Slice(probes, func(i, j int) bool { return probes[i].Name() < probes[j].Name() })
	return probes
}

func ContextWithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}
