package health

import (
	"context"
	"errors"
	"testing"
)

type fakeProbe struct {
	name string
	err  error
}

func (p fakeProbe) Name() string                { return p.name }
func (p fakeProbe) Check(context.Context) error { return p.err }

func TestCheckAllMarksDependencyFailure(t *testing.T) {
	result := CheckAll(context.Background(), []Probe{
		fakeProbe{name: "redis"},
		fakeProbe{name: "mysql", err: errors.New("down")},
	})
	if result.Status != "not_ready" || result.Dependencies["mysql"] != "unavailable" {
		t.Fatalf("unexpected result: %#v", result)
	}
}
