package order

import "testing"

func TestIDGeneratorSeparatesGatewayNodeIDs(t *testing.T) {
	first, err := NewIDGenerator(17)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewIDGenerator(18)
	if err != nil {
		t.Fatal(err)
	}
	if first.Next() == second.Next() {
		t.Fatal("distinct gateway node IDs produced the same order ID")
	}
}
