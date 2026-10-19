package seckill

import "testing"

func TestValidMoney(t *testing.T) {
	for _, value := range []string{"0", "1", "12.3", "12.34"} {
		if !validMoney(value) {
			t.Fatalf("validMoney(%q) = false", value)
		}
	}
	for _, value := range []string{"-1", "1.234", "one", ""} {
		if validMoney(value) {
			t.Fatalf("validMoney(%q) = true", value)
		}
	}
}
