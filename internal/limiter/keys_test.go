package limiter

import "testing"

func TestKeyBuilder(t *testing.T) {
	keys := KeyBuilder{}
	if got := keys.Global(); got != "seckill:limit:global" {
		t.Fatalf("Global() = %q", got)
	}
	if got := keys.Product(2001); got != "seckill:limit:product:{2001}" {
		t.Fatalf("Product() = %q", got)
	}
	if got := keys.User(1001, 3001); got != "seckill:user:{1001}:3001" {
		t.Fatalf("User() = %q", got)
	}
}
