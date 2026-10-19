package inventory

import "testing"

func TestKeyBuilderUsesActivityHashTag(t *testing.T) {
	keys := KeyBuilder{}
	if got, want := keys.Stock(1001, 2001), "seckill:stock:{1001}:2001"; got != want {
		t.Fatalf("Stock() = %q, want %q", got, want)
	}
}

func TestKeyBuilderRankingUsesActivityHashTag(t *testing.T) {
	keys := KeyBuilder{}
	if got, want := keys.Ranking(1001), "seckill:ranking:{1001}"; got != want {
		t.Fatalf("Ranking() = %q, want %q", got, want)
	}
}
