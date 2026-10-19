package config

import "testing"

func TestLoadRejectsUnknownLockMode(t *testing.T) {
	t.Setenv("SECKILL_LOCK_MODE", "unknown")
	if _, err := Load("gateway"); err == nil {
		t.Fatal("expected invalid lock mode error")
	}
}

func TestLoadUsesEtcdLockDefaults(t *testing.T) {
	cfg, err := Load("gateway")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Etcd.LockSessionTTL.String() != "10s" || cfg.Etcd.LockAcquireTimeout.String() != "2s" {
		t.Fatalf("unexpected Etcd lock defaults: ttl=%s timeout=%s", cfg.Etcd.LockSessionTTL, cfg.Etcd.LockAcquireTimeout)
	}
}

func TestFaultInjectionIsDevelopmentOnly(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	t.Setenv("FAULT_REDIS_DELAY_MS", "125")
	t.Setenv("FAULT_DB_DELAY_MS", "250")
	t.Setenv("FAULT_CONSUMER_DELAY_MS", "300")
	t.Setenv("FAULT_ETCD_DELAY_MS", "400")
	dev, err := Load("gateway")
	if err != nil {
		t.Fatal(err)
	}
	if dev.Faults.RedisDelay.Milliseconds() != 125 || dev.Faults.DBDelay.Milliseconds() != 250 || dev.Faults.ConsumerDelay.Milliseconds() != 300 || dev.Faults.EtcdDelay.Milliseconds() != 400 {
		t.Fatalf("development fault delays not loaded: %+v", dev.Faults)
	}

	t.Setenv("APP_ENV", "production")
	prod, err := Load("gateway")
	if err != nil {
		t.Fatal(err)
	}
	if prod.Faults != (FaultConfig{}) {
		t.Fatalf("fault injection active outside development: %+v", prod.Faults)
	}
}
