// config 包加载所有服务共用的环境变量配置。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName     string
	AppEnv          string
	HTTPAddr        string
	LogLevel        string
	ShutdownTimeout time.Duration
	DependencyProbe time.Duration
	MySQL           MySQLConfig
	Redis           RedisConfig
	Kafka           KafkaConfig
	Etcd            EtcdConfig
	Observability   ObservabilityConfig
	Seckill         SeckillConfig
	RateLimit       RateLimitConfig
	Order           OrderConfig
	Outbox          OutboxConfig
	Release         ReleaseConfig
	Faults          FaultConfig
}

type MySQLConfig struct {
	DSN             string
	Address         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

type RedisConfig struct {
	Address  string
	Password string
	DB       int
	PoolSize int
}

type KafkaConfig struct {
	Brokers             []string
	OrderTopic          string
	OrderSucceededTopic string
	ConsumerGroup       string
	RankConsumerGroup   string
	ConsumerPollRecords int
}

type EtcdConfig struct {
	Endpoints          []string
	DialTimeout        time.Duration
	LockSessionTTL     time.Duration
	LockAcquireTimeout time.Duration
}

type ObservabilityConfig struct {
	PrometheusAddr string
	OTLPEndpoint   string
}

type SeckillConfig struct {
	LockMode string
}

type RateLimitConfig struct {
	LocalRate    int
	LocalBurst   int
	GlobalRate   int
	GlobalBurst  int
	ProductRate  int
	ProductBurst int
	UserRate     int
	UserBurst    int
}

type OrderConfig struct {
	NodeID           int
	CancelAPIEnabled bool
}

type OutboxConfig struct {
	ConsumerGroup string
	ConsumerName  string
	ClaimIdle     time.Duration
	PollInterval  time.Duration
	BatchSize     int
}

type ReleaseConfig struct {
	PollInterval time.Duration
	Lease        time.Duration
}

type FaultConfig struct {
	RedisDelay    time.Duration
	DBDelay       time.Duration
	ConsumerDelay time.Duration
	EtcdDelay     time.Duration
}

func Load(service string) (Config, error) {
	if strings.TrimSpace(service) == "" {
		return Config{}, fmt.Errorf("service name is required")
	}
	appEnv := stringEnv("APP_ENV", "development")
	cfg := Config{
		ServiceName:     service,
		AppEnv:          appEnv,
		HTTPAddr:        stringEnv("HTTP_ADDR", defaultHTTPAddr(service)),
		LogLevel:        stringEnv("LOG_LEVEL", "info"),
		ShutdownTimeout: durationEnv("SHUTDOWN_TIMEOUT", 10*time.Second),
		DependencyProbe: durationEnv("DEPENDENCY_PROBE_TIMEOUT", 750*time.Millisecond),
		MySQL: MySQLConfig{
			DSN:             stringEnv("MYSQL_DSN", "seckill:development-seckill-password@tcp(mysql:3306)/seckill?parseTime=true&charset=utf8mb4&loc=UTC"),
			Address:         stringEnv("MYSQL_ADDRESS", "mysql:3306"),
			MaxOpenConns:    intEnv("MYSQL_MAX_OPEN_CONNS", 30),
			MaxIdleConns:    intEnv("MYSQL_MAX_IDLE_CONNS", 10),
			ConnMaxLifetime: durationEnv("MYSQL_CONN_MAX_LIFETIME", 5*time.Minute),
			ConnMaxIdleTime: durationEnv("MYSQL_CONN_MAX_IDLE_TIME", 1*time.Minute),
		},
		Redis: RedisConfig{
			Address:  stringEnv("REDIS_ADDRESS", "redis:6379"),
			Password: os.Getenv("REDIS_PASSWORD"),
			DB:       intEnv("REDIS_DB", 0),
			PoolSize: intEnv("REDIS_POOL_SIZE", 30),
		},
		Kafka: KafkaConfig{
			Brokers:             csvEnv("KAFKA_BROKERS", "kafka:9092"),
			OrderTopic:          stringEnv("KAFKA_ORDER_TOPIC", "seckill.order-created.v1"),
			OrderSucceededTopic: stringEnv("KAFKA_ORDER_SUCCEEDED_TOPIC", "seckill.order-succeeded.v1"),
			ConsumerGroup:       stringEnv("KAFKA_CONSUMER_GROUP", "seckill-order-consumer"),
			RankConsumerGroup:   stringEnv("KAFKA_RANK_CONSUMER_GROUP", "seckill-rank-consumer"),
			ConsumerPollRecords: intEnv("KAFKA_CONSUMER_POLL_RECORDS", 1),
		},
		Etcd: EtcdConfig{
			Endpoints:          csvEnv("ETCD_ENDPOINTS", "etcd:2379"),
			DialTimeout:        durationEnv("ETCD_DIAL_TIMEOUT", 2*time.Second),
			LockSessionTTL:     durationEnv("ETCD_LOCK_SESSION_TTL", 10*time.Second),
			LockAcquireTimeout: durationEnv("ETCD_LOCK_ACQUIRE_TIMEOUT", 2*time.Second),
		},
		Observability: ObservabilityConfig{
			PrometheusAddr: stringEnv("PROMETHEUS_ADDR", ":9090"),
			OTLPEndpoint:   stringEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "jaeger:4318"),
		},
		Seckill: SeckillConfig{LockMode: stringEnv("SECKILL_LOCK_MODE", "redis_lua_only")},
		RateLimit: RateLimitConfig{
			LocalRate:    intEnv("LOCAL_LIMIT_RATE", 1000),
			LocalBurst:   intEnv("LOCAL_LIMIT_BURST", 1000),
			GlobalRate:   intEnv("REDIS_LIMIT_GLOBAL_RATE", 500),
			GlobalBurst:  intEnv("REDIS_LIMIT_GLOBAL_BURST", 500),
			ProductRate:  intEnv("REDIS_LIMIT_PRODUCT_RATE", 200),
			ProductBurst: intEnv("REDIS_LIMIT_PRODUCT_BURST", 200),
			UserRate:     intEnv("REDIS_LIMIT_USER_RATE", 5),
			UserBurst:    intEnv("REDIS_LIMIT_USER_BURST", 5),
		},
		Order: OrderConfig{NodeID: intEnv("ORDER_ID_NODE_ID", 1), CancelAPIEnabled: boolEnv("ORDER_CANCEL_API_ENABLED", false)},
		Outbox: OutboxConfig{
			ConsumerGroup: stringEnv("RESERVATION_OUTBOX_CONSUMER_GROUP", "seckill-reservation-outbox"),
			ConsumerName:  stringEnv("RESERVATION_OUTBOX_CONSUMER_NAME", outboxConsumerName(service)),
			ClaimIdle:     durationEnv("RESERVATION_OUTBOX_CLAIM_IDLE", 30*time.Second),
			PollInterval:  durationEnv("RESERVATION_OUTBOX_POLL_INTERVAL", 200*time.Millisecond),
			BatchSize:     intEnv("RESERVATION_OUTBOX_BATCH_SIZE", 100),
		},
		Release: ReleaseConfig{
			PollInterval: durationEnv("INVENTORY_RELEASE_POLL_INTERVAL", 250*time.Millisecond),
			Lease:        durationEnv("INVENTORY_RELEASE_LEASE", 30*time.Second),
		},
		Faults: FaultConfig{
			RedisDelay:    millisEnv("FAULT_REDIS_DELAY_MS"),
			DBDelay:       millisEnv("FAULT_DB_DELAY_MS"),
			ConsumerDelay: millisEnv("FAULT_CONSUMER_DELAY_MS"),
			EtcdDelay:     millisEnv("FAULT_ETCD_DELAY_MS"),
		},
	}
	if cfg.AppEnv != "development" {
		cfg.Faults = FaultConfig{}
	}
	if cfg.Seckill.LockMode != "redis_lua_only" && cfg.Seckill.LockMode != "etcd" {
		return Config{}, fmt.Errorf("SECKILL_LOCK_MODE must be redis_lua_only or etcd, got %q", cfg.Seckill.LockMode)
	}
	if strings.TrimSpace(cfg.Outbox.ConsumerGroup) == "" || strings.TrimSpace(cfg.Outbox.ConsumerName) == "" {
		return Config{}, fmt.Errorf("reservation outbox consumer group and name are required")
	}
	return cfg, nil
}

func outboxConsumerName(service string) string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return service + "-1"
	}
	return service + "-" + host
}

func defaultHTTPAddr(service string) string {
	if service == "order-consumer" {
		return ":8082"
	}
	if service == "rank-consumer" {
		return ":8083"
	}
	if service == "outbox-publisher" {
		return ":8084"
	}
	if service == "release-worker" {
		return ":8085"
	}
	return ":8080"
}

func stringEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func csvEnv(key, fallback string) []string {
	parts := strings.Split(stringEnv(key, fallback), ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func intEnv(key string, fallback int) int {
	if value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key))); err == nil && value > 0 {
		return value
	}
	return fallback
}

func boolEnv(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	if value, err := time.ParseDuration(strings.TrimSpace(os.Getenv(key))); err == nil && value > 0 {
		return value
	}
	return fallback
}

func millisEnv(key string) time.Duration {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return 0
	}
	return time.Duration(value) * time.Millisecond
}
