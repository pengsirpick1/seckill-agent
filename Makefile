COMPOSE := docker compose

.PHONY: up down build logs ps test test-race test-integration migrate config

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

build:
	$(COMPOSE) build gateway go-test

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

config:
	$(COMPOSE) config -q

test:
	$(COMPOSE) run --rm --no-deps go-test sh -c 'go mod tidy && go test ./...'

test-race:
	$(COMPOSE) run --rm --no-deps go-test sh -c 'go mod tidy && go test -race ./...'

migrate:
	$(COMPOSE) run --rm migrate

test-integration:
	$(COMPOSE) run --rm --no-deps -e MYSQL_DSN='seckill:development-seckill-password@tcp(mysql:3306)/seckill?parseTime=true&charset=utf8mb4&loc=UTC' go-test sh -c 'go test -tags=integration -race ./tests/integration'
