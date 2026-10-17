DATABASE_URL ?= postgres://brokerd:brokerd@localhost:5432/brokerd?sslmode=disable
export DATABASE_URL

.PHONY: build test test-short bench lint db-up db-down migrate reconcile settle

build:
	go build ./...

test:
	go test -race ./...

test-short:
	go test -race -short ./...

bench:
	go test -run='^$$' -bench=. -benchmem ./...

lint:
	golangci-lint run ./...

db-up:
	docker compose up -d --wait postgres

db-down:
	docker compose down -v

migrate:
	go run ./cmd/brokerd migrate

reconcile:
	go run ./cmd/brokerd reconcile

settle:
	go run ./cmd/brokerd settle
