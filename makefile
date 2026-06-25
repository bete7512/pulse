-include .env

DOCKER_IMAGE ?= $(DOCKERHUB_USERNAME)/pulse

.PHONY: migrate_up migrate_down migrate_status run proto proto-lint generate-mock test docker-build

generate-mock:
	@echo "Generating mocks..."
	@go generate ./...
test:
	@echo "Running tests..."
	@go test -race ./...

migrate_up:
	@echo "Running database migrations..."
	@go run cmd/migrate/main.go up
migrate_down:
	@echo "Rolling back database migrations..."
	@go run cmd/migrate/main.go down
migrate_status:
	@echo "Checking migration status..."
	@go run cmd/migrate/main.go status
run:
	@echo "Starting the application..."
	@go run cmd/pulsed/main.go
proto:
	@echo "Generating gRPC code from .proto files..."
	@buf generate proto
proto-lint:
	@echo "Linting .proto files..."
	@buf lint proto
docker-build:
	@echo "Building image $(DOCKER_IMAGE):dev ..."
	@docker build -t $(DOCKER_IMAGE):dev .