-include .env

.PHONY: migrate_up migrate_down migrate_status run proto proto-lint

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