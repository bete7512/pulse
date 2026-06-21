-include .env

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
	@go run -race cmd/pulsed/main.go