migrate:
	go tool migrate create -dir migrations -ext sql $(name)

test:
	go test -race ./...
