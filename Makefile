.PHONY: test simulate coordinator worker fmt

test:
	go test ./...

simulate:
	go run ./cmd/simulator -tasks 1000 -workers 32 -fault-every 25

coordinator:
	go run ./cmd/coordinator

worker:
	go run ./cmd/worker

fmt:
	gofmt -w ./cmd ./internal
