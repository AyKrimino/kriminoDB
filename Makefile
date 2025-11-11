build:
	@go build -o bin/kriminoDB ./cmd/kriminodb/main.go

run: build
	@./bin/kriminoDB

test:
	@go test ./... -v
