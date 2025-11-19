BUILD_DIR := bin
BINARY := $(BUILD_DIR)/kriminoDB
SRC := ./cmd/kriminodb/main.go

.PHONY: build run node1 node2 clean

build:
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BINARY) $(SRC)
	@echo "Built to $(BINARY)"

run: build
	@$(BINARY) --client-port 3000 --peer-port 4000
	@echo "Running single node"

node1: build
	@$(BINARY) --client-port 3000 --peer-port 4000
	@echo "Node 1 running (client:3000, gossip:4000)"

node2: build
	@$(BINARY) --client-port 3001 --peer-port 4001 --bootstrap localhost:4000
	@echo "Node 2 running (client:3001, gossip:4001) → bootstrapping to node1"

test:
	@go test ./... -v

clean:
	@rm -rf $(BUILD_DIR)
	@echo "Cleaned build artifacts"
