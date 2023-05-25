# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get

# Main package name
MAIN_PACKAGE=./cmd/dbq_server.go

# Output binary name
BINARY_NAME=dbq_server

all: build

build:
	$(GOBUILD) -o $(BINARY_NAME) $(MAIN_PACKAGE)

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)

run:
	$(GOBUILD) -o $(BINARY_NAME) $(MAIN_PACKAGE)
	./$(BINARY_NAME)

test:
	$(GOTEST) -v ./...

get:
	$(GOGET) ./...

.PHONY: all build clean run test get

