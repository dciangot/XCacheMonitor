VERSION=`git describe --tags`
BUILD_DATE := `date +%Y-%m-%d\ %H:%M`
VERSIONFILE := version.go

GOCMD=go
GOBUILD=$(GOCMD) build -x -ldflags "-w -v"
GOBUILD_DBG=$(GOCMD) build -x
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=cache-sensor
REPO=github.com/Cloud-PG/CacheReportSensor

export GO111MODULE=on
# Force 64 bit architecture
export GOARCH=amd64

all: build test

build:
	$(GOBUILD) -o $(BINARY_NAME)

build-debug:
	$(GOBUILD_DBG) -o $(BINARY_NAME) -v

doc:
	cp README.md docs/README.md

test: build
	$(GOTEST) -v ./...

docker-test: docker-img-build
	docker run -e COLLECTOR_PORT=1294 cachereport

clean: 
	$(GOCLEAN)
	rm -f $(BINARY_NAME)

run: build
	./$(BINARY_NAME)

install:
	$(GOCMD) install $(REPO)

tidy:
	$(GOCMD) mod tidy

docker-bin-build:
	docker run --rm -it -v "$(GOPATH)":/go -w /go/src/$(REPO) golang:1.12.1 go build -o "$(BINARY_NAME)" -v

docker-img-build:
	docker build . -t cachereport

windows-build:
	env GOOS=windows $(GOBUILD) -o $(BINARY_NAME).exe -v

macos-build:
	env GOOS=darwin $(GOBUILD) -o $(BINARY_NAME)_osx -v

gensrc:
	rm -f $(VERSIONFILE)
	@echo "package main" > $(VERSIONFILE)
	@echo "const (" >> $(VERSIONFILE)
	@echo "  VERSION = \"$(VERSION)\"" >> $(VERSIONFILE)
	@echo "  BUILD_DATE = \"$(BUILD_DATE)\"" >> $(VERSIONFILE)
	@echo ")" >> $(VERSIONFILE)

build-release: tidy gensrc build doc test windows-build macos-build docker-img-build
	zip dodas.zip dodas
	zip dodas.exe.zip dodas.exe
	zip dodas_osx.zip dodas_osx
