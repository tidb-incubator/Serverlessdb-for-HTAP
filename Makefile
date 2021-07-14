# Set DEBUGGER=1 to build debug symbols
LDFLAGS = $(if $(DEBUGGER),,-s -w) $(shell ./hack/version.sh)

GOVER_MAJOR := $(shell go version | sed -E -e "s/.*go([0-9]+)[.]([0-9]+).*/\1/")
GOVER_MINOR := $(shell go version | sed -E -e "s/.*go([0-9]+)[.]([0-9]+).*/\2/")
GO113 := $(shell [ $(GOVER_MAJOR) -gt 1 ] || [ $(GOVER_MAJOR) -eq 1 ] && [ $(GOVER_MINOR) -ge 13 ]; echo $$?)
ifeq ($(GO113), 1)
$(error Please upgrade your Go compiler to 1.13 or higher version)
endif

# Enable GO111MODULE=on explicitly, disable it with GO111MODULE=off when necessary.
export GO111MODULE := on
GOOS := $(if $(GOOS),$(GOOS),linux)
GOARCH := $(if $(GOARCH),$(GOARCH),amd64)
GOENV  := GO15VENDOREXPERIMENT="1" CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH)
GO     := $(GOENV) go
GO_BUILD := $(GO) build -trimpath

DOCKER_REGISTRY ?= localhost:5000
DOCKER_REPO ?= ${DOCKER_REGISTRY}/serverlessdb-for-htap
#IMAGE_TAG ?= latest
IMAGE_TAG ?= v1.0.0

default: build


ifeq ($(NO_BUILD),y)
docker:
	@echo "NO_BUILD=y, skip build for $@"
else
docker: build
endif
	docker build --tag "${DOCKER_REPO}/scale-operator:${IMAGE_TAG}" images/scale-operator
	docker build --tag "${DOCKER_REPO}/serverlessdb-operator:${IMAGE_TAG}" images/sldb-operator

build: scale-operator sldb-operator

scale-operator:
	$(GO_BUILD) -ldflags '$(LDFLAGS)' -o images/scale-operator/bin/scale-operator cmd/scale-operator/main.go

sldb-operator:
	$(GO_BUILD) -ldflags '$(LDFLAGS)' -o images/sldb-operator/bin/serverless-operator cmd/sldb-operator/main.go


