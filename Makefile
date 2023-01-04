all: build test

build:
	go build -v

test:
	go test -covermode=count -coverprofile=coverage.out -v

race-test:
	go test -race -v

docker-test:
	$(eval WORKDIR := /go/src/github.com/everoute/libovsdb)
	docker run --rm -iu 0:0 -w $(WORKDIR) -v $(CURDIR):$(WORKDIR) -v /lib/modules:/lib/modules --privileged everoute/unit-test make test

docker-race-test:
	$(eval WORKDIR := /go/src/github.com/everoute/libovsdb)
	docker run --rm -iu 0:0 -w $(WORKDIR) -v $(CURDIR):$(WORKDIR) -v /lib/modules:/lib/modules --privileged everoute/unit-test make race-test

