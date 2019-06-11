all: build execute

build:
	go build cmd/modb/modb.go cmd/modb/commands.go cmd/modb/client.go

run:
	go run modb.go

execute:
	./modb server

list-modules:
	go list -m all
	# To update a module, do `go get github.com/chilts/sid`

clean:
	rm -rf data/*
	rm -f modb
