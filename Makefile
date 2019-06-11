all: build execute

build:
	go build

run:
	go run modb.go

execute:
	./modb

list-modules:
	go list -m all
	# To update a module, do `go get github.com/chilts/sid`

clean:
	rm -rf data/*
	rm -f modb
