all: build execute

build:
	go build

run:
	go run modb.go

execute:
	./modb
