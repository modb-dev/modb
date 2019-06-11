build:
	go build cmd/modb/modb.go cmd/modb/commands.go cmd/modb/client.go

run:
	go run modb.go

modb: build
	./modb

modb-server: build
	./modb server data/bbolt.db

modb-server-bbolt: build
	./modb server --datastore bbolt data/bbolt.db

modb-server-badger: build
	./modb server --datastore badger data/badger.db

modb-help: build
	@echo "-------------------------------------------------------------------------------"
	./modb help
	@echo "-------------------------------------------------------------------------------"
	./modb --help
	@echo "-------------------------------------------------------------------------------"

list-modules:
	go list -m all
	# To update a module, do `go get github.com/chilts/sid`

clean:
	rm -rf data/*
	rm -f modb

.PHONY: build
