MAIN = ./cmd/tdb
INTERNAL = ./internal
PKG = ./pkg
TARGET = out

all: build

build:
	go build -o $(TARGET) $(MAIN)

clean:
	rm -f $(TARGET)

run: 
	go run $(MAIN) -u user -p pass -log -dbg -db ./db.tdb

dev:
	air -- -m -log -dbg -u user -p pass

check:
	go vet $(MAIN) $(INTERNAL)/** $(PKG)

test-unit:
	go test $(PKG) $(INTERNAL)/**

test-e2e:
	node ./tests/test.mjs

client-js-test:
	cd ./client/js &&  pnpm test
