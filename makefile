MAIN = ./cmd/tdb
DIRS = ./internal/** ./pkg ./tools/generate
TARGET = out

all: 
	go build -o $(TARGET)-tdb $(MAIN)

build $(PKG):
	go build -o $(TARGET)-$(PKG) ./cmd/$(PKG)

install:
	go install -ldflags="-X 'main.version=$(shell date)'" $(MAIN) 

clean:
	rm -f $(TARGET)

run: 
	go run $(MAIN) -u user -p pass -log -dbg -db ./db.tdb

dev:
	air -- -m -log -dbg -u user -p pass

check:
	go vet $(MAIN) $(DIRS)

test-unit:
	go test -v $(PKG) $(DIRS)

test-e2e:
	node ./tests/test.mjs

client-js-test:
	cd ./tools/client/js && pnpm test

run-docs:
	mkdocs serve
