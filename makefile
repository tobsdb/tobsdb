

DIR = ./cmd/tdb
API_DIR = ./cmd/api
TARGET = out

$(TARGET):
	go build -o $(TARGET) $(DIR)

start: 
	go run $(DIR)

test:
	go test ./pkg
	node ./tests/test.mjs

run:
	air -- -m -log -dbg

clean:
	rm -f $(TARGET)

check:
	go run $(DIR) -check

api:
	go run $(API_DIR)
