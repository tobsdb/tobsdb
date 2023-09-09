

DIR = ./cmd/tdb
TARGET = out

test:
	go test ./pkg
	node --test

run:
	go run $(DIR) -m

build:
	go build -o $(TARGET) $(DIR)

clean:
	rm -f $(TARGET)

check:
	go run $(DIR) -check


