

DIR = ./cmd/tdb
TARGET = out

$(TARGET):
	go build -o $(TARGET) $(DIR)

start: 
	go run $(DIR)

test:
	go test ./pkg
	node --test

run:
	air -- -m

clean:
	rm -f $(TARGET)

check:
	go run $(DIR) -check


