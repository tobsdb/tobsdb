

DIR = ./cmd/tdb
TARGET = out

test:
	node --test

run:
	go run $(DIR) -m

build:
	go build -o $(TARGET) $(DIR)

clean:
	rm -f $(TARGET)


