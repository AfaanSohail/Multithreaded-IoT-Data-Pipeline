CC = g++
CFLAGS = -Wall -Wextra -pthread
LDFLAGS = -lrt

all: src/dispatcher src/ingester src/processor src/reporter

src/dispatcher: src/dispatcher.cpp
	$(CC) $(CFLAGS) -o src/dispatcher src/dispatcher.cpp $(LDFLAGS)

src/ingester: src/ingester.cpp
	$(CC) $(CFLAGS) -o src/ingester src/ingester.cpp $(LDFLAGS)

src/processor: src/processor.cpp
	$(CC) $(CFLAGS) -o src/processor src/processor.cpp $(LDFLAGS)

src/reporter: src/reporter.cpp
	$(CC) $(CFLAGS) -o src/reporter src/reporter.cpp $(LDFLAGS)

clean:
	rm -f src/dispatcher src/ingester src/processor src/reporter
	rm -f logs/*