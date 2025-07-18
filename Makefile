CC = clang
INCLUDES = -I/opt/homebrew/opt/rocksdb/include
LIBS = -L/opt/homebrew/opt/rocksdb/lib -lrocksdb

all: graph benchmark

graph: main.o graphdb.o
	$(CC) main.o graphdb.o $(LIBS) -o graph

benchmark: benchmark.o graphdb.o
	$(CC) benchmark.o graphdb.o $(LIBS) -o benchmark

main.o: main.c graphdb.h
	$(CC) -c main.c $(INCLUDES)

benchmark.o: benchmark.c graphdb.h
	$(CC) -c benchmark.c $(INCLUDES)

graphdb.o: graphdb.c graphdb.h
	$(CC) -c graphdb.c $(INCLUDES)

clean:
	rm -f *.o graph benchmark 