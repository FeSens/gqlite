CC = clang
INCLUDES = -I/opt/homebrew/opt/rocksdb/include
LIBS = -L/opt/homebrew/opt/rocksdb/lib -lrocksdb

all: graph benchmark gqlite_cli

graph: main.o graphdb.o cypher_parser.o
	$(CC) main.o graphdb.o cypher_parser.o $(LIBS) -o graph

benchmark: benchmark.o graphdb.o
	$(CC) benchmark.o graphdb.o $(LIBS) -o benchmark

gqlite_cli: cli.o graphdb.o cypher_parser.o
	$(CC) cli.o graphdb.o cypher_parser.o $(LIBS) -o gqlite_cli

cli.o: cli.c graphdb.h cypher_parser.h
	$(CC) -c cli.c $(INCLUDES)

main.o: main.c graphdb.h
	$(CC) -c main.c $(INCLUDES)

benchmark.o: benchmark.c graphdb.h
	$(CC) -c benchmark.c $(INCLUDES)

graphdb.o: graphdb.c graphdb.h
	$(CC) -c graphdb.c $(INCLUDES)

cypher_parser.o: cypher_parser.c cypher_parser.h
	$(CC) -c cypher_parser.c $(INCLUDES)

test: test_graphdb test_cypher_parser

run_tests: test
	./test/test_graphdb
	./test/test_cypher_parser

test_graphdb: test/test_graphdb.o graphdb.o test/unity/src/unity.o
	$(CC) test/test_graphdb.o graphdb.o test/unity/src/unity.o $(LIBS) -o test/test_graphdb

test_cypher_parser: test/test_cypher_parser.o cypher_parser.o graphdb.o test/unity/src/unity.o
	$(CC) test/test_cypher_parser.o cypher_parser.o graphdb.o test/unity/src/unity.o $(LIBS) -o test/test_cypher_parser

test/test_graphdb.o: test/test_graphdb.c
	$(CC) -c test/test_graphdb.c -o test/test_graphdb.o $(INCLUDES) -I test/unity/src

test/test_cypher_parser.o: test/test_cypher_parser.c
	$(CC) -c test/test_cypher_parser.c -o test/test_cypher_parser.o $(INCLUDES) -I test/unity/src

test/unity/src/unity.o: test/unity/src/unity.c
	$(CC) -c test/unity/src/unity.c -o test/unity/src/unity.o -I test/unity/src

clean:
	rm -f *.o graph benchmark gqlite_cli test/*.o test/test_graphdb test/test_cypher_parser test/unity/src/unity.o 