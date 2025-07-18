#include "graphdb.h"
#include "cypher_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BUFFER_SIZE 1024

int main(int argc, char** argv) {
    const char* db_path = (argc > 1) ? argv[1] : "./graphdb";
    GraphDB* gdb = graphdb_open(db_path);
    if (!gdb) {
        fprintf(stderr, "Failed to open database at %s\n", db_path);
        return 1;
    }

    printf("GQLite CLI - Enter Cypher queries (type 'exit' to quit)\n");
    char buffer[BUFFER_SIZE];
    while (1) {
        printf("> ");
        if (!fgets(buffer, BUFFER_SIZE, stdin)) break;
        buffer[strcspn(buffer, "\n")] = 0;  // Remove newline
        if (strcmp(buffer, "exit") == 0 || strcmp(buffer, "quit") == 0) break;
        if (strlen(buffer) == 0) continue;

        CypherResult* res = execute_cypher(gdb, buffer);
        print_cypher_result(res);
        free_cypher_result(res);
    }

    graphdb_close(gdb);
    return 0;
} 