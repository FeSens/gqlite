#ifndef CYPHER_PARSER_H
#define CYPHER_PARSER_H

#include "graphdb.h"

typedef struct {
    char** columns;
    int column_count;
    char*** rows;
    int row_count;
} CypherResult;

CypherResult* execute_cypher(GraphDB* gdb, const char* query);
void free_cypher_result(CypherResult* result);
void print_cypher_result(CypherResult* result);

#endif 