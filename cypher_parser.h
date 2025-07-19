#ifndef CYPHER_PARSER_H
#define CYPHER_PARSER_H

#include "graphdb.h"

// Structured result types (see comment block earlier)

typedef struct {
    char* var;   // Variable alias used in the query (may be NULL)
    char* id;    // Node identifier
    char* label; // Node label/category
} CypherNodeResult;

typedef struct {
    char* var;     // Relationship alias (may be NULL)
    char* from_id; // Source node id
    char* to_id;   // Destination node id
    char* type;    // Relationship type
} CypherEdgeResult;

typedef struct {
    CypherNodeResult* nodes;
    int node_count;
    CypherEdgeResult* edges;
    int edge_count;
} CypherRowResult;

typedef struct {
    CypherRowResult* rows;
    int row_count;
} CypherResult;

// Core API
CypherResult* execute_cypher(GraphDB* gdb, const char* query);
void free_cypher_result(CypherResult* result);

// Convenience utility for CLI output
void print_cypher_result(const CypherResult* result);

// Convenience utility for D3.js integration
char* cypher_result_to_d3_json(const CypherResult* result);
void free_d3_json(char* json);

#endif 