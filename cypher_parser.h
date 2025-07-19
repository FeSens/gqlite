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

// Add structured result definitions for nodes and edges that decouple query execution from presentation

// Forward-declaration of GraphDB to avoid circular includes
struct GraphDB;

// Basic node representation produced by the executor
typedef struct {
    char* var;   // Variable name used in the Cypher query (may be NULL)
    char* id;    // Node identifier
    char* label; // Node label (category)
} CypherNodeResult;

// Basic edge representation produced by the executor
typedef struct {
    char* var;     // Relationship variable name (may be NULL)
    char* from_id; // Source node id
    char* to_id;   // Destination node id
    char* type;    // Relationship type string
} CypherEdgeResult;

// One logical record (row) returned by the executor in structured form
typedef struct {
    CypherNodeResult* nodes;
    int node_count;
    CypherEdgeResult* edges;
    int edge_count;
} CypherRawRow;

// Full structured result that is implementation agnostic and can be
// post-processed into different presentation formats (CLI table, JSON, etc.)
typedef struct {
    CypherRawRow* rows;
    int row_count;
} CypherRawResult;

// Helper APIs
CypherRawResult* execute_cypher_raw(GraphDB* gdb, const char* query);
void free_cypher_raw_result(CypherRawResult* result);

// Convert a structured result into the legacy tabular representation that the
// current CLI expects. This keeps existing callers working while allowing new
// integrations to rely on the richer CypherRawResult structure.
CypherResult* cypher_raw_to_table(GraphDB* gdb, const CypherRawResult* raw, const char* query);

#endif 