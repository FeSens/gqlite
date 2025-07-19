#ifndef GRAPHDB_H
#define GRAPHDB_H

#include <rocksdb/c.h>

typedef struct GraphDB {
    rocksdb_t *db;
    rocksdb_options_t *options;
    rocksdb_block_based_table_options_t *table_options;
    rocksdb_cache_t *cache;
    rocksdb_writeoptions_t *writeoptions;
    rocksdb_readoptions_t *readoptions;
} GraphDB;

typedef struct {
  char* id;
  char* type;
} Neighbor;

GraphDB* graphdb_open(const char* path);
void graphdb_close(GraphDB* gdb);
void graphdb_add_node(GraphDB* gdb, const char* node_id, const char* label);
void graphdb_add_edge(GraphDB* gdb, const char* from, const char* to, const char* type);
Neighbor* graphdb_get_outgoing(GraphDB* gdb, const char* node, const char* type, int* count);
Neighbor* graphdb_get_incoming(GraphDB* gdb, const char* node, const char* type, int* count);
char* graphdb_get_node_label(GraphDB* gdb, const char* node_id);
char** graphdb_get_nodes_by_label(GraphDB* gdb, const char* label, int* count);
char** graphdb_get_all_nodes(GraphDB* gdb, int* count);
void graphdb_delete_node(GraphDB* gdb, const char* node_id);
void graphdb_delete_edge(GraphDB* gdb, const char* from, const char* to, const char* type);
void find_shortest_path(GraphDB* gdb, const char* start, const char* end, const char* type);

#endif 