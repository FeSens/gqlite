#include "graphdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int main() {
    srand(time(NULL));

    GraphDB* gdb = graphdb_open("./benchmarkdb");
    if (!gdb) return 1;

    const int NUM_NODES = 1000000;
    const int NUM_EDGES = 3500000;
    const char* EDGE_TYPE = "FRIEND";

    // Benchmark node inserts
    clock_t start = clock();
    for (int i = 0; i < NUM_NODES; i++) {
        char node_id[16];
        sprintf(node_id, "node%d", i);
        graphdb_add_node(gdb, node_id, "Node");
    }
    clock_t end = clock();
    double node_time = (double)(end - start) / CLOCKS_PER_SEC;
    printf("Time to insert %d nodes: %f seconds\n", NUM_NODES, node_time);

    // Benchmark edge inserts
    start = clock();
    for (int i = 0; i < NUM_EDGES; i++) {
        int from_idx = rand() % NUM_NODES;
        int to_idx = rand() % NUM_NODES;
        char from[16], to[16];
        sprintf(from, "node%d", from_idx);
        sprintf(to, "node%d", to_idx);
        graphdb_add_edge(gdb, from, to, EDGE_TYPE);
    }
    end = clock();
    double edge_time = (double)(end - start) / CLOCKS_PER_SEC;
    printf("Time to insert %d edges: %f seconds\n", NUM_EDGES, edge_time);

    // Benchmark shortest path (from node0 to a random node)
    char start_node[16] = "node0";
    int random_end = rand() % NUM_NODES;
    char end_node[16];
    sprintf(end_node, "node%d", random_end);

    start = clock();
    find_shortest_path(gdb, start_node, end_node, EDGE_TYPE);
    end = clock();
    double path_time = (double)(end - start) / CLOCKS_PER_SEC;
    printf("Time to find shortest path from %s to %s: %f seconds\n", start_node, end_node, path_time);

    graphdb_close(gdb);
    return 0;
} 