#include "graphdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cypher_parser.h"

// Example usage
int main() {
    GraphDB* gdb = graphdb_open("./graphdb");
    if (!gdb) return 1;
    graphdb_add_node(gdb, "node1", "Person");
    graphdb_add_node(gdb, "node2", "Person");
    graphdb_add_node(gdb, "node3", "Person");
    graphdb_add_node(gdb, "node4", "Person");
    graphdb_add_node(gdb, "node5", "Person");
    graphdb_add_node(gdb, "node5", "Person");


    graphdb_add_edge(gdb, "node1", "node2", "FRIEND");  
    graphdb_add_edge(gdb, "node1", "node3", "FRIEND");
    graphdb_add_edge(gdb, "node1", "node4", "PARENT");

    graphdb_add_edge(gdb, "node2", "node3", "FRIEND");
    graphdb_add_edge(gdb, "node3", "node4", "FRIEND");
    graphdb_add_edge(gdb, "node4", "node5", "FRIEND");

    graphdb_add_node(gdb, "Mark", "Person");
    graphdb_add_node(gdb, "Alex", "Person");
    graphdb_add_node(gdb, "Felipe", "Person");
    graphdb_add_node(gdb, "research@felipebonetto.com", "Email");
    graphdb_add_node(gdb, "Mark", "Person");
    graphdb_add_edge(gdb, "Mark", "Alex", "FRIEND");
    graphdb_add_edge(gdb, "Mark", "Felipe", "FRIEND");
    graphdb_add_edge(gdb, "Alex", "Felipe", "FRIEND");
    graphdb_add_edge(gdb, "Felipe", "Mark", "UNCLE");
    graphdb_add_edge(gdb, "Felipe", "Alex", "COUSIN");
    graphdb_add_edge(gdb, "Felipe", "research@felipebonetto.com", "CONTACT_INFO");


    int count;
    Neighbor* neighbors = graphdb_get_outgoing(gdb, "node1", "FRIEND", &count);
    if (neighbors) {
        for (int i = 0; i < count; i++) {
            printf("Neighbor: %s\n", neighbors[i].id);
            free(neighbors[i].id);
            free(neighbors[i].type);
        }
        free(neighbors);
    }

    // Find shortest path from node1 to node5
    find_shortest_path(gdb, "node1", "node5", "FRIEND");

    // Example of new API usage
    char* label = graphdb_get_node_label(gdb, "node1");
    if (label) {
        printf("Label of node1: %s\n", label);
        free(label);
    }

    int inc_count;
    Neighbor* inc_neighbors = graphdb_get_incoming(gdb, "node3", "FRIEND", &inc_count);
    if (inc_neighbors) {
        printf("Incoming to node3:\n");
        for (int i = 0; i < inc_count; i++) {
            printf("Incoming: %s\n", inc_neighbors[i].id);
            free(inc_neighbors[i].id);
            free(inc_neighbors[i].type);
        }
        free(inc_neighbors);
    }

    // Basic Cypher example
    CypherResult* res = execute_cypher(gdb, "MATCH (a)-[:FRIEND]->(b) WHERE a.id = 'node1' RETURN b.id");
    print_cypher_result(res);
    free_cypher_result(res);

    // Delete edge example
    graphdb_delete_edge(gdb, "node1", "node3", "FRIEND");

    // Shortest path after deletion
    find_shortest_path(gdb, "node1", "node5", "FRIEND");

    // Restore the edge for consistency
    graphdb_add_edge(gdb, "node1", "node3", "FRIEND");

    graphdb_close(gdb);
    return 0;
}