#include "../graphdb.h"
#include "unity/src/unity.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h> // for rmdir, etc.
#include <rocksdb/c.h>
#include <dirent.h>
#include <sys/stat.h>

#define TEST_DB_PATH "./testdb_temp"

GraphDB* gdb;

static void remove_directory(const char* path) {
    DIR* dir = opendir(path);
    if (!dir) return;
    struct dirent* entry;
    char full_path[1024];
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;
        snprintf(full_path, sizeof(full_path), "%s/%s", path, entry->d_name);
        struct stat statbuf;
        stat(full_path, &statbuf);
        if (S_ISDIR(statbuf.st_mode)) {
            remove_directory(full_path);
        } else {
            unlink(full_path);
        }
    }
    closedir(dir);
    rmdir(path);
}

void setUp(void) {
    // Create a temporary database
    gdb = graphdb_open(TEST_DB_PATH);
    TEST_ASSERT_NOT_NULL(gdb);
}

void tearDown(void) {
    graphdb_close(gdb);
    remove_directory(TEST_DB_PATH);
}

void test_graphdb_open_close(void) {
    // Already opened in setUp, check if db is not null
    TEST_ASSERT_NOT_NULL(gdb->db);
    // Close is in tearDown
}

void test_graphdb_add_node(void) {
    graphdb_add_node(gdb, "node1", "Person");
    char* label = graphdb_get_node_label(gdb, "node1");
    TEST_ASSERT_NOT_NULL(label);
    TEST_ASSERT_EQUAL_STRING("Person", label);
    free(label);
}

void test_graphdb_add_edge(void) {
    graphdb_add_node(gdb, "node1", "Person");
    graphdb_add_node(gdb, "node2", "Person");
    graphdb_add_edge(gdb, "node1", "node2", "FRIEND");
    int count;
    Neighbor* neighbors = graphdb_get_outgoing(gdb, "node1", "FRIEND", &count);
    TEST_ASSERT_EQUAL_INT(1, count);
    TEST_ASSERT_EQUAL_STRING("node2", neighbors[0].id);
    free(neighbors[0].id);
    free(neighbors[0].type);
    free(neighbors);
}

void test_graphdb_get_incoming(void) {
    graphdb_add_node(gdb, "node1", "Person");
    graphdb_add_node(gdb, "node2", "Person");
    graphdb_add_edge(gdb, "node1", "node2", "FRIEND");
    int count;
    Neighbor* incoming = graphdb_get_incoming(gdb, "node2", "FRIEND", &count);
    TEST_ASSERT_EQUAL_INT(1, count);
    TEST_ASSERT_EQUAL_STRING("node1", incoming[0].id);
    free(incoming[0].id);
    free(incoming[0].type);
    free(incoming);
}

void test_graphdb_get_nodes_by_label(void) {
    graphdb_add_node(gdb, "node1", "Person");
    graphdb_add_node(gdb, "node2", "Person");
    graphdb_add_node(gdb, "node3", "Animal");
    int count;
    char** nodes = graphdb_get_nodes_by_label(gdb, "Person", &count);
    TEST_ASSERT_EQUAL_INT(2, count);
    // Order not guaranteed, check presence
    int found1 = 0, found2 = 0;
    for (int i = 0; i < count; i++) {
        if (strcmp(nodes[i], "node1") == 0) found1 = 1;
        if (strcmp(nodes[i], "node2") == 0) found2 = 1;
        free(nodes[i]);
    }
    free(nodes);
    TEST_ASSERT_TRUE(found1 && found2);
}

void test_graphdb_get_all_nodes(void) {
    graphdb_add_node(gdb, "node1", "Person");
    graphdb_add_node(gdb, "node2", "Person");
    int count;
    char** nodes = graphdb_get_all_nodes(gdb, &count);
    TEST_ASSERT_EQUAL_INT(2, count);
    for (int i = 0; i < count; i++) free(nodes[i]);
    free(nodes);
}

void test_graphdb_delete_node(void) {
    graphdb_add_node(gdb, "node1", "Person");
    graphdb_add_node(gdb, "node2", "Person");
    graphdb_add_edge(gdb, "node1", "node2", "FRIEND");
    graphdb_delete_node(gdb, "node1");
    char* label = graphdb_get_node_label(gdb, "node1");
    TEST_ASSERT_NULL(label);
    int count;
    Neighbor* incoming = graphdb_get_incoming(gdb, "node2", "FRIEND", &count);
    TEST_ASSERT_EQUAL_INT(0, count);
    if (incoming) free(incoming);
}

void test_graphdb_delete_edge(void) {
    graphdb_add_node(gdb, "node1", "Person");
    graphdb_add_node(gdb, "node2", "Person");
    graphdb_add_edge(gdb, "node1", "node2", "FRIEND");
    graphdb_delete_edge(gdb, "node1", "node2", "FRIEND");
    int count;
    Neighbor* neighbors = graphdb_get_outgoing(gdb, "node1", "FRIEND", &count);
    TEST_ASSERT_EQUAL_INT(0, count);
    if (neighbors) free(neighbors);
}

void test_find_shortest_path(void) {
    graphdb_add_node(gdb, "node1", "Person");
    graphdb_add_node(gdb, "node2", "Person");
    graphdb_add_node(gdb, "node3", "Person");
    graphdb_add_edge(gdb, "node1", "node2", "FRIEND");
    graphdb_add_edge(gdb, "node2", "node3", "FRIEND");
    // Normally prints, but for test, we might need to capture output or modify function
    // For now, just call it
    find_shortest_path(gdb, "node1", "node3", "FRIEND");
    // To actually test, we may need to adjust the function to return the path
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_graphdb_open_close);
    RUN_TEST(test_graphdb_add_node);
    RUN_TEST(test_graphdb_add_edge);
    RUN_TEST(test_graphdb_get_incoming);
    RUN_TEST(test_graphdb_get_nodes_by_label);
    RUN_TEST(test_graphdb_get_all_nodes);
    RUN_TEST(test_graphdb_delete_node);
    RUN_TEST(test_graphdb_delete_edge);
    RUN_TEST(test_find_shortest_path);
    return UNITY_END();
} 