#include "../cypher_parser.h"
#include "../graphdb.h"
#include "unity/src/unity.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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
    gdb = graphdb_open(TEST_DB_PATH);
    TEST_ASSERT_NOT_NULL(gdb);
    // Add some test data
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

}

void tearDown(void) {
    graphdb_close(gdb);
    remove_directory(TEST_DB_PATH);
}

void test_execute_cypher_simple(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH (a)-[:FRIEND]->(b) WHERE a.id = 'Mark' RETURN b.id");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(1, res->column_count);
    TEST_ASSERT_EQUAL_STRING("b.id", res->columns[0]);
    TEST_ASSERT_EQUAL_INT(2, res->row_count);
    // Check results (order not guaranteed)
    int found2 = 0, found3 = 0;
    for (int i = 0; i < res->row_count; i++) {
        if (strcmp(res->rows[i][0], "Alex") == 0) found2 = 1;
        if (strcmp(res->rows[i][0], "Felipe") == 0) found3 = 1;
    }
    TEST_ASSERT_TRUE(found2 && found3);
    free_cypher_result(res);
}

void test_execute_cypher_with_label(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH (a:Person)-[:FRIEND]->(b:Person) WHERE a.id = 'Mark' RETURN b.id, b.label");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(2, res->column_count);
    TEST_ASSERT_EQUAL_STRING("b.id", res->columns[0]);
    TEST_ASSERT_EQUAL_STRING("b.label", res->columns[1]);
    TEST_ASSERT_EQUAL_INT(2, res->row_count);
    free_cypher_result(res);
}

void test_execute_cypher_no_results(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH (a)-[:FRIEND]->(b) WHERE a.id = 'node4' RETURN b.id");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(1, res->column_count);
    TEST_ASSERT_EQUAL_INT(0, res->row_count);
    free_cypher_result(res);
}

void test_execute_cypher_with_filter(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH (a:Person)-[:FRIEND]->(b:Person) WHERE a.id = 'Mark' RETURN b.id, b.label");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(2, res->column_count);
    TEST_ASSERT_EQUAL_STRING("b.id", res->columns[0]);
    TEST_ASSERT_EQUAL_STRING("b.label", res->columns[1]);
    TEST_ASSERT_EQUAL_INT(2, res->row_count);
    free_cypher_result(res);
}

void test_execute_cypher_with_filter_and_return(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH (a:Person)-[:CONTACT_INFO]->(b:Email) WHERE a.id = 'Felipe' RETURN b.id, b.label");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(2, res->column_count);
    TEST_ASSERT_EQUAL_STRING("b.id", res->columns[0]);
    TEST_ASSERT_EQUAL_STRING("b.label", res->columns[1]);
    TEST_ASSERT_EQUAL_INT(1, res->row_count);
    TEST_ASSERT_EQUAL_STRING("research@felipebonetto.com", res->rows[0][0]);
    TEST_ASSERT_EQUAL_STRING("Email", res->rows[0][1]);
    free_cypher_result(res);
}


int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_execute_cypher_simple);
    RUN_TEST(test_execute_cypher_with_label);
    RUN_TEST(test_execute_cypher_no_results);
    RUN_TEST(test_execute_cypher_with_filter);
    RUN_TEST(test_execute_cypher_with_filter_and_return);
    return UNITY_END();
} 