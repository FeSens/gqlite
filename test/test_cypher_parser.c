#include "../cypher_parser.h"
#include "../graphdb.h"
#include "unity/src/unity.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <stdbool.h>

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

void test_create_node(void) {
    CypherResult* res = execute_cypher(gdb, "CREATE (n:Person {id:'NewPerson'})");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(0, res->row_count);
    free_cypher_result(res);
    res = execute_cypher(gdb, "MATCH (n:Person) WHERE n.id = 'NewPerson' RETURN n.id, n.label");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(2, res->column_count);
    TEST_ASSERT_EQUAL_INT(1, res->row_count);
    TEST_ASSERT_EQUAL_STRING("NewPerson", res->rows[0][0]);
    TEST_ASSERT_EQUAL_STRING("Person", res->rows[0][1]);
    free_cypher_result(res);
}

void test_create_edge(void) {
    CypherResult* res = execute_cypher(gdb, "CREATE (a:Person {id:'P1'})-[:KNOWS]->(b:Person {id:'P2'})");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(0, res->row_count);
    free_cypher_result(res);
    res = execute_cypher(gdb, "MATCH (a)-[:KNOWS]->(b) WHERE a.id = 'P1' RETURN b.id");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(1, res->column_count);
    TEST_ASSERT_EQUAL_INT(1, res->row_count);
    TEST_ASSERT_EQUAL_STRING("P2", res->rows[0][0]);
    free_cypher_result(res);
}

void test_delete_node(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH (a) WHERE a.id = 'Mark' DELETE a");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(0, res->row_count);
    free_cypher_result(res);
    res = execute_cypher(gdb, "MATCH (a) WHERE a.id = 'Mark' RETURN a.id");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(1, res->column_count);
    TEST_ASSERT_EQUAL_INT(0, res->row_count);
    free_cypher_result(res);
}

void test_delete_edge(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH (a)-[r:FRIEND]->(b) WHERE a.id = 'Mark' DELETE r");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(0, res->row_count);
    free_cypher_result(res);
    res = execute_cypher(gdb, "MATCH (a)-[:FRIEND]->(b) WHERE a.id = 'Mark' RETURN b.id");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(1, res->column_count);
    TEST_ASSERT_EQUAL_INT(0, res->row_count);
    free_cypher_result(res);
}

void test_multi_hop_query(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person) WHERE a.id = 'Mark' RETURN a.id, b.id, c.id");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(3, res->column_count);
    TEST_ASSERT_EQUAL_STRING("a.id", res->columns[0]);
    TEST_ASSERT_EQUAL_STRING("b.id", res->columns[1]);
    TEST_ASSERT_EQUAL_STRING("c.id", res->columns[2]);
    TEST_ASSERT_EQUAL_INT(1, res->row_count);
    TEST_ASSERT_EQUAL_STRING("Mark", res->rows[0][0]);
    TEST_ASSERT_EQUAL_STRING("Alex", res->rows[0][1]);
    TEST_ASSERT_EQUAL_STRING("Felipe", res->rows[0][2]);
    free_cypher_result(res);
}

void test_multi_condition_where(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH (a)-[:FRIEND]->(b) WHERE a.id = 'Mark' AND b.id = 'Alex' RETURN a.id, b.id");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(2, res->column_count);
    TEST_ASSERT_EQUAL_STRING("a.id", res->columns[0]);
    TEST_ASSERT_EQUAL_STRING("b.id", res->columns[1]);
    TEST_ASSERT_EQUAL_INT(1, res->row_count);
    TEST_ASSERT_EQUAL_STRING("Mark", res->rows[0][0]);
    TEST_ASSERT_EQUAL_STRING("Alex", res->rows[0][1]);
    free_cypher_result(res);
}

void test_variable_length_path(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH (a)-[*1..2]->(b) WHERE a.id = 'Mark' RETURN b.id");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(1, res->column_count);
    TEST_ASSERT_EQUAL_INT(2, res->row_count);
    printf("Row count: %d\n", res->row_count);
    for (int i = 0; i < res->row_count; i++) {
        printf("Row %d: %s\n", i, res->rows[i][0]);
    }
    int found_alex = 0, found_felipe = 0;
    for (int i = 0; i < res->row_count; i++) {
        if (strcmp(res->rows[i][0], "Alex") == 0) found_alex = 1;
        if (strcmp(res->rows[i][0], "Felipe") == 0) found_felipe = 1;
    }
    printf("Found Alex: %d\n", found_alex);
    printf("Found Felipe: %d\n", found_felipe);
    TEST_ASSERT_TRUE(found_alex && found_felipe);
    free_cypher_result(res);
}

void test_return_path(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH p = (a)-[*1..2]->(b) WHERE a.id = 'Mark' RETURN p");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(1, res->column_count);
    TEST_ASSERT_EQUAL_STRING("p", res->columns[0]);
    TEST_ASSERT_TRUE(res->row_count > 0);
    // Check format like "(Mark)-[:FRIEND]->(Alex)"
    printf("Row count: %d\n", res->row_count);
    for (int i = 0; i < res->row_count; i++) {
        printf("Row %d: %s\n", i, res->rows[i][0]);
    }
    bool found = false;
    for (int i = 0; i < res->row_count; i++) {
        if (strstr(res->rows[i][0], "(Mark:Person)") != NULL && strstr(res->rows[i][0], "->(Alex:Person)") != NULL) {
            found = true;
            break;
        }
    }
    TEST_ASSERT_TRUE(found);
    free_cypher_result(res);
}

void test_match_all_nodes(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH (a) RETURN a.id, a.label");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(2, res->column_count);
    TEST_ASSERT_EQUAL_STRING("a.id", res->columns[0]);
    TEST_ASSERT_EQUAL_STRING("a.label", res->columns[1]);
    TEST_ASSERT_EQUAL_INT(4, res->row_count);
    bool found_mark = false, found_alex = false, found_felipe = false, found_email = false;
    for (int i = 0; i < res->row_count; i++) {
        char* id = res->rows[i][0];
        char* label = res->rows[i][1];
        if (strcmp(id, "Mark") == 0 && strcmp(label, "Person") == 0) found_mark = true;
        else if (strcmp(id, "Alex") == 0 && strcmp(label, "Person") == 0) found_alex = true;
        else if (strcmp(id, "Felipe") == 0 && strcmp(label, "Person") == 0) found_felipe = true;
        else if (strcmp(id, "research@felipebonetto.com") == 0 && strcmp(label, "Email") == 0) found_email = true;
    }
    TEST_ASSERT_TRUE(found_mark && found_alex && found_felipe && found_email);
    free_cypher_result(res);
}

void test_match_any_rel(void) {
    CypherResult* res = execute_cypher(gdb, "MATCH p = (a)-[:]->(b) WHERE a.id = 'Mark' RETURN p");
    TEST_ASSERT_NOT_NULL(res);
    TEST_ASSERT_EQUAL_INT(1, res->column_count);
    TEST_ASSERT_EQUAL_STRING("p", res->columns[0]);
    TEST_ASSERT_EQUAL_INT(2, res->row_count);
    printf("Row count: %d\n", res->row_count);
    for (int i = 0; i < res->row_count; i++) {
        printf("Row %d: %s\n", i, res->rows[i][0]);
    }
    bool found_alex = false, found_felipe = false;
    for (int i = 0; i < res->row_count; i++) {
        if (strstr(res->rows[i][0], "(Mark:Person)-[:FRIEND]->(Alex:Person)") != NULL) found_alex = true;
        if (strstr(res->rows[i][0], "(Mark:Person)-[:FRIEND]->(Felipe:Person)") != NULL) found_felipe = true;
    }
    TEST_ASSERT_TRUE(found_alex && found_felipe);
    free_cypher_result(res);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_execute_cypher_simple);
    RUN_TEST(test_execute_cypher_with_label);
    RUN_TEST(test_execute_cypher_no_results);
    RUN_TEST(test_execute_cypher_with_filter);
    RUN_TEST(test_execute_cypher_with_filter_and_return);
    RUN_TEST(test_create_node);
    RUN_TEST(test_create_edge);
    RUN_TEST(test_delete_node);
    RUN_TEST(test_delete_edge);
    RUN_TEST(test_multi_hop_query);
    RUN_TEST(test_multi_condition_where);
    RUN_TEST(test_variable_length_path);
    RUN_TEST(test_return_path);
    RUN_TEST(test_match_all_nodes);
    RUN_TEST(test_match_any_rel);
    return UNITY_END();
} 