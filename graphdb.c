#include "graphdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

GraphDB* graphdb_open(const char* path) {
    GraphDB* gdb = (GraphDB*)malloc(sizeof(GraphDB));
    if (!gdb) return NULL;

    gdb->options = rocksdb_options_create();
    gdb->table_options = rocksdb_block_based_options_create();
    gdb->cache = rocksdb_cache_create_lru(512 * 1024 * 1024);

    rocksdb_options_set_create_if_missing(gdb->options, 1);
    rocksdb_options_set_use_direct_reads(gdb->options, 1);
    rocksdb_options_set_use_direct_io_for_flush_and_compaction(gdb->options, 1);
    rocksdb_options_increase_parallelism(gdb->options, 16);
    rocksdb_options_optimize_level_style_compaction(gdb->options, 512 * 1024 * 1024);
    rocksdb_options_set_compression(gdb->options, rocksdb_snappy_compression);
    rocksdb_options_set_write_buffer_size(gdb->options, 256 * 1024 * 1024);
    rocksdb_options_set_max_write_buffer_number(gdb->options, 8);
    rocksdb_options_set_min_write_buffer_number_to_merge(gdb->options, 2);

    rocksdb_block_based_options_set_block_size(gdb->table_options, 16384);
    rocksdb_block_based_options_set_filter_policy(gdb->table_options, rocksdb_filterpolicy_create_bloom(10));
    rocksdb_block_based_options_set_cache_index_and_filter_blocks(gdb->table_options, 1);
    rocksdb_block_based_options_set_block_cache(gdb->table_options, gdb->cache);
    rocksdb_options_set_block_based_table_factory(gdb->options, gdb->table_options);

    char* err = NULL;
    gdb->db = rocksdb_open(gdb->options, path, &err);
    if (err) {
        fprintf(stderr, "Error opening DB: %s\n", err);
        free(err);
        graphdb_close(gdb);
        return NULL;
    }

    gdb->writeoptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_set_sync(gdb->writeoptions, 0);
    gdb->readoptions = rocksdb_readoptions_create();
    return gdb;
}

void graphdb_close(GraphDB* gdb) {
    if (!gdb) return;
    rocksdb_close(gdb->db);
    rocksdb_options_destroy(gdb->options);
    rocksdb_block_based_options_destroy(gdb->table_options);
    rocksdb_cache_destroy(gdb->cache);
    rocksdb_writeoptions_destroy(gdb->writeoptions);
    rocksdb_readoptions_destroy(gdb->readoptions);
    free(gdb);
}

void graphdb_add_node(GraphDB* gdb, const char* node_id, const char* label) {
    if (!gdb) return;
    size_t key_len = 2 + strlen(node_id); // 'N:' is now 'N'
    char* key = (char*)malloc(key_len + 1);
    sprintf(key, "N%s", node_id); // Removed ':'
    char* err = NULL;
    rocksdb_put(gdb->db, gdb->writeoptions, key, key_len, label, strlen(label), &err);
    if (err) {
        fprintf(stderr, "Error adding node: %s\n", err);
        free(err);
    }
    free(key);
}

void graphdb_add_edge(GraphDB* gdb, const char* from, const char* to, const char* type) {
    if (!gdb) return;
    size_t o_key_len = 1 + strlen(from) + 1 + strlen(type) + 1 + strlen(to); // 'O:' to 'O'
    char* o_key = (char*)malloc(o_key_len + 1);
    sprintf(o_key, "O%s:%s:%s", from, type, to);
    char* err = NULL;
    rocksdb_put(gdb->db, gdb->writeoptions, o_key, o_key_len, "", 0, &err);
    if (err) {
        fprintf(stderr, "Error adding outgoing edge: %s\n", err);
        free(err);
    }
    free(o_key);
    size_t i_key_len = 1 + strlen(to) + 1 + strlen(type) + 1 + strlen(from); // 'I:' to 'I'
    char* i_key = (char*)malloc(i_key_len + 1);
    sprintf(i_key, "I%s:%s:%s", to, type, from);
    rocksdb_put(gdb->db, gdb->writeoptions, i_key, i_key_len, "", 0, &err);
    if (err) {
        fprintf(stderr, "Error adding incoming edge: %s\n", err);
        free(err);
    }
    free(i_key);
}

char** graphdb_get_outgoing(GraphDB* gdb, const char* node, const char* type, int* count) {
    if (!gdb) {
        *count = 0;
        return NULL;
    }
    size_t prefix_len = 1 + strlen(node) + 1 + strlen(type) + 1; // Adjusted for shorter prefix
    char* prefix = (char*)malloc(prefix_len + 1);
    sprintf(prefix, "O%s:%s:", node, type);
    rocksdb_iterator_t* it = rocksdb_create_iterator(gdb->db, gdb->readoptions);
    rocksdb_iter_seek(it, prefix, prefix_len);
    char** neighbors = NULL;
    *count = 0;
    while (rocksdb_iter_valid(it)) {
        size_t klen;
        const char* key = rocksdb_iter_key(it, &klen);
        if (klen <= prefix_len || memcmp(key, prefix, prefix_len) != 0) break;
        const char* to_start = key + prefix_len;
        size_t to_len = klen - prefix_len;
        char* to = (char*)malloc(to_len + 1);
        memcpy(to, to_start, to_len);
        to[to_len] = '\0';
        neighbors = (char**)realloc(neighbors, sizeof(char*) * (*count + 1));
        neighbors[*count] = to;
        (*count)++;
        rocksdb_iter_next(it);
    }
    rocksdb_iter_destroy(it);
    free(prefix);
    return neighbors;
}

char** graphdb_get_incoming(GraphDB* gdb, const char* node, const char* type, int* count) {
    if (!gdb) {
        *count = 0;
        return NULL;
    }
    size_t prefix_len = 1 + strlen(node) + 1 + strlen(type) + 1; // Adjusted
    char* prefix = (char*)malloc(prefix_len + 1);
    sprintf(prefix, "I%s:%s:", node, type);
    rocksdb_iterator_t* it = rocksdb_create_iterator(gdb->db, gdb->readoptions);
    rocksdb_iter_seek(it, prefix, prefix_len);
    char** neighbors = NULL;
    *count = 0;
    while (rocksdb_iter_valid(it)) {
        size_t klen;
        const char* key = rocksdb_iter_key(it, &klen);
        if (klen <= prefix_len || memcmp(key, prefix, prefix_len) != 0) break;
        const char* from_start = key + prefix_len;
        size_t from_len = klen - prefix_len;
        char* from = (char*)malloc(from_len + 1);
        memcpy(from, from_start, from_len);
        from[from_len] = '\0';
        neighbors = (char**)realloc(neighbors, sizeof(char*) * (*count + 1));
        neighbors[*count] = from;
        (*count)++;
        rocksdb_iter_next(it);
    }
    rocksdb_iter_destroy(it);
    free(prefix);
    return neighbors;
}

char* graphdb_get_node_label(GraphDB* gdb, const char* node_id) {
    size_t key_len = 1 + strlen(node_id); // Adjusted
    char* key = (char*)malloc(key_len + 1);
    sprintf(key, "N%s", node_id);

    size_t val_len;
    char* err = NULL;
    char* value = rocksdb_get(gdb->db, gdb->readoptions, key, key_len, &val_len, &err);
    free(key);
    if (err) {
        fprintf(stderr, "Error getting node label: %s\n", err);
        free(err);
        return NULL;
    }
    if (!value) return NULL;

    char* label = (char*)malloc(val_len + 1);
    memcpy(label, value, val_len);
    label[val_len] = '\0';
    free(value);
    return label;
}

void graphdb_delete_node(GraphDB* gdb, const char* node_id) {
    size_t n_key_len = 1 + strlen(node_id);
    char* n_key = (char*)malloc(n_key_len + 1);
    sprintf(n_key, "N%s", node_id);
    char* err = NULL;
    rocksdb_delete(gdb->db, gdb->writeoptions, n_key, n_key_len, &err);
    free(n_key);
    if (err) {
        fprintf(stderr, "Error deleting node: %s\n", err);
        free(err);
    }

    size_t o_prefix_len = 1 + strlen(node_id) + 1;
    char* o_prefix = (char*)malloc(o_prefix_len + 1);
    sprintf(o_prefix, "O%s:", node_id);
    rocksdb_iterator_t* o_it = rocksdb_create_iterator(gdb->db, gdb->readoptions);
    rocksdb_iter_seek(o_it, o_prefix, o_prefix_len);
    while (rocksdb_iter_valid(o_it)) {
        size_t klen;
        const char* key = rocksdb_iter_key(o_it, &klen);
        if (klen < o_prefix_len || memcmp(key, o_prefix, o_prefix_len) != 0) break;
        rocksdb_delete(gdb->db, gdb->writeoptions, key, klen, &err);
        if (err) fprintf(stderr, "Error deleting outgoing edge: %s\n", err);
        free(err); err = NULL;
        rocksdb_iter_next(o_it);
    }
    rocksdb_iter_destroy(o_it);
    free(o_prefix);

    size_t i_prefix_len = 1 + strlen(node_id) + 1;
    char* i_prefix = (char*)malloc(i_prefix_len + 1);
    sprintf(i_prefix, "I%s:", node_id);
    rocksdb_iterator_t* i_it = rocksdb_create_iterator(gdb->db, gdb->readoptions);
    rocksdb_iter_seek(i_it, i_prefix, i_prefix_len);
    while (rocksdb_iter_valid(i_it)) {
        size_t klen;
        const char* key = rocksdb_iter_key(i_it, &klen);
        if (klen < i_prefix_len || memcmp(key, i_prefix, i_prefix_len) != 0) break;
        rocksdb_delete(gdb->db, gdb->writeoptions, key, klen, &err);
        if (err) fprintf(stderr, "Error deleting incoming edge: %s\n", err);
        free(err); err = NULL;
        rocksdb_iter_next(i_it);
    }
    rocksdb_iter_destroy(i_it);
    free(i_prefix);
}

void graphdb_delete_edge(GraphDB* gdb, const char* from, const char* to, const char* type) {
    size_t o_key_len = 1 + strlen(from) + 1 + strlen(type) + 1 + strlen(to);
    char* o_key = (char*)malloc(o_key_len + 1);
    sprintf(o_key, "O%s:%s:%s", from, type, to);
    char* err = NULL;
    rocksdb_delete(gdb->db, gdb->writeoptions, o_key, o_key_len, &err);
    free(o_key);
    if (err) {
        fprintf(stderr, "Error deleting outgoing edge: %s\n", err);
        free(err);
    }

    size_t i_key_len = 1 + strlen(to) + 1 + strlen(type) + 1 + strlen(from);
    char* i_key = (char*)malloc(i_key_len + 1);
    sprintf(i_key, "I%s:%s:%s", to, type, from);
    rocksdb_delete(gdb->db, gdb->writeoptions, i_key, i_key_len, &err);
    free(i_key);
    if (err) {
        fprintf(stderr, "Error deleting incoming edge: %s\n", err);
        free(err);
    }
}

void graphdb_execute_basic_cypher(GraphDB* gdb, const char* query) {
    char start[256] = {0};
    char type[256] = {0};
    if (sscanf(query, "MATCH (a)-[:%[^]]]->(b) WHERE a.id = '%[^']' RETURN b.id", type, start) == 2) {
        int count;
        char** neighbors = graphdb_get_outgoing(gdb, start, type, &count);
        if (neighbors) {
            printf("Results for query: %s\n", query);
            for (int i = 0; i < count; i++) {
                printf("b.id: %s\n", neighbors[i]);
                free(neighbors[i]);
            }
            free(neighbors);
        } else {
            printf("No results\n");
        }
    } else {
        printf("Unsupported query format\n");
    }
}

void find_shortest_path(GraphDB* gdb, const char* start, const char* end, const char* type) {
    if (strcmp(start, end) == 0) {
        printf("Shortest path: %s\n", start);
        return;
    }

    char** queue = NULL;
    int queue_size = 0;
    int queue_capacity = 10;
    queue = (char**)malloc(sizeof(char*) * queue_capacity);

    char** visited = NULL;
    int visited_count = 0;
    int visited_capacity = 10;
    visited = (char**)malloc(sizeof(char*) * visited_capacity);

    typedef struct {
        char* child;
        char* parent;
    } ParentEntry;
    ParentEntry* parents = NULL;
    int parents_count = 0;
    int parents_capacity = 10;
    parents = (ParentEntry*)malloc(sizeof(ParentEntry) * parents_capacity);

    queue[queue_size++] = strdup(start);
    visited[visited_count++] = strdup(start);

    int found = 0;

    while (queue_size > 0) {
        char* current = queue[0];
        memmove(queue, queue + 1, sizeof(char*) * (--queue_size));

        int count;
        char** neighbors = graphdb_get_outgoing(gdb, current, type, &count);

        for (int i = 0; i < count; i++) {
            char* neigh = neighbors[i];

            int is_visited = 0;
            for (int j = 0; j < visited_count; j++) {
                if (strcmp(visited[j], neigh) == 0) {
                    is_visited = 1;
                    break;
                }
            }

            if (!is_visited) {
                if (visited_count >= visited_capacity) {
                    visited_capacity *= 2;
                    visited = (char**)realloc(visited, sizeof(char*) * visited_capacity);
                }
                visited[visited_count++] = strdup(neigh);

                if (parents_count >= parents_capacity) {
                    parents_capacity *= 2;
                    parents = (ParentEntry*)realloc(parents, sizeof(ParentEntry) * parents_capacity);
                }
                parents[parents_count].child = strdup(neigh);
                parents[parents_count].parent = strdup(current);
                parents_count++;

                if (queue_size >= queue_capacity) {
                    queue_capacity *= 2;
                    queue = (char**)realloc(queue, sizeof(char*) * queue_capacity);
                }
                queue[queue_size++] = strdup(neigh);

                if (strcmp(neigh, end) == 0) {
                    found = 1;
                    break;
                }
            }
            free(neigh);
        }
        free(neighbors);

        if (found) break;
    }

    if (!found) {
        printf("No path found from %s to %s\n", start, end);
    } else {
        char** path = NULL;
        int path_count = 0;
        int path_capacity = 10;
        path = (char**)malloc(sizeof(char*) * path_capacity);

        char* current = strdup(end);
        while (current != NULL) {
            if (path_count >= path_capacity) {
                path_capacity *= 2;
                path = (char**)realloc(path, sizeof(char*) * path_capacity);
            }
            path[path_count++] = current;

            char* parent = NULL;
            for (int j = 0; j < parents_count; j++) {
                if (strcmp(parents[j].child, current) == 0) {
                    parent = parents[j].parent;
                    break;
                }
            }

            if (strcmp(current, start) == 0) break;

            if (parent == NULL) break;

            current = strdup(parent);
        }

        printf("Shortest path: ");
        for (int i = path_count - 1; i >= 0; i--) {
            printf("%s", path[i]);
            if (i > 0) printf(" -> ");
            free(path[i]);
        }
        printf("\n");

        free(path);
    }

    for (int i = 0; i < queue_size; i++) free(queue[i]);
    free(queue);
    for (int i = 0; i < visited_count; i++) free(visited[i]);
    free(visited);
    for (int i = 0; i < parents_count; i++) {
        free(parents[i].child);
        free(parents[i].parent);
    }
    free(parents);
} 