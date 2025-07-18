// graphdb.c
#include "graphdb.h"
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <ctype.h>

// Queue for thread-safe operations
typedef struct Node {
    char* data;
    struct Node* next;
} Node;

typedef struct Queue {
    Node* front;
    Node* rear;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} Queue;

Queue* queue_create() {
    Queue* q = (Queue*)malloc(sizeof(Queue));
    q->front = q->rear = NULL;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond, NULL);
    return q;
}

int queue_empty(Queue* q) {
    pthread_mutex_lock(&q->mutex);
    int empty = (q->front == NULL);
    pthread_mutex_unlock(&q->mutex);
    return empty;
}

void queue_enqueue(Queue* q, const char* data) {
    Node* new_node = (Node*)malloc(sizeof(Node));
    // Allow NULL to be used as a sentinel (e.g., to stop worker threads)
    new_node->data = data ? strdup(data) : NULL;
    new_node->next = NULL;
    pthread_mutex_lock(&q->mutex);
    if (q->rear == NULL) {
        q->front = q->rear = new_node;
    } else {
        q->rear->next = new_node;
        q->rear = new_node;
    }
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);
}

char* queue_dequeue(Queue* q) {
    pthread_mutex_lock(&q->mutex);
    while (q->front == NULL) {
        pthread_cond_wait(&q->cond, &q->mutex);
    }
    Node* temp = q->front;
    char* data = temp->data;
    q->front = q->front->next;
    if (q->front == NULL) q->rear = NULL;
    free(temp);
    pthread_mutex_unlock(&q->mutex);
    return data;
}

void queue_destroy(Queue* q) {
    while (!queue_empty(q)) {
        char* data = queue_dequeue(q);
        free(data);
    }
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond);
    free(q);
}

// Cache for prefetched neighbors
typedef struct {
    char* node;
    char** neighbors;
    int count;
} CacheEntry;

void add_to_cache(CacheEntry** cache, int* count, const char* node, char** neighbors, int neigh_count, pthread_mutex_t* mutex) {
    pthread_mutex_lock(mutex);
    *cache = (CacheEntry*)realloc(*cache, sizeof(CacheEntry) * (*count + 1));
    (*cache)[*count].node = strdup(node);
    (*cache)[*count].neighbors = neighbors;
    (*cache)[*count].count = neigh_count;
    (*count)++;
    pthread_mutex_unlock(mutex);
}

char** get_from_cache(CacheEntry** cache, int* count, const char* node, int* neigh_count, pthread_mutex_t* mutex) {
    pthread_mutex_lock(mutex);
    for (int i = 0; i < *count; i++) {
        if (strcmp((*cache)[i].node, node) == 0) {
            char** neighbors = (*cache)[i].neighbors;
            *neigh_count = (*cache)[i].count;
            free((*cache)[i].node);
            memmove(&(*cache)[i], &(*cache)[i + 1], sizeof(CacheEntry) * (*count - i - 1));
            (*count)--;
            pthread_mutex_unlock(mutex);
            return neighbors;
        }
    }
    pthread_mutex_unlock(mutex);
    return NULL;
}

// Prefetch argument
typedef struct {
    rocksdb_t* db;
    rocksdb_readoptions_t* readoptions;
    const char* type;
    Queue* node_queue;
    CacheEntry** cache;
    int* cache_count;
    pthread_mutex_t* cache_mutex;
} PrefetchArg;

// Prefetch thread function
void* prefetch_thread(void* arg) {
    PrefetchArg* pa = (PrefetchArg*)arg;
    while (1) {
        char* node = queue_dequeue(pa->node_queue);
        if (node == NULL) break;
        size_t prefix_len = 1 + strlen(node) + 1 + strlen(pa->type) + 1;
        char* prefix = (char*)malloc(prefix_len + 1);
        sprintf(prefix, "O%s:%s:", node, pa->type);
        rocksdb_iterator_t* it = rocksdb_create_iterator(pa->db, pa->readoptions);
        rocksdb_iter_seek(it, prefix, prefix_len);
        char** neighbors = NULL;
        int neigh_count = 0;
        while (rocksdb_iter_valid(it)) {
            size_t klen;
            const char* key = rocksdb_iter_key(it, &klen);
            if (klen <= prefix_len || memcmp(key, prefix, prefix_len) != 0) break;
            const char* to_start = key + prefix_len;
            size_t to_len = klen - prefix_len;
            char* to = (char*)malloc(to_len + 1);
            memcpy(to, to_start, to_len);
            to[to_len] = '\0';
            neighbors = (char**)realloc(neighbors, sizeof(char*) * (neigh_count + 1));
            neighbors[neigh_count++] = to;
            rocksdb_iter_next(it);
        }
        rocksdb_iter_destroy(it);
        free(prefix);
        add_to_cache(pa->cache, pa->cache_count, node, neighbors, neigh_count, pa->cache_mutex);
        free(node);
    }
    return NULL;
}

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

    rocksdb_options_set_prefix_extractor(gdb->options, rocksdb_slicetransform_create_fixed_prefix(1));

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
    rocksdb_readoptions_set_readahead_size(gdb->readoptions, 2ULL * 1024 * 1024);
    rocksdb_readoptions_set_async_io(gdb->readoptions, 1);

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
    size_t key_len = 1 + strlen(node_id); // 'N' + node_id
    char* key = (char*)malloc(key_len + 1);
    sprintf(key, "N%s", node_id);
    char* err = NULL;
    rocksdb_put(gdb->db, gdb->writeoptions, key, key_len, label, strlen(label), &err);
    if (err) {
        fprintf(stderr, "Error adding node: %s\n", err);
        free(err);
    }
    free(key);

    // Add label index "L<label>:<node_id>" -> ""
    size_t l_key_len = 1 + strlen(label) + 1 + strlen(node_id);
    char* l_key = (char*)malloc(l_key_len + 1);
    sprintf(l_key, "L%s:%s", label, node_id);
    rocksdb_put(gdb->db, gdb->writeoptions, l_key, l_key_len, "", 0, &err);
    if (err) {
        fprintf(stderr, "Error adding label index: %s\n", err);
        free(err);
    }
    free(l_key);
}

void graphdb_add_edge(GraphDB* gdb, const char* from, const char* to, const char* type) {
    if (!gdb) return;
    size_t o_key_len = 1 + strlen(from) + 1 + strlen(type) + 1 + strlen(to); // 'O' + from + ':' + type + ':' + to
    char* o_key = (char*)malloc(o_key_len + 1);
    sprintf(o_key, "O%s:%s:%s", from, type, to);
    char* err = NULL;
    rocksdb_put(gdb->db, gdb->writeoptions, o_key, o_key_len, "", 0, &err);
    if (err) {
        fprintf(stderr, "Error adding outgoing edge: %s\n", err);
        free(err);
    }
    free(o_key);
    size_t i_key_len = 1 + strlen(to) + 1 + strlen(type) + 1 + strlen(from); // 'I' + to + ':' + type + ':' + from
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

char** graphdb_get_nodes_by_label(GraphDB* gdb, const char* label, int* count) {
    if (!gdb) {
        *count = 0;
        return NULL;
    }
    size_t prefix_len = 1 + strlen(label) + 1;
    char* prefix = (char*)malloc(prefix_len + 1);
    sprintf(prefix, "L%s:", label);
    rocksdb_iterator_t* it = rocksdb_create_iterator(gdb->db, gdb->readoptions);
    rocksdb_iter_seek(it, prefix, prefix_len);
    char** nodes = NULL;
    *count = 0;
    while (rocksdb_iter_valid(it)) {
        size_t klen;
        const char* key = rocksdb_iter_key(it, &klen);
        if (klen <= prefix_len || memcmp(key, prefix, prefix_len) != 0) break;
        const char* id_start = key + prefix_len;
        size_t id_len = klen - prefix_len;
        char* id = (char*)malloc(id_len + 1);
        memcpy(id, id_start, id_len);
        id[id_len] = '\0';
        nodes = (char**)realloc(nodes, sizeof(char*) * (*count + 1));
        nodes[*count] = id;
        (*count)++;
        rocksdb_iter_next(it);
    }
    rocksdb_iter_destroy(it);
    free(prefix);
    return nodes;
}

char** graphdb_get_all_nodes(GraphDB* gdb, int* count) {
    if (!gdb) {
        *count = 0;
        return NULL;
    }
    size_t prefix_len = 1;
    char* prefix = (char*)malloc(prefix_len + 1);
    sprintf(prefix, "N");
    rocksdb_iterator_t* it = rocksdb_create_iterator(gdb->db, gdb->readoptions);
    rocksdb_iter_seek(it, prefix, prefix_len);
    char** nodes = NULL;
    *count = 0;
    while (rocksdb_iter_valid(it)) {
        size_t klen;
        const char* key = rocksdb_iter_key(it, &klen);
        if (klen <= prefix_len || memcmp(key, prefix, prefix_len) != 0) break;
        const char* id_start = key + prefix_len;
        size_t id_len = klen - prefix_len;
        char* id = (char*)malloc(id_len + 1);
        memcpy(id, id_start, id_len);
        id[id_len] = '\0';
        nodes = (char**)realloc(nodes, sizeof(char*) * (*count + 1));
        nodes[*count] = id;
        (*count)++;
        rocksdb_iter_next(it);
    }
    rocksdb_iter_destroy(it);
    free(prefix);
    return nodes;
}

void graphdb_delete_node(GraphDB* gdb, const char* node_id) {
    char* label = graphdb_get_node_label(gdb, node_id);
    if (label) {
        size_t l_key_len = 1 + strlen(label) + 1 + strlen(node_id);
        char* l_key = (char*)malloc(l_key_len + 1);
        sprintf(l_key, "L%s:%s", label, node_id);
        char* err = NULL;
        rocksdb_delete(gdb->db, gdb->writeoptions, l_key, l_key_len, &err);
        free(l_key);
        free(label);
        if (err) free(err);
    }

    size_t n_key_len = 1 + strlen(node_id);
    char* n_key = (char*)malloc(n_key_len + 1);
    sprintf(n_key, "N%s", node_id);
    char* err = NULL;
    rocksdb_delete(gdb->db, gdb->writeoptions, n_key, n_key_len, &err);
    free(n_key);
    if (err) free(err);

    // Delete outgoing edges and their incoming counterparts
    size_t o_prefix_len = 1 + strlen(node_id) + 1;
    char* o_prefix = (char*)malloc(o_prefix_len + 1);
    sprintf(o_prefix, "O%s:", node_id);
    rocksdb_iterator_t* o_it = rocksdb_create_iterator(gdb->db, gdb->readoptions);
    rocksdb_iter_seek(o_it, o_prefix, o_prefix_len);
    while (rocksdb_iter_valid(o_it)) {
        size_t klen;
        const char* key = rocksdb_iter_key(o_it, &klen);
        if (klen < o_prefix_len || memcmp(key, o_prefix, o_prefix_len) != 0) break;
        // Extract type and to
        const char* after_prefix = key + o_prefix_len;
        const char* colon = strchr(after_prefix, ':');
        if (!colon) {
            rocksdb_iter_next(o_it);
            continue;
        }
        size_t type_len = colon - after_prefix;
        char* type = (char*)malloc(type_len + 1);
        memcpy(type, after_prefix, type_len);
        type[type_len] = '\0';
        const char* to_start = colon + 1;
        size_t to_len = klen - (to_start - key);
        char* to = (char*)malloc(to_len + 1);
        memcpy(to, to_start, to_len);
        to[to_len] = '\0';
        // Delete outgoing
        rocksdb_delete(gdb->db, gdb->writeoptions, key, klen, &err);
        if (err) free(err); err = NULL;
        // Delete corresponding incoming
        size_t i_key_len = 1 + strlen(to) + 1 + strlen(type) + 1 + strlen(node_id);
        char* i_key = (char*)malloc(i_key_len + 1);
        sprintf(i_key, "I%s:%s:%s", to, type, node_id);
        rocksdb_delete(gdb->db, gdb->writeoptions, i_key, i_key_len, &err);
        free(i_key);
        if (err) free(err); err = NULL;
        free(type);
        free(to);
        rocksdb_iter_next(o_it);
    }
    rocksdb_iter_destroy(o_it);
    free(o_prefix);

    // Delete incoming edges and their outgoing counterparts
    size_t i_prefix_len = 1 + strlen(node_id) + 1;
    char* i_prefix = (char*)malloc(i_prefix_len + 1);
    sprintf(i_prefix, "I%s:", node_id);
    rocksdb_iterator_t* i_it = rocksdb_create_iterator(gdb->db, gdb->readoptions);
    rocksdb_iter_seek(i_it, i_prefix, i_prefix_len);
    while (rocksdb_iter_valid(i_it)) {
        size_t klen;
        const char* key = rocksdb_iter_key(i_it, &klen);
        if (klen < i_prefix_len || memcmp(key, i_prefix, i_prefix_len) != 0) break;
        // Extract type and from
        const char* after_prefix = key + i_prefix_len;
        const char* colon = strchr(after_prefix, ':');
        if (!colon) {
            rocksdb_iter_next(i_it);
            continue;
        }
        size_t type_len = colon - after_prefix;
        char* type = (char*)malloc(type_len + 1);
        memcpy(type, after_prefix, type_len);
        type[type_len] = '\0';
        const char* from_start = colon + 1;
        size_t from_len = klen - (from_start - key);
        char* from = (char*)malloc(from_len + 1);
        memcpy(from, from_start, from_len);
        from[from_len] = '\0';
        // Delete incoming
        rocksdb_delete(gdb->db, gdb->writeoptions, key, klen, &err);
        if (err) free(err); err = NULL;
        // Delete corresponding outgoing
        size_t o_key_len = 1 + strlen(from) + 1 + strlen(type) + 1 + strlen(node_id);
        char* o_key = (char*)malloc(o_key_len + 1);
        sprintf(o_key, "O%s:%s:%s", from, type, node_id);
        rocksdb_delete(gdb->db, gdb->writeoptions, o_key, o_key_len, &err);
        free(o_key);
        if (err) free(err); err = NULL;
        free(type);
        free(from);
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

// Improved find_shortest_path with prefetch
#define PREFETCH_THREADS 8

void find_shortest_path(GraphDB* gdb, const char* start, const char* end, const char* type) {
    if (strcmp(start, end) == 0) {
        printf("Shortest path: %s\n", start);
        return;
    }

    Queue* current_level = queue_create();
    Queue* next_level = queue_create();
    Queue* prefetch_queue = queue_create();

    char** visited = NULL;
    int visited_count = 0;
    int visited_capacity = 10;
    visited = (char**)malloc(sizeof(char*) * visited_capacity);
    visited[visited_count++] = strdup(start);

    typedef struct {
        char* child;
        char* parent;
    } ParentEntry;
    ParentEntry* parents = NULL;
    int parents_count = 0;
    int parents_capacity = 10;
    parents = (ParentEntry*)malloc(sizeof(ParentEntry) * parents_capacity);

    queue_enqueue(current_level, start);

    CacheEntry* cache = NULL;
    int cache_count = 0;
    pthread_mutex_t cache_mutex = PTHREAD_MUTEX_INITIALIZER;

    PrefetchArg pa;
    pa.db = gdb->db;
    pa.readoptions = gdb->readoptions;
    pa.type = type;
    pa.node_queue = prefetch_queue;
    pa.cache = &cache;
    pa.cache_count = &cache_count;
    pa.cache_mutex = &cache_mutex;

    pthread_t threads[PREFETCH_THREADS];
    for (int i = 0; i < PREFETCH_THREADS; i++) {
        pthread_create(&threads[i], NULL, prefetch_thread, &pa);
    }

    int found = 0;

    while (!queue_empty(current_level) && !found) {
        while (!queue_empty(current_level)) {
            char* current = queue_dequeue(current_level);

            int count;
            char** neighbors = get_from_cache(&cache, &cache_count, current, &count, &cache_mutex);
            if (neighbors == NULL) {
                neighbors = graphdb_get_outgoing(gdb, current, type, &count);
            }

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

                    queue_enqueue(next_level, neigh);
                    queue_enqueue(prefetch_queue, neigh);

                    if (strcmp(neigh, end) == 0) {
                        found = 1;
                    }
                }
                free(neigh);
            }
            free(neighbors);
            free(current);
            if (found) break;
        }
        Queue* temp = current_level;
        current_level = next_level;
        next_level = temp;
        // Clear next_level for reuse
        while (!queue_empty(next_level)) {
            free(queue_dequeue(next_level));
        }
        if (found) break;
    }

    // Stop prefetch threads
    for (int i = 0; i < PREFETCH_THREADS; i++) {
        queue_enqueue(prefetch_queue, NULL);
    }
    for (int i = 0; i < PREFETCH_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Cleanup queues
    queue_destroy(current_level);
    queue_destroy(next_level);
    queue_destroy(prefetch_queue);

    // Cleanup remaining cache
    for (int i = 0; i < cache_count; i++) {
        free(cache[i].node);
        for (int j = 0; j < cache[i].count; j++) {
            free(cache[i].neighbors[j]);
        }
        free(cache[i].neighbors);
    }
    free(cache);
    pthread_mutex_destroy(&cache_mutex);

    if (!found) {
        printf("No path found from %s to %s\n", start, end);
    } else {
        char** path = NULL;
        int path_count = 0;
        int path_capacity = 10;
        path = (char**)malloc(sizeof(char*) * path_capacity);

        char* current = strdup(end);
        while (current) {
            if (path_count >= path_capacity) {
                path_capacity *= 2;
                path = (char**)realloc(path, sizeof(char*) * path_capacity);
            }
            path[path_count++] = current;

            if (strcmp(current, start) == 0) break;

            char* parent = NULL;
            for (int j = 0; j < parents_count; j++) {
                if (strcmp(parents[j].child, current) == 0) {
                    parent = parents[j].parent;
                    break;
                }
            }
            current = parent ? strdup(parent) : NULL;
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

    // Cleanup visited and parents
    for (int i = 0; i < visited_count; i++) free(visited[i]);
    free(visited);
    for (int i = 0; i < parents_count; i++) {
        free(parents[i].child);
        free(parents[i].parent);
    }
    free(parents);
}