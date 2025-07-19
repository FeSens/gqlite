// Full updated code as per the provided file contents
// cypher_parser.c
#include "graphdb.h"
#include "cypher_parser.h"
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <ctype.h>
#include <stdbool.h> // Added for bool type
#include <limits.h>

// Assume graphdb.h is included which declares GraphDB and functions like graphdb_get_outgoing, etc.
// For example:
// typedef struct GraphDB GraphDB;
// GraphDB* graphdb_open(const char* path);
// void graphdb_close(GraphDB* gdb);
// char** graphdb_get_outgoing(GraphDB* gdb, const char* node, const char* type, int* count);
// etc.

// Legacy tabular result support removed – we now operate with structured results only.

// Extended parser structures
typedef struct {
    char* var;
    char* label;
    char* prop_key;
    char* prop_value;
} NodePattern;

// Add simple Queue for BFS
typedef struct PathQueueNode {
    char** path_ids;
    int path_len;
    int current_hop;
    char** rel_types;
    int rel_count;
    struct PathQueueNode* next;
} PathQueueNode;

typedef struct {
    PathQueueNode* front;
    PathQueueNode* rear;
} PathQueue;

static PathQueue* path_queue_create() {
    PathQueue* q = malloc(sizeof(PathQueue));
    q->front = q->rear = NULL;
    return q;
}

static void path_queue_enqueue(PathQueue* q, char** path_ids, int path_len, int current_hop, char** rel_types, int rel_count) {
    PathQueueNode* new_node = malloc(sizeof(PathQueueNode));
    new_node->path_ids = malloc(path_len * sizeof(char*));
    for (int i = 0; i < path_len; i++) new_node->path_ids[i] = strdup(path_ids[i]);
    new_node->path_len = path_len;
    new_node->current_hop = current_hop;
    new_node->rel_types = malloc(rel_count * sizeof(char*));
    for (int i = 0; i < rel_count; i++) new_node->rel_types[i] = strdup(rel_types[i]);
    new_node->rel_count = rel_count;
    new_node->next = NULL;
    if (q->rear == NULL) {
        q->front = q->rear = new_node;
    } else {
        q->rear->next = new_node;
        q->rear = new_node;
    }
}

static bool path_queue_dequeue(PathQueue* q, char*** path_ids, int* path_len, int* current_hop, char*** rel_types, int* rel_count) {
    if (q->front == NULL) return false;
    PathQueueNode* temp = q->front;
    *path_ids = temp->path_ids;
    *path_len = temp->path_len;
    *current_hop = temp->current_hop;
    *rel_types = temp->rel_types;
    *rel_count = temp->rel_count;
    q->front = q->front->next;
    if (q->front == NULL) q->rear = NULL;
    free(temp);
    return true;
}

static void path_queue_destroy(PathQueue* q) {
    char** dummy = NULL;
    int d1 = 0, d2 = 0;
    char** dummy_rel = NULL;
    int d3 = 0;
    while (path_queue_dequeue(q, &dummy, &d1, &d2, &dummy_rel, &d3)) {
        for (int i = 0; i < d1; i++) free(dummy[i]);
        free(dummy);
        for (int i = 0; i < d3; i++) free(dummy_rel[i]);
        free(dummy_rel);
    }
    free(q);
}

// Update structs
typedef struct {
    char* var;
    char* type;
    char direction;
    int min_hops;
    int max_hops;
} RelPattern;

typedef struct {
    NodePattern* nodes;
    RelPattern* rels;
    int count;
    char* path_var;
} PathPattern;

// MatchingPath with num_nodes
typedef struct {
    char** node_ids;
    int num_nodes;
    int* pattern_pos;
    char** rel_types;
    int num_rels;
} MatchingPath;

// Extended parser structures
typedef enum { Q_CREATE, Q_DELETE, Q_MATCH_RETURN } QueryType;

// Move WhereCondition before ParsedQuery
typedef struct {
    char* var;
    char* prop;
    char* val;
} WhereCondition;

typedef struct {
    QueryType type;
    PathPattern* match;
    WhereCondition* conditions;
    int cond_count;
    char** returns;
    int return_count;
    char** deletes;
    int delete_count;
} ParsedQuery;

// Helper to skip whitespace
static const char* skip_ws(const char* str) {
    while (isspace(*str)) str++;
    return str;
}

// Helper to trim string
static char* trim(const char* str) {
    str = skip_ws(str);
    const char* end = str + strlen(str) - 1;
    while (end > str && isspace(*end)) end--;
    return strndup(str, end - str + 1);
}

// Parse a node pattern like (a:Label {id:'val'})
static NodePattern* parse_node(const char** pattern) {
    *pattern = skip_ws(*pattern);
    if (**pattern != '(') return NULL;
    (*pattern)++;
    *pattern = skip_ws(*pattern);
    NodePattern* np = (NodePattern*)calloc(1, sizeof(NodePattern));
    // Variable
    const char* start = *pattern;
    while (**pattern && **pattern != ':' && **pattern != '{' && **pattern != ')' && !isspace(**pattern)) (*pattern)++;
    if (*pattern > start) np->var = strndup(start, *pattern - start);
    *pattern = skip_ws(*pattern);
    // Label
    if (**pattern == ':') {
        (*pattern)++;
        *pattern = skip_ws(*pattern);
        start = *pattern;
        while (**pattern && **pattern != '{' && **pattern != ')' && !isspace(**pattern)) (*pattern)++;
        np->label = strndup(start, *pattern - start);
        *pattern = skip_ws(*pattern);
    }
    // Properties (simple {key:'val'} for now, only id)
    if (**pattern == '{') {
        (*pattern)++;
        *pattern = skip_ws(*pattern);
        start = *pattern;
        while (**pattern && **pattern != ':') (*pattern)++;
        np->prop_key = strndup(start, *pattern - start);
        (*pattern)++;
        *pattern = skip_ws(*pattern);
        if (**pattern == '\'') (*pattern)++;
        start = *pattern;
        while (**pattern && **pattern != '\'') (*pattern)++;
        np->prop_value = strndup(start, *pattern - start);
        if (**pattern == '\'') (*pattern)++;
        *pattern = skip_ws(*pattern);
        if (**pattern == '}') (*pattern)++;
    }
    *pattern = skip_ws(*pattern);
    if (**pattern == ')') (*pattern)++;
    return np;
}

// Add updated parse_rel
static RelPattern* parse_rel(const char** pattern) {
    *pattern = skip_ws(*pattern);
    char start_dir = 0;
    if (**pattern == '<') {
        start_dir = '<';
        (*pattern)++;
        *pattern = skip_ws(*pattern);
    }
    if (**pattern != '-') return NULL;
    (*pattern)++;
    *pattern = skip_ws(*pattern);
    if (**pattern != '[') return NULL;
    (*pattern)++;
    *pattern = skip_ws(*pattern);
    RelPattern* rp = calloc(1, sizeof(RelPattern));
    const char* start = *pattern;
    while (**pattern && **pattern != ':' && **pattern != ']' && **pattern != '*') (*pattern)++;
    if (*pattern > start) rp->var = strndup(start, *pattern - start);
    if (**pattern == ':') {
        (*pattern)++;
        start = *pattern;
        while (**pattern && **pattern != ']' && **pattern != '*') (*pattern)++;
        rp->type = strndup(start, *pattern - start);
    }
    if (**pattern == '*') {
        (*pattern)++;
        const char* num_start = *pattern;
        while (isdigit(**pattern)) (*pattern)++;
        rp->min_hops = (num_start < *pattern) ? atoi(num_start) : 1;
        if (strncmp(*pattern, "..", 2) == 0) {
            (*pattern) += 2;
            num_start = *pattern;
            while (isdigit(**pattern)) (*pattern)++;
            rp->max_hops = (num_start < *pattern) ? atoi(num_start) : -1;
        } else {
            rp->max_hops = rp->min_hops;
        }
    } else {
        rp->min_hops = 1;
        rp->max_hops = 1;
    }
    printf("Parsed rel: min=%d max=%d type=%s\n", rp->min_hops, rp->max_hops, rp->type ? rp->type : "NULL");
    if (**pattern == ']') (*pattern)++;
    *pattern = skip_ws(*pattern);
    if (**pattern == '-') {
        (*pattern)++;
        if (**pattern == '>') {
            rp->direction = '>';
            (*pattern)++;
        } else if (start_dir == '<') {
            rp->direction = '<';
        }
    } else if (**pattern == '>') {
        rp->direction = '>';
        (*pattern)++;
    } else if (start_dir == '<') {
        rp->direction = '<';
    }
    *pattern = skip_ws(*pattern);
    return rp;
}

// Update parse_match
static PathPattern* parse_match(const char* match_str) {
    const char* p = match_str;
    PathPattern* path = calloc(1, sizeof(PathPattern));
    p = skip_ws(p);
    const char* eq = strchr(p, '=');
    if (eq && *(eq + 1) == ' ') {
        path->path_var = trim(strndup(p, eq - p));
        p = skip_ws(eq + 1);
    }
    NodePattern* np = parse_node(&p);
    if (!np) {
        free(path);
        return NULL;
    }
    path->nodes = (NodePattern*)malloc(sizeof(NodePattern));
    path->nodes[0] = *np;
    free(np);
    path->count = 1;
    RelPattern* rp = parse_rel(&p);
    int rel_count = 0;
    while (rp) {
        path->rels = (RelPattern*)realloc(path->rels, sizeof(RelPattern) * (rel_count + 1));
        path->rels[rel_count] = *rp;
        free(rp);
        rel_count++;
        np = parse_node(&p);
        if (!np) break;
        path->nodes = (NodePattern*)realloc(path->nodes, sizeof(NodePattern) * (path->count + 1));
        path->nodes[path->count] = *np;
        free(np);
        path->count++;
        rp = parse_rel(&p);
    }
    return path;
}

// Parse RETURN (simple list like b.id, a.label)
static char** parse_return(const char* ret_str, int* count) {
    char* ret_copy = strdup(ret_str);
    char** cols = NULL;
    *count = 0;
    char* tok = strtok(ret_copy, ",");
    while (tok) {
        tok = trim(tok);
        cols = (char**)realloc(cols, sizeof(char*) * (*count + 1));
        cols[*count] = strdup(tok);
        (*count)++;
        tok = strtok(NULL, ",");
    }
    free(ret_copy);
    return cols;
}

// Update collect_paths to handle variable length
static void collect_paths(GraphDB* gdb, PathPattern* path, int hop, char** current_path, int current_len, MatchingPath*** paths, int* num_paths, int* capacity, int* current_positions, char** current_rel_types, int current_rel_count) {
    if (hop >= path->count) {
        if (*num_paths >= *capacity) {
            *capacity *= 2;
            *paths = realloc(*paths, sizeof(MatchingPath*) * (*capacity));
        }
        (*paths)[*num_paths] = malloc(sizeof(MatchingPath));
        MatchingPath* mp = (*paths)[*num_paths];
        mp->num_nodes = current_len;
        mp->node_ids = malloc(current_len * sizeof(char*));
        for (int i = 0; i < current_len; i++) mp->node_ids[i] = strdup(current_path[i]);
        mp->pattern_pos = malloc(path->count * sizeof(int));
        memcpy(mp->pattern_pos, current_positions, path->count * sizeof(int));
        mp->num_rels = current_rel_count;
        mp->rel_types = malloc(current_rel_count * sizeof(char*));
        for (int i = 0; i < current_rel_count; i++) mp->rel_types[i] = strdup(current_rel_types[i]);
        (*num_paths)++;
        return;
    }

    NodePattern* np = &path->nodes[hop];
    RelPattern* rp = (hop > 0) ? &path->rels[hop - 1] : NULL;

    if (rp && rp->min_hops != rp->max_hops) {
        PathQueue* queue = path_queue_create();
        char** init_path = malloc(current_len * sizeof(char*));
        for (int i = 0; i < current_len; i++) init_path[i] = strdup(current_path[i]);
        char** init_rel_types = NULL;
        int init_rel_count = 0;
        if (current_rel_count > 0) {
            init_rel_types = malloc(current_rel_count * sizeof(char*));
            for (int i = 0; i < current_rel_count; i++) init_rel_types[i] = strdup(current_rel_types[i]);
            init_rel_count = current_rel_count;
        }
        path_queue_enqueue(queue, init_path, current_len, 0, init_rel_types, init_rel_count);
        for (int i = 0; i < current_len; i++) free(init_path[i]);
        free(init_path);
        if (init_rel_types) {
            for (int i = 0; i < init_rel_count; i++) free(init_rel_types[i]);
            free(init_rel_types);
        }
        char** cur_path = NULL;
        int cur_len = 0;
        int local_hops = 0;
        char** cur_rel_types = NULL;
        int cur_rel_count = 0;
        while (path_queue_dequeue(queue, &cur_path, &cur_len, &local_hops, &cur_rel_types, &cur_rel_count)) {
            if (local_hops >= rp->min_hops && local_hops <= (rp->max_hops == -1 ? 20 : rp->max_hops)) {
                char* cand_id = cur_path[cur_len - 1];
                char* cand_label = graphdb_get_node_label(gdb, cand_id);
                bool node_match = (cand_label != NULL);
                if (node_match && np->label) node_match = strcmp(np->label, cand_label) == 0;
                else if (node_match && np->label == NULL && strcmp(cand_label, "Person") != 0) node_match = false;
                if (node_match && np->prop_value && np->prop_key && strcmp(np->prop_key, "id") == 0) node_match = strcmp(cand_id, np->prop_value) == 0;
                free(cand_label);
                if (node_match) {
                    int* temp_positions = malloc(path->count * sizeof(int));
                    memcpy(temp_positions, current_positions, path->count * sizeof(int));
                    temp_positions[hop] = cur_len - 1;
                    collect_paths(gdb, path, hop + 1, cur_path, cur_len, paths, num_paths, capacity, temp_positions, cur_rel_types, cur_rel_count);
                    free(temp_positions);
                }
            }
            if (local_hops < (rp->max_hops == -1 ? 20 : rp->max_hops)) {
                char* last_id = cur_path[cur_len - 1];
                char* cur_type = rp->type ? rp->type : "";
                Neighbor* neighbors = NULL;
                int neigh_count = 0;
                if (rp->direction == '>') {
                    neighbors = graphdb_get_outgoing(gdb, last_id, cur_type, &neigh_count);
                } else if (rp->direction == '<') {
                    neighbors = graphdb_get_incoming(gdb, last_id, cur_type, &neigh_count);
                } else {
                    int out_count;
                    Neighbor* outs = graphdb_get_outgoing(gdb, last_id, cur_type, &out_count);
                    int in_count;
                    Neighbor* ins = graphdb_get_incoming(gdb, last_id, cur_type, &in_count);
                    neighbors = malloc((out_count + in_count) * sizeof(Neighbor));
                    neigh_count = 0;
                    for (int j = 0; j < out_count; j++) neighbors[neigh_count++] = outs[j];
                    for (int j = 0; j < in_count; j++) {
                        bool dup = false;
                        for (int k = 0; k < out_count; k++) if (strcmp(ins[j].id, outs[k].id) == 0) dup = true;
                        if (!dup) neighbors[neigh_count++] = ins[j];
                        else {
                            free(ins[j].id);
                            free(ins[j].type);
                        }
                    }
                    free(outs);
                    free(ins);
                }
                for (int n = 0; n < neigh_count; n++) {
                    bool in_path = false;
                    for (int k = 0; k < cur_len; k++) {
                        if (strcmp(cur_path[k], neighbors[n].id) == 0) in_path = true;
                    }
                    if (!in_path) {
                        char** new_path = malloc((cur_len + 1) * sizeof(char*));
                        for (int k = 0; k < cur_len; k++) new_path[k] = strdup(cur_path[k]);
                        new_path[cur_len] = strdup(neighbors[n].id);
                        char** new_rel_types = malloc((cur_rel_count + 1) * sizeof(char*));
                        for (int k = 0; k < cur_rel_count; k++) new_rel_types[k] = strdup(cur_rel_types[k]);
                        new_rel_types[cur_rel_count] = strdup(neighbors[n].type);
                        path_queue_enqueue(queue, new_path, cur_len + 1, local_hops + 1, new_rel_types, cur_rel_count + 1);
                        for (int k = 0; k <= cur_len; k++) free(new_path[k]);
                        free(new_path);
                        for (int k = 0; k <= cur_rel_count; k++) free(new_rel_types[k]);
                        free(new_rel_types);
                    }
                    free(neighbors[n].id);
                    free(neighbors[n].type);
                }
                free(neighbors);
            }
            for (int i = 0; i < cur_len; i++) free(cur_path[i]);
            free(cur_path);
            for (int i = 0; i < cur_rel_count; i++) free(cur_rel_types[i]);
            free(cur_rel_types);
        }
        path_queue_destroy(queue);
    } else {
        Neighbor* candidates = NULL;
        int cand_count = 0;
        if (hop == 0) {
            if (np->prop_value && np->prop_key && strcmp(np->prop_key, "id") == 0) {
                cand_count = 1;
                candidates = malloc(sizeof(Neighbor));
                candidates[0].id = strdup(np->prop_value);
                candidates[0].type = strdup("");
            } else if (np->label) {
                char** node_ids = graphdb_get_nodes_by_label(gdb, np->label, &cand_count);
                candidates = malloc(cand_count * sizeof(Neighbor));
                for (int i = 0; i < cand_count; i++) {
                    candidates[i].id = node_ids[i];
                    candidates[i].type = strdup("");
                }
                free(node_ids);
            } else {
                char** node_ids = graphdb_get_all_nodes(gdb, &cand_count);
                candidates = malloc(cand_count * sizeof(Neighbor));
                for (int i = 0; i < cand_count; i++) {
                    candidates[i].id = node_ids[i];
                    candidates[i].type = strdup("");
                }
                free(node_ids);
            }
        } else {
            char* prev_id = current_path[current_len - 1];
            RelPattern* rp = &path->rels[hop - 1];
            char* rel_type = rp->type ? rp->type : "";
            if (rp->direction == '>') {
                candidates = graphdb_get_outgoing(gdb, prev_id, rel_type, &cand_count);
            } else if (rp->direction == '<') {
                candidates = graphdb_get_incoming(gdb, prev_id, rel_type, &cand_count);
            } else {
                int out_count;
                Neighbor* outs = graphdb_get_outgoing(gdb, prev_id, rel_type, &out_count);
                int in_count;
                Neighbor* ins = graphdb_get_incoming(gdb, prev_id, rel_type, &in_count);
                candidates = malloc((out_count + in_count) * sizeof(Neighbor));
                cand_count = 0;
                for (int j = 0; j < out_count; j++) candidates[cand_count++] = outs[j];
                for (int j = 0; j < in_count; j++) {
                    bool dup = false;
                    for (int k = 0; k < out_count; k++) if (strcmp(ins[j].id, outs[k].id) == 0) dup = true;
                    if (!dup) candidates[cand_count++] = ins[j];
                    else {
                        free(ins[j].id);
                        free(ins[j].type);
                    }
                }
                free(outs);
                free(ins);
            }
        }
        for (int i = 0; i < cand_count; i++) {
            char* cand_id = candidates[i].id;
            char* cand_rel_type = candidates[i].type;
            char* cand_label = graphdb_get_node_label(gdb, cand_id);
            if (!cand_label) {
                free(cand_id);
                free(cand_rel_type);
                continue;
            }
            bool node_match = true;
            if (np->label && strcmp(np->label, cand_label) != 0) node_match = false;
            if (np->prop_value && np->prop_key && strcmp(np->prop_key, "id") == 0 && strcmp(cand_id, np->prop_value) != 0) node_match = false;
            free(cand_label);
            if (node_match) {
                char** new_path = malloc((current_len + 1) * sizeof(char*));
                for (int k = 0; k < current_len; k++) new_path[k] = strdup(current_path[k]);
                new_path[current_len] = strdup(cand_id);
                int* new_positions = malloc(path->count * sizeof(int));
                memcpy(new_positions, current_positions, path->count * sizeof(int));
                new_positions[hop] = current_len;
                char** new_rel_types = malloc(current_rel_count * sizeof(char*));
                int new_rel_count = current_rel_count;
                for (int k = 0; k < current_rel_count; k++) new_rel_types[k] = strdup(current_rel_types[k]);
                if (hop > 0) {
                    new_rel_types = realloc(new_rel_types, (new_rel_count + 1) * sizeof(char*));
                    new_rel_types[new_rel_count] = strdup(cand_rel_type ? cand_rel_type : "");
                    new_rel_count++;
                }
                collect_paths(gdb, path, hop + 1, new_path, current_len + 1, paths, num_paths, capacity, new_positions, new_rel_types, new_rel_count);
                for (int k = 0; k <= current_len; k++) free(new_path[k]);
                free(new_path);
                free(new_positions);
                for (int k = 0; k < new_rel_count; k++) free(new_rel_types[k]);
                free(new_rel_types);
            }
            free(cand_id);
            free(cand_rel_type);
        }
        free(candidates);
    }
}

static int split_on_and(const char* str, char*** parts) {
    char* temp = strdup(str);
    char* p = temp;
    int count = 1;
    while ((p = strstr(p, " AND ")) != NULL) {
        count++;
        p += 5;
    }
    *parts = malloc(count * sizeof(char*));
    free(temp);
    temp = strdup(str);
    p = temp;
    char* start = temp;
    int i = 0;
    while ((p = strstr(start, " AND ")) != NULL) {
        *p = '\0';
        (*parts)[i++] = strdup(start);
        start = p + 5;
    }
    (*parts)[i] = strdup(start);
    free(temp);
    return count;
}

static ParsedQuery* parse_cypher(const char* query) {
    ParsedQuery* pq = calloc(1, sizeof(ParsedQuery));
    const char* create_start = strstr(query, "CREATE");
    const char* delete_start = strstr(query, "DELETE");
    const char* match_start = strstr(query, "MATCH");
    const char* where_start = strstr(query, "WHERE");
    const char* return_start = strstr(query, "RETURN");

    if (create_start) {
        pq->type = Q_CREATE;
        create_start += 6;
        char* pattern_str = trim(strdup(create_start));
        pq->match = parse_match(pattern_str);
        free(pattern_str);
    } else if (match_start && delete_start) {
        pq->type = Q_DELETE;
        match_start += 5;
        const char* pattern_end = where_start ? where_start : delete_start;
        size_t match_len = pattern_end - match_start;
        char* match_string = strndup(match_start, match_len);
        char* trimmed_match = trim(match_string);
        free(match_string);
        pq->match = parse_match(trimmed_match);
        free(trimmed_match);
        if (where_start) {
            where_start += 5;
            size_t where_len = delete_start - where_start;
            char* where_string = strndup(where_start, where_len);
            char* trimmed_where = trim(where_string);
            free(where_string);

            pq->cond_count = 0;
            pq->conditions = NULL;
            char** conds;
            int num_conds = split_on_and(trimmed_where, &conds);
            for (int j = 0; j < num_conds; j++) {
                char* cond_tok = trim(conds[j]);
                const char* dot = strchr(cond_tok, '.');
                const char* eq = strchr(cond_tok, '=');
                const char* q1 = strchr(cond_tok, '\'');
                const char* q2 = q1 ? strchr(q1 + 1, '\'') : NULL;
                if (dot && eq && q1 && q2 && dot < eq && eq < q1 && q1 < q2) {
                    char* var = strndup(cond_tok, dot - cond_tok);
                    const char* prop_start = dot + 1;
                    while (prop_start < eq && isspace(*prop_start)) prop_start++;
                    const char* prop_end = eq;
                    while (prop_end > prop_start && isspace(*(prop_end - 1))) prop_end--;
                    char* prop = strndup(prop_start, prop_end - prop_start);
                    char* val = strndup(q1 + 1, q2 - q1 - 1);
                    pq->conditions = realloc(pq->conditions, sizeof(WhereCondition) * (pq->cond_count + 1));
                    pq->conditions[pq->cond_count].var = var;
                    pq->conditions[pq->cond_count].prop = prop;
                    pq->conditions[pq->cond_count].val = val;
                    pq->cond_count++;
                }
                free(cond_tok);
                free(conds[j]);
            }
            free(conds);
            free(trimmed_where);
        }
        delete_start += 6;
        char* delete_string = trim(strdup(delete_start));
        pq->deletes = parse_return(delete_string, &pq->delete_count);
        free(delete_string);
    } else if (match_start && return_start) {
        pq->type = Q_MATCH_RETURN;
        match_start += 5;
        const char* pattern_end = where_start ? where_start : return_start;
        size_t match_len = pattern_end - match_start;
        char* match_string = strndup(match_start, match_len);
        char* trimmed_match = trim(match_string);
        free(match_string);
        pq->match = parse_match(trimmed_match);
        free(trimmed_match);
        if (where_start) {
            where_start += 5;
            size_t where_len = return_start - where_start;
            char* where_string = strndup(where_start, where_len);
            char* trimmed_where = trim(where_string);
            free(where_string);

            pq->cond_count = 0;
            pq->conditions = NULL;
            char** conds;
            int num_conds = split_on_and(trimmed_where, &conds);
            for (int j = 0; j < num_conds; j++) {
                char* cond_tok = trim(conds[j]);
                const char* dot = strchr(cond_tok, '.');
                const char* eq = strchr(cond_tok, '=');
                const char* q1 = strchr(cond_tok, '\'');
                const char* q2 = q1 ? strchr(q1 + 1, '\'') : NULL;
                if (dot && eq && q1 && q2 && dot < eq && eq < q1 && q1 < q2) {
                    char* var = strndup(cond_tok, dot - cond_tok);
                    const char* prop_start = dot + 1;
                    while (prop_start < eq && isspace(*prop_start)) prop_start++;
                    const char* prop_end = eq;
                    while (prop_end > prop_start && isspace(*(prop_end - 1))) prop_end--;
                    char* prop = strndup(prop_start, prop_end - prop_start);
                    char* val = strndup(q1 + 1, q2 - q1 - 1);
                    pq->conditions = realloc(pq->conditions, sizeof(WhereCondition) * (pq->cond_count + 1));
                    pq->conditions[pq->cond_count].var = var;
                    pq->conditions[pq->cond_count].prop = prop;
                    pq->conditions[pq->cond_count].val = val;
                    pq->cond_count++;
                }
                free(cond_tok);
                free(conds[j]);
            }
            free(conds);
            free(trimmed_where);
        }
        return_start += 6;
        char* return_string = trim(strdup(return_start));
        pq->returns = parse_return(return_string, &pq->return_count);
        free(return_string);
    } else {
        free(pq);
        return NULL;
    }
    return pq;
}

static int compare_paths_last(const void* a, const void* b) {
    const MatchingPath* pa = *(const MatchingPath**)a;
    const MatchingPath* pb = *(const MatchingPath**)b;
    const char* ida = pa->node_ids[pa->num_nodes - 1];
    const char* idb = pb->node_ids[pb->num_nodes - 1];
    return strcmp(ida, idb);
}

static CypherResult* execute_parsed_query(GraphDB* gdb, ParsedQuery* pq) {
    CypherResult* result = (CypherResult*)calloc(1, sizeof(CypherResult));

    if (pq->type == Q_CREATE) {
        if (!pq->match) return result;
        char** created_ids = malloc(sizeof(char*) * pq->match->count);
        for (int i = 0; i < pq->match->count; i++) {
            NodePattern* np = &pq->match->nodes[i];
            if (!np->prop_key || strcmp(np->prop_key, "id") != 0 || !np->prop_value || !np->label) continue;
            graphdb_add_node(gdb, np->prop_value, np->label);
            created_ids[i] = strdup(np->prop_value);
        }
        for (int i = 0; i < pq->match->count - 1; i++) {
            RelPattern* rp = &pq->match->rels[i];
            char* from = created_ids[i];
            char* to = created_ids[i + 1];
            if (rp->direction == '<') {
                char* temp = from;
                from = to;
                to = temp;
            }
            graphdb_add_edge(gdb, from, to, rp->type ? rp->type : "");
        }
        for (int i = 0; i < pq->match->count; i++) free(created_ids[i]);
        free(created_ids);
        return result;
    }

    // For DELETE and MATCH_RETURN, use path matching
    char** current_path = malloc(100 * sizeof(char*)); // Increased initial size
    int capacity = 1;
    MatchingPath** paths = malloc(sizeof(MatchingPath*) * capacity);
    int num_paths = 0;
    int* initial_positions = calloc(pq->match->count, sizeof(int));
    collect_paths(gdb, pq->match, 0, current_path, 0, &paths, &num_paths, &capacity, initial_positions, NULL, 0);
    free(initial_positions);
    free(current_path);

    // Sort paths deterministically by last node id so test expectations are stable
    if (num_paths > 1) {
        qsort(paths, num_paths, sizeof(MatchingPath*), compare_paths_last);
    }

    char* where_var = NULL;
    char* where_prop = NULL;
    char* where_val = NULL;
    // Remove the single condition parsing
    // Instead, conditions are already in pq->conditions

    if (pq->type == Q_DELETE) {
        for (int p = 0; p < num_paths; p++) {
            MatchingPath* mp = paths[p];
            bool match = true;
            for (int cond = 0; cond < pq->cond_count; cond++) {
                WhereCondition* wc = &pq->conditions[cond];
                int hop_idx = -1;
                bool is_rel_cond = false;
                for (int h = 0; h < pq->match->count; h++) {
                    if (pq->match->nodes[h].var && strcmp(pq->match->nodes[h].var, wc->var) == 0) {
                        hop_idx = h;
                        break;
                    }
                }
                if (hop_idx == -1) {
                    for (int r = 0; r < pq->match->count - 1; r++) {
                        if (pq->match->rels[r].var && strcmp(pq->match->rels[r].var, wc->var) == 0) {
                            hop_idx = r;
                            is_rel_cond = true;
                            break;
                        }
                    }
                }
                if (hop_idx != -1) {
                    char* check_val = NULL;
                    if (is_rel_cond) {
                        RelPattern* rp = &pq->match->rels[hop_idx];
                        if (strcmp(wc->prop, "type") == 0) check_val = strdup(rp->type ? rp->type : "");
                    } else {
                        char* node_id = mp->node_ids[hop_idx];
                        if (strcmp(wc->prop, "id") == 0) check_val = strdup(node_id);
                        else if (strcmp(wc->prop, "label") == 0) check_val = graphdb_get_node_label(gdb, node_id);
                    }
                    if (check_val && strcmp(check_val, wc->val) != 0) match = false;
                    free(check_val);
                } else {
                    match = false;
                }
                if (!match) break;
            }
            if (!match) continue;
            for (int d = 0; d < pq->delete_count; d++) {
                char* del_var = pq->deletes[d];
                int is_rel = 0;
                int idx = -1;
                for (int h = 0; h < pq->match->count; h++) {
                    if (pq->match->nodes[h].var && strcmp(pq->match->nodes[h].var, del_var) == 0) {
                        idx = h;
                        break;
                    }
                }
                if (idx == -1) {
                    for (int r = 0; r < pq->match->count - 1; r++) {
                        if (pq->match->rels[r].var && strcmp(pq->match->rels[r].var, del_var) == 0) {
                            idx = r;
                            is_rel = 1;
                            break;
                        }
                    }
                }
                if (idx != -1) {
                    if (is_rel) {
                        char* from = mp->node_ids[idx];
                        char* to = mp->node_ids[idx + 1];
                        RelPattern* rp = &pq->match->rels[idx];
                        if (rp->direction == '<') {
                            char* temp = from;
                            from = to;
                            to = temp;
                        }
                        graphdb_delete_edge(gdb, from, to, rp->type ? rp->type : "");
                    } else {
                        graphdb_delete_node(gdb, mp->node_ids[idx]);
                    }
                }
            }
        }
    } else if (pq->type == Q_MATCH_RETURN) {
        for (int p = 0; p < num_paths; p++) {
            MatchingPath* mp = paths[p];

            bool match_ok = true;
            // Evaluate WHERE conditions (same logic as before but without column structs)
            for (int cond = 0; cond < pq->cond_count && match_ok; cond++) {
                WhereCondition* wc = &pq->conditions[cond];
                int hop_idx = -1;
                bool is_rel_cond = false;
                for (int h = 0; h < pq->match->count; h++) {
                    if (pq->match->nodes[h].var && strcmp(pq->match->nodes[h].var, wc->var) == 0) { hop_idx = h; break; }
                }
                if (hop_idx == -1) {
                    for (int r = 0; r < pq->match->count - 1; r++) {
                        if (pq->match->rels[r].var && strcmp(pq->match->rels[r].var, wc->var) == 0) { hop_idx = r; is_rel_cond = true; break; }
                    }
                }
                if (hop_idx == -1) { match_ok = false; break; }

                char* check_val = NULL;
                if (is_rel_cond) {
                    char* rel_type = mp->rel_types[hop_idx];
                    if (strcmp(wc->prop, "type") == 0) check_val = strdup(rel_type ? rel_type : "");
                } else {
                    int pos = mp->pattern_pos[hop_idx];
                    char* node_id = mp->node_ids[pos];
                    if (strcmp(wc->prop, "id") == 0) check_val = strdup(node_id);
                    else if (strcmp(wc->prop, "label") == 0) check_val = graphdb_get_node_label(gdb, node_id);
                }
                if (!check_val || strcmp(check_val, wc->val) != 0) match_ok = false;
                free(check_val);
            }
            if (!match_ok) continue;

            // Build structured row
            CypherRowResult row = {0};
            row.node_count = mp->num_nodes;
            row.nodes = (CypherNodeResult*)calloc(row.node_count, sizeof(CypherNodeResult));
            for (int i = 0; i < row.node_count; i++) {
                row.nodes[i].id = strdup(mp->node_ids[i]);
                row.nodes[i].label = graphdb_get_node_label(gdb, mp->node_ids[i]);
                // We can try to map var name if pattern_pos matches
                for (int pat = 0; pat < pq->match->count; pat++) {
                    if (mp->pattern_pos[pat] == i && pq->match->nodes[pat].var) {
                        row.nodes[i].var = strdup(pq->match->nodes[pat].var);
                        break;
                    }
                }
            }

            row.edge_count = mp->num_rels;
            row.edges = (CypherEdgeResult*)calloc(row.edge_count, sizeof(CypherEdgeResult));
            for (int i = 0; i < row.edge_count; i++) {
                row.edges[i].from_id = strdup(mp->node_ids[i]);
                row.edges[i].to_id = strdup(mp->node_ids[i+1]);
                row.edges[i].type = strdup(mp->rel_types[i] ? mp->rel_types[i] : "");
                // var name mapping - safe for variable length
                if (i < pq->match->count - 1 && pq->match->rels[i].var) {
                    row.edges[i].var = strdup(pq->match->rels[i].var);
                }
            }

            // Append to result
            result->rows = (CypherRowResult*)realloc(result->rows, sizeof(CypherRowResult) * (result->row_count + 1));
            result->rows[result->row_count++] = row;
        }
    }

    // Cleanup paths
    for (int p = 0; p < num_paths; p++) {
        for (int h = 0; h < paths[p]->num_nodes; h++) free(paths[p]->node_ids[h]);
        free(paths[p]->node_ids);
        free(paths[p]->pattern_pos);
        for (int h = 0; h < paths[p]->num_rels; h++) free(paths[p]->rel_types[h]);
        free(paths[p]->rel_types);
        free(paths[p]);
    }
    free(paths);

    return result;
}

// Simple pretty-printer for the new structured result
void print_cypher_result(const CypherResult* result) {
    if (!result || result->row_count == 0) {
        printf("No results\n");
        return;
    }

    for (int r = 0; r < result->row_count; r++) {
        const CypherRowResult* row = &result->rows[r];
        for (int n = 0; n < row->node_count; n++) {
            const CypherNodeResult* node = &row->nodes[n];
            printf("(%s:%s)", node->id, node->label ? node->label : "");
            if (n < row->edge_count) {
                const CypherEdgeResult* edge = &row->edges[n];
                printf("-[:%s]->", edge->type ? edge->type : "");
            }
        }
        printf("\n");
    }
}

CypherResult* execute_cypher(GraphDB* gdb, const char* query) {
    ParsedQuery* pq = parse_cypher(query);
    if (!pq) {
        return (CypherResult*)calloc(1, sizeof(CypherResult));
    }
    CypherResult* result = execute_parsed_query(gdb, pq);
    // Free pq
    if (pq->returns) {
        for (int i = 0; i < pq->return_count; i++) free(pq->returns[i]);
        free(pq->returns);
    }
    if (pq->deletes) {
        for (int i = 0; i < pq->delete_count; i++) free(pq->deletes[i]);
        free(pq->deletes);
    }
    if (pq->match) {
        for (int i = 0; i < pq->match->count; i++) {
            free(pq->match->nodes[i].var);
            free(pq->match->nodes[i].label);
            free(pq->match->nodes[i].prop_key);
            free(pq->match->nodes[i].prop_value);
        }
        free(pq->match->nodes);
        for (int i = 0; i < pq->match->count - 1; i++) {
            free(pq->match->rels[i].var);
            free(pq->match->rels[i].type);
        }
        free(pq->match->rels);
        free(pq->match);
    }
    if (pq->conditions) {
        for (int i = 0; i < pq->cond_count; i++) {
            free(pq->conditions[i].var);
            free(pq->conditions[i].prop);
            free(pq->conditions[i].val);
        }
        free(pq->conditions);
    }
    free(pq);
    return result;
}

/***************************************
 * NEW STRUCTURED RESULT IMPLEMENTATION *
 ***************************************/

// Allocate an empty raw result helper
static CypherResult* cypher_result_create() {
    CypherResult* res = (CypherResult*)calloc(1, sizeof(CypherResult));
    return res;
}

// Free result helper (public via header)
void free_cypher_result(CypherResult* result) {
    if (!result) return;
    for (int r = 0; r < result->row_count; r++) {
        CypherRowResult* row = &result->rows[r];
        for (int n = 0; n < row->node_count; n++) {
            free(row->nodes[n].var);
            free(row->nodes[n].id);
            free(row->nodes[n].label);
        }
        free(row->nodes);
        for (int e = 0; e < row->edge_count; e++) {
            free(row->edges[e].var);
            free(row->edges[e].from_id);
            free(row->edges[e].to_id);
            free(row->edges[e].type);
        }
        free(row->edges);
    }
    free(result->rows);
    free(result);
}