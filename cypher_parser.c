// cypher_parser.c
#include "graphdb.h"
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <ctype.h>

// Assume graphdb.h is included which declares GraphDB and functions like graphdb_get_outgoing, etc.
// For example:
// typedef struct GraphDB GraphDB;
// GraphDB* graphdb_open(const char* path);
// void graphdb_close(GraphDB* gdb);
// char** graphdb_get_outgoing(GraphDB* gdb, const char* node, const char* type, int* count);
// etc.

// Basic structure for query results
typedef struct {
    char** columns;
    int column_count;
    char*** rows;
    int row_count;
} CypherResult;

void free_cypher_result(CypherResult* result) {
    if (result) {
        for (int i = 0; i < result->column_count; i++) free(result->columns[i]);
        free(result->columns);
        for (int i = 0; i < result->row_count; i++) {
            for (int j = 0; j < result->column_count; j++) free(result->rows[i][j]);
            free(result->rows[i]);
        }
        free(result->rows);
        free(result);
    }
}

// Extended parser structures
typedef struct {
    char* var;
    char* label;
    char* prop_key;
    char* prop_value;
} NodePattern;

typedef struct {
    char* var;
    char* type;
    char direction;  // '>' for outgoing, '<' for incoming, 0 for undirected
} RelPattern;

typedef struct {
    NodePattern* nodes;
    RelPattern* rels;
    int count; // number of nodes
} PathPattern;

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

// Parse rel pattern like -[:Type]- or <-[:Type]- or -[:Type]->
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
    RelPattern* rp = (RelPattern*)calloc(1, sizeof(RelPattern));
    // Variable
    const char* start = *pattern;
    while (**pattern && **pattern != ':' && **pattern != ']') (*pattern)++;
    if (*pattern > start) rp->var = strndup(start, *pattern - start);
    // Type
    if (**pattern == ':') {
        (*pattern)++;
        start = *pattern;
        while (**pattern && **pattern != ']') (*pattern)++;
        rp->type = strndup(start, *pattern - start);
    }
    (*pattern)++;
    *pattern = skip_ws(*pattern);
    /*
     * At this point we have just skipped the closing ']' of the relationship
     * specification and are positioned on the characters that form the arrow
     * (typically "-" or "->" for outgoing, "-" when the arrow started with
     * '<-' for incoming, or just '-' for undirected). The original logic
     * inspected the '>' *before* advancing past this dash, leaving the '>'
     * character un-consumed (e.g. in the pattern "-[:TYPE]->"). That leftover
     * character caused the next parse_node call to fail. We now consume the
     * trailing part of the arrow properly and set the direction.
     */

    if (**pattern == '-') {
        /* Consume the dash following ']' */
        (*pattern)++;
        /* If the next character is '>', it's an outgoing relationship */
        if (**pattern == '>') {
            rp->direction = '>';
            (*pattern)++;  /* Consume '>' */
        } else if (start_dir == '<') {
            /* We had "<-[:TYPE]-" so this is incoming */
            rp->direction = '<';
        }
    } else if (**pattern == '>') {
        /* Fallback: we are directly on a '>' (should be rare) */
        rp->direction = '>';
        (*pattern)++;
    } else if (start_dir == '<') {
        /* Incoming relationship where the leading '<' was already consumed */
        rp->direction = '<';
    }
    *pattern = skip_ws(*pattern);
    return rp;
}

// Parse full path pattern (multi-hop support, but execution assumes single-hop)
static PathPattern* parse_match(const char* match_str) {
    const char* p = match_str;
    PathPattern* path = (PathPattern*)calloc(1, sizeof(PathPattern));
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

// Extended execute_cypher
CypherResult* execute_cypher(GraphDB* gdb, const char* query) {
    CypherResult* result = (CypherResult*)malloc(sizeof(CypherResult));
    result->column_count = 0;
    result->columns = NULL;
    result->row_count = 0;
    result->rows = NULL;

    const char* create_start = strstr(query, "CREATE");
    const char* delete_start = strstr(query, "DELETE");
    const char* match_start = strstr(query, "MATCH");
    const char* where_start = strstr(query, "WHERE");
    const char* return_start = strstr(query, "RETURN");
    
    if (create_start) {
        create_start += 6;
        char* pattern_str = trim(strdup(create_start));
        PathPattern* path = parse_match(pattern_str);
        free(pattern_str);
        if (!path) {
            printf("Invalid CREATE pattern\n");
            return result;
        }
        char** created_ids = (char**)malloc(sizeof(char*) * path->count);
        for (int i = 0; i < path->count; i++) {
            NodePattern* np = &path->nodes[i];
            if (!np->prop_key || strcmp(np->prop_key, "id") != 0 || !np->prop_value || !np->label) {
                printf("Node must have label and {id: 'val'}\n");
                continue;
            }
            graphdb_add_node(gdb, np->prop_value, np->label);
            created_ids[i] = strdup(np->prop_value);
        }
        for (int i = 0; i < path->count - 1; i++) {
            RelPattern* rp = &path->rels[i];
            char* from = created_ids[i];
            char* to = created_ids[i+1];
            if (rp->direction == '<') {
                char* temp = from;
                from = to;
                to = temp;
            }
            graphdb_add_edge(gdb, from, to, rp->type ? rp->type : "");
        }
        for (int i = 0; i < path->count; i++) free(created_ids[i]);
        free(created_ids);
        // Free path
        for (int i = 0; i < path->count; i++) {
            free(path->nodes[i].var);
            free(path->nodes[i].label);
            free(path->nodes[i].prop_key);
            free(path->nodes[i].prop_value);
        }
        free(path->nodes);
        for (int i = 0; i < path->count - 1; i++) {
            free(path->rels[i].var);
            free(path->rels[i].type);
        }
        free(path->rels);
        free(path);
        return result;
    }
    if (match_start && delete_start) {
        match_start += 5;
        const char* pattern_end = where_start ? where_start : delete_start;
        size_t match_len = pattern_end - match_start;
        char* match_string = strndup(match_start, match_len);
        char* trimmed_match = trim(match_string);
        free(match_string);
        PathPattern* path = parse_match(trimmed_match);
        free(trimmed_match);
        if (!path) {
            printf("Invalid MATCH pattern for DELETE\n");
            return result;
        }
        if (path->count == 1) {
            /* ----------------------------------------------
             * Handle DELETE where only a single node pattern
             * is provided, e.g.:
             *   MATCH (a) WHERE a.id = 'Mark' DELETE a
             * ------------------------------------------- */

            NodePattern* np = &path->nodes[0];

            /* Parse WHERE clause (already stored in where_string) */
            char* where_string = NULL;
            if (where_start) {
                where_start += 5; /* skip "WHERE" */
                size_t where_len = delete_start - where_start; /* minus length of "DELETE" processed later */
                where_string = strndup(where_start, where_len);
                char* trimmed_where = trim(where_string);
                free(where_string);
                where_string = trimmed_where;
            }

            /* Parse variables to delete */
            delete_start += 6; /* skip "DELETE" */
            char* delete_string = trim(strdup(delete_start));
            char** delete_vars = NULL;
            int delete_count = 0;
            char* tok = strtok(delete_string, ",");
            while (tok) {
                tok = trim(tok);
                delete_vars = (char**)realloc(delete_vars, sizeof(char*) * (delete_count + 1));
                delete_vars[delete_count++] = strdup(tok);
                tok = strtok(NULL, ",");
            }
            free(delete_string);

            /* Simple WHERE parse */
            char* where_var = NULL;
            char* where_prop = NULL;
            char* where_val = NULL;
            if (where_string) {
                const char* dot = strchr(where_string, '.');
                const char* eq  = strchr(where_string, '=');
                const char* q1  = strchr(where_string, '\'');
                const char* q2  = q1 ? strchr(q1 + 1, '\'') : NULL;
                if (dot && eq && q1 && q2 && dot < eq && q1 < q2) {
                    where_var = strndup(where_string, dot - where_string);
                    const char* prop_start = dot + 1;
                    while (prop_start < eq && isspace(*prop_start)) prop_start++;
                    const char* prop_end   = eq;
                    while (prop_end > prop_start && isspace(*(prop_end - 1))) prop_end--;
                    where_prop = strndup(prop_start, prop_end - prop_start);
                    where_val  = strndup(q1 + 1, q2 - q1 - 1);
                }
            }

            /* Determine candidate nodes */
            int cand_count = 0;
            char** candidates = NULL;
            if (np->prop_value && strcmp(np->prop_key, "id") == 0) {
                candidates = (char**)malloc(sizeof(char*));
                candidates[0] = strdup(np->prop_value);
                cand_count = 1;
            } else if (np->label) {
                candidates = graphdb_get_nodes_by_label(gdb, np->label, &cand_count);
            } else {
                candidates = graphdb_get_all_nodes(gdb, &cand_count);
            }

            for (int i = 0; i < cand_count; i++) {
                char* node_id = candidates[i];
                char* node_label = graphdb_get_node_label(gdb, node_id);
                if (!node_label) continue;

                if (np->label && strcmp(node_label, np->label) != 0) {
                    free(node_label);
                    continue;
                }

                if (where_string && where_var && strcmp(where_var, np->var) == 0) {
                    char* check_val = strcmp(where_prop, "id") == 0 ? node_id : (strcmp(where_prop, "label") == 0 ? node_label : NULL);
                    if (check_val && strcmp(check_val, where_val) != 0) {
                        free(node_label);
                        continue;
                    }
                }

                /* Perform deletion if variable matches */
                for (int d = 0; d < delete_count; d++) {
                    if (strcmp(delete_vars[d], np->var) == 0) {
                        graphdb_delete_node(gdb, node_id);
                    }
                }

                free(node_label);
            }

            for (int i = 0; i < cand_count; i++) free(candidates[i]);
            free(candidates);
            for (int d = 0; d < delete_count; d++) free(delete_vars[d]);
            free(delete_vars);
            if (where_string) free(where_string);
            if (where_var) free(where_var);
            if (where_prop) free(where_prop);
            if (where_val) free(where_val);

            /* Free path structures */
            for (int i = 0; i < path->count; i++) {
                free(path->nodes[i].var);
                free(path->nodes[i].label);
                free(path->nodes[i].prop_key);
                free(path->nodes[i].prop_value);
            }
            free(path->nodes);
            if (path->rels) free(path->rels);
            free(path);

            return result;
        }

        /* Fallback: only support single-hop (2-node) delete paths for now */
        if (path->count != 2) {
            printf("Only single-hop paths supported for DELETE\n");
            free(path->nodes);
            free(path->rels);
            free(path);
            return result;
        }

        char* where_string = NULL;
        if (where_start) {
            where_start += 5;
            size_t where_len = delete_start - where_start;
            where_string = strndup(where_start, where_len);
            char* trimmed_where = trim(where_string);
            free(where_string);
            where_string = trimmed_where;
        }

        delete_start += 6;
        char* delete_string = trim(strdup(delete_start));
        char** delete_vars = NULL;
        int delete_count = 0;
        char* tok = strtok(delete_string, ",");
        while (tok) {
            tok = trim(tok);
            delete_vars = (char**)realloc(delete_vars, sizeof(char*) * (delete_count + 1));
            delete_vars[delete_count++] = strdup(tok);
            tok = strtok(NULL, ",");
        }
        free(delete_string);

        char* where_var = NULL;
        char* where_prop = NULL;
        char* where_val = NULL;
        if (where_string) {
            const char* dot = strchr(where_string, '.');
            const char* eq  = strchr(where_string, '=');
            const char* q1  = strchr(where_string, '\'');
            const char* q2  = q1 ? strchr(q1 + 1, '\'') : NULL;
            if (dot && eq && q1 && q2 && dot < eq && q1 < q2) {
                where_var = strndup(where_string, dot - where_string);
                const char* prop_start = dot + 1;
                while (prop_start < eq && isspace(*prop_start)) prop_start++;
                const char* prop_end   = eq;
                while (prop_end > prop_start && isspace(*(prop_end - 1))) prop_end--;
                where_prop = strndup(prop_start, prop_end - prop_start);
                where_val = strndup(q1 + 1, q2 - q1 - 1);
            }
        }

        NodePattern* start_np = &path->nodes[0];
        NodePattern* end_np = &path->nodes[1];
        RelPattern* rel = &path->rels[0];

        char** starts = NULL;
        int start_count = 0;
        if (start_np->prop_value && strcmp(start_np->prop_key, "id") == 0) {
            starts = (char**)malloc(sizeof(char*));
            starts[0] = strdup(start_np->prop_value);
            start_count = 1;
        } else if (start_np->label) {
            starts = graphdb_get_nodes_by_label(gdb, start_np->label, &start_count);
        } else {
            starts = graphdb_get_all_nodes(gdb, &start_count);
        }

        for (int s = 0; s < start_count; s++) {
            char* start_id = starts[s];
            char* start_label = graphdb_get_node_label(gdb, start_id);
            if (!start_label) continue;
            if (start_np->label && strcmp(start_label, start_np->label) != 0) {
                free(start_label);
                continue;
            }
            if (where_string && where_var && strcmp(where_var, start_np->var) == 0) {
                char* check_val = strcmp(where_prop, "id") == 0 ? start_id : (strcmp(where_prop, "label") == 0 ? start_label : NULL);
                if (check_val && strcmp(check_val, where_val) != 0) {
                    free(start_label);
                    continue;
                }
            }

            int neigh_count;
            char** neighbors;
            char dir = rel->direction;
            if (dir == '>') {
                neighbors = graphdb_get_outgoing(gdb, start_id, rel->type ? rel->type : "", &neigh_count);
            } else if (dir == '<') {
                neighbors = graphdb_get_incoming(gdb, start_id, rel->type ? rel->type : "", &neigh_count);
            } else {
                free(start_label);
                continue;
            }

            for (int n = 0; n < neigh_count; n++) {
                char* end_id = neighbors[n];
                char* end_label = graphdb_get_node_label(gdb, end_id);
                if (!end_label) {
                    free(end_id);
                    continue;
                }
                if (end_np->label && strcmp(end_label, end_np->label) != 0) {
                    free(end_label);
                    free(end_id);
                    continue;
                }
                if (end_np->prop_value && strcmp(end_np->prop_key, "id") == 0 && strcmp(end_id, end_np->prop_value) != 0) {
                    free(end_label);
                    free(end_id);
                    continue;
                }
                if (where_string && where_var && strcmp(where_var, end_np->var) == 0) {
                    char* check_val = strcmp(where_prop, "id") == 0 ? end_id : (strcmp(where_prop, "label") == 0 ? end_label : NULL);
                    if (check_val && strcmp(check_val, where_val) != 0) {
                        free(end_label);
                        free(end_id);
                        continue;
                    }
                }

                // Delete
                for (int d = 0; d < delete_count; d++) {
                    char* del_var = delete_vars[d];
                    if (start_np->var && strcmp(del_var, start_np->var) == 0) {
                        graphdb_delete_node(gdb, start_id);
                    } else if (end_np->var && strcmp(del_var, end_np->var) == 0) {
                        graphdb_delete_node(gdb, end_id);
                    } else if (rel->var && strcmp(del_var, rel->var) == 0) {
                        char* from = (dir == '>') ? start_id : end_id;
                        char* to = (dir == '>') ? end_id : start_id;
                        graphdb_delete_edge(gdb, from, to, rel->type ? rel->type : "");
                    }
                }
                free(end_label);
                free(end_id);
            }
            free(neighbors);
            free(start_label);
        }
        for (int s = 0; s < start_count; s++) free(starts[s]);
        free(starts);
        for (int d = 0; d < delete_count; d++) free(delete_vars[d]);
        free(delete_vars);
        // Free path and other
        for (int i = 0; i < path->count; i++) {
            free(path->nodes[i].var);
            free(path->nodes[i].label);
            free(path->nodes[i].prop_key);
            free(path->nodes[i].prop_value);
        }
        free(path->nodes);
        for (int i = 0; i < path->count - 1; i++) {
            free(path->rels[i].var);
            free(path->rels[i].type);
        }
        free(path->rels);
        free(path);
        free(where_string);
        free(where_var);
        free(where_prop);
        free(where_val);
        return result;
    }
    if (!match_start || !return_start) {
        printf("Invalid query: missing MATCH or RETURN\n");
        return result;
    }
    match_start += 5; // Skip "MATCH"
    size_t match_len = (where_start ? where_start : return_start) - match_start;
    char* match_string = strndup(match_start, match_len);
    char* trimmed_match = trim(match_string);
    free(match_string);
    PathPattern* path = parse_match(trimmed_match);
    free(trimmed_match);
    if (!path) {
        printf("Invalid MATCH pattern\n");
        return result;
    }

    /* --------------------------------------------------------------------
     * Handle node-only MATCH queries (pattern contains exactly one node and
     * **no** relationships). These are used by the tests, e.g.:
     *   MATCH (n:Person) WHERE n.id = 'NewPerson' RETURN n.id, n.label
     * ------------------------------------------------------------------ */
    if (path->count == 1) {
        NodePattern* np = &path->nodes[0];

        /* --------------------------------------------------------------
         * Parse WHERE clause (if present) just for this branch
         * ----------------------------------------------------------- */
        char* node_where_var  = NULL;
        char* node_where_prop = NULL;
        char* node_where_val  = NULL;

        if (where_start) {
            const char* ws_start = where_start + 5; /* skip "WHERE" */
            size_t ws_len = return_start - ws_start;
            char* where_str = strndup(ws_start, ws_len);
            char* trimmed_where = trim(where_str);
            free(where_str);

            const char* dot = strchr(trimmed_where, '.');
            const char* eq  = strchr(trimmed_where, '=');
            const char* q1  = strchr(trimmed_where, '\'');
            const char* q2  = q1 ? strchr(q1 + 1, '\'') : NULL;

            if (dot && eq && q1 && q2 && dot < eq && q1 < q2) {
                node_where_var  = strndup(trimmed_where, dot - trimmed_where);
                const char* prop_start = dot + 1;
                while (prop_start < eq && isspace(*prop_start)) prop_start++;
                const char* prop_end = eq;
                while (prop_end > prop_start && isspace(*(prop_end - 1))) prop_end--;
                node_where_prop = strndup(prop_start, prop_end - prop_start);
                node_where_val  = strndup(q1 + 1, q2 - q1 - 1);
            }
            free(trimmed_where);
        }

        /* --------------------------------------------------------------
         * Parse RETURN columns for this branch
         * ----------------------------------------------------------- */
        result->column_count = 0;
        result->columns = NULL;
        if (return_start) {
            const char* rs = return_start + 6; /* skip "RETURN" */
            char* return_str = strdup(rs);
            char* trimmed_ret = trim(return_str);
            free(return_str);
            result->columns = parse_return(trimmed_ret, &result->column_count);
            free(trimmed_ret);
        }

        /* Decide initial candidate set */
        char** candidates = NULL;
        int cand_count = 0;
        if (np->prop_value && strcmp(np->prop_key, "id") == 0) {
            candidates = (char**)malloc(sizeof(char*));
            candidates[0] = strdup(np->prop_value);
            cand_count = 1;
        } else if (np->label) {
            candidates = graphdb_get_nodes_by_label(gdb, np->label, &cand_count);
        } else {
            candidates = graphdb_get_all_nodes(gdb, &cand_count);
        }

        for (int i = 0; i < cand_count; i++) {
            char* node_id = candidates[i];
            char* node_label = graphdb_get_node_label(gdb, node_id);
            if (!node_label) continue;

            /* Label filter from the MATCH pattern itself */
            if (np->label && strcmp(node_label, np->label) != 0) {
                free(node_label);
                continue;
            }

            /* Apply optional WHERE filter */
            if (node_where_var && strcmp(node_where_var, np->var) == 0) {
                char* check_val = strcmp(node_where_prop, "id") == 0
                                    ? node_id
                                    : (strcmp(node_where_prop, "label") == 0 ? node_label : NULL);
                if (check_val && strcmp(check_val, node_where_val) != 0) {
                    free(node_label);
                    continue;
                }
            }

            /* Passed all filters – add a row to the result */
            result->row_count++;
            result->rows = (char***)realloc(result->rows, sizeof(char**) * result->row_count);
            result->rows[result->row_count - 1] = (char**)malloc(sizeof(char*) * result->column_count);

            for (int c = 0; c < result->column_count; c++) {
                char* col = result->columns[c];
                char* dot = strchr(col, '.');
                char* var = dot ? strndup(col, dot - col) : strdup(col);
                char* prop = dot ? strdup(dot + 1) : NULL;
                char* value = NULL;

                if (strcmp(var, np->var) == 0) {
                    if (prop && strcmp(prop, "id") == 0)
                        value = strdup(node_id);
                    else if (prop && strcmp(prop, "label") == 0)
                        value = strdup(node_label);
                    else
                        value = strdup(node_id);
                } else {
                    value = strdup("");
                }

                result->rows[result->row_count - 1][c] = value;
                free(var);
                if (prop) free(prop);
            }

            free(node_label);
        }

        /* Clean-up */
        for (int i = 0; i < cand_count; i++) free(candidates[i]);
        free(candidates);
        if (node_where_var) free(node_where_var);
        if (node_where_prop) free(node_where_prop);
        if (node_where_val) free(node_where_val);

        // Free path structures
        for (int i = 0; i < path->count; i++) {
            free(path->nodes[i].var);
            free(path->nodes[i].label);
            free(path->nodes[i].prop_key);
            free(path->nodes[i].prop_value);
        }
        free(path->nodes);
        if (path->rels) free(path->rels);
        free(path);

        return result;
    }

    /* Existing single-hop (node–relationship–node) logic follows */
    if (path->count != 2) {
        printf("Only single-hop paths supported\n");
        for (int i = 0; i < path->count; i++) {
            free(path->nodes[i].var);
            free(path->nodes[i].label);
            free(path->nodes[i].prop_key);
            free(path->nodes[i].prop_value);
        }
        free(path->nodes);
        if (path->rels) {
            for (int i = 0; i < path->count - 1; i++) {
                free(path->rels[i].var);
                free(path->rels[i].type);
            }
            free(path->rels);
        }
        free(path);
        return result;
    }

    /* --------------------------------------------------------------
     * Parse WHERE clause for the single-hop branch (if present)
     * ----------------------------------------------------------- */
    char* where_string = NULL;
    if (where_start) {
        const char* ws_begin = where_start + 5; /* skip "WHERE" */
        size_t ws_len = return_start - ws_begin;
        where_string = strndup(ws_begin, ws_len);
        char* trimmed_where = trim(where_string);
        free(where_string);
        where_string = trimmed_where;
    }

    return_start += 6; // Skip "RETURN"
    char* return_string = strdup(return_start);
    char* trimmed_return = trim(return_string);
    free(return_string);
    result->columns = parse_return(trimmed_return, &result->column_count);
    free(trimmed_return);

    /*
     * Simple WHERE parse for patterns like:  var.prop = 'value'
     * The previous implementation relied on multiple strtok calls that
     * could produce fewer tokens than expected, leading to NULL dereferences
     * and segmentation faults. We now parse the three key components (var,
     * prop and value) explicitly using delimiter searches which are safer
     * and easier to reason about.
     */
    char* where_var = NULL;
    char* where_prop = NULL;
    char* where_val = NULL;
    if (where_string) {
        const char* dot = strchr(where_string, '.');
        const char* eq  = strchr(where_string, '=');
        const char* q1  = strchr(where_string, '\'');
        const char* q2  = q1 ? strchr(q1 + 1, '\'') : NULL;

        if (dot && eq && q1 && q2 && dot < eq && q1 < q2) {
            /* variable */
            where_var = strndup(where_string, dot - where_string);

            /* property (trim trailing spaces) */
            const char* prop_start = dot + 1;
            while (prop_start < eq && isspace(*prop_start)) prop_start++;
            const char* prop_end   = eq;
            while (prop_end > prop_start && isspace(*(prop_end - 1))) prop_end--;
            where_prop = strndup(prop_start, prop_end - prop_start);

            /* value between single quotes */
            where_val = strndup(q1 + 1, q2 - q1 - 1);
        }
    }

    // Execution for single-hop
    NodePattern* start_np = &path->nodes[0];
    NodePattern* end_np = &path->nodes[1];
    RelPattern* rel = &path->rels[0];

    char** starts = NULL;
    int start_count = 0;
    if (start_np->prop_value && strcmp(start_np->prop_key, "id") == 0) {
        starts = (char**)malloc(sizeof(char*));
        starts[0] = strdup(start_np->prop_value);
        start_count = 1;
    } else if (start_np->label) {
        starts = graphdb_get_nodes_by_label(gdb, start_np->label, &start_count);
    } else {
        starts = graphdb_get_all_nodes(gdb, &start_count);
    }

    for (int s = 0; s < start_count; s++) {
        char* start_id = starts[s];
        char* start_label = graphdb_get_node_label(gdb, start_id);
        if (!start_label) continue;
        if (start_np->label && strcmp(start_label, start_np->label) != 0) {
            free(start_label);
            continue;
        }
        // Check WHERE for start
        if (where_string && strcmp(where_var, start_np->var) == 0) {
            char* check_val = strcmp(where_prop, "id") == 0 ? start_id : (strcmp(where_prop, "label") == 0 ? start_label : NULL);
            if (check_val && strcmp(check_val, where_val) != 0) {
                free(start_label);
                continue;
            }
        }

        int neigh_count;
        char** neighbors;
        if (rel->direction == '>') {
            neighbors = graphdb_get_outgoing(gdb, start_id, rel->type ? rel->type : "", &neigh_count);
        } else if (rel->direction == '<') {
            neighbors = graphdb_get_incoming(gdb, start_id, rel->type ? rel->type : "", &neigh_count);
        } else {
            // Undirected, skip for now
            free(start_label);
            continue;
        }

        for (int n = 0; n < neigh_count; n++) {
            char* end_id = neighbors[n];
            char* end_label = graphdb_get_node_label(gdb, end_id);
            if (!end_label) {
                free(end_id);
                continue;
            }
            if (end_np->label && strcmp(end_label, end_np->label) != 0) {
                free(end_label);
                free(end_id);
                continue;
            }
            if (end_np->prop_value && strcmp(end_np->prop_key, "id") == 0 && strcmp(end_id, end_np->prop_value) != 0) {
                free(end_label);
                free(end_id);
                continue;
            }
            // Check WHERE for end
            if (where_string && strcmp(where_var, end_np->var) == 0) {
                char* check_val = strcmp(where_prop, "id") == 0 ? end_id : (strcmp(where_prop, "label") == 0 ? end_label : NULL);
                if (check_val && strcmp(check_val, where_val) != 0) {
                    free(end_label);
                    free(end_id);
                    continue;
                }
            }

            // Add row
            result->row_count++;
            result->rows = (char***)realloc(result->rows, sizeof(char**) * result->row_count);
            result->rows[result->row_count - 1] = (char**)malloc(sizeof(char*) * result->column_count);
            for (int c = 0; c < result->column_count; c++) {
                char* col = result->columns[c];
                char* dot = strchr(col, '.');
                char* var = dot ? strndup(col, dot - col) : strdup(col);
                char* prop = dot ? strdup(dot + 1) : NULL;
                char* value = NULL;
                if (strcmp(var, start_np->var) == 0) {
                    if (prop && strcmp(prop, "id") == 0) value = strdup(start_id);
                    else if (prop && strcmp(prop, "label") == 0) value = strdup(start_label);
                    else value = strdup(start_id);
                } else if (strcmp(var, end_np->var) == 0) {
                    if (prop && strcmp(prop, "id") == 0) value = strdup(end_id);
                    else if (prop && strcmp(prop, "label") == 0) value = strdup(end_label);
                    else value = strdup(end_id);
                } else if (rel->var && strcmp(var, rel->var) == 0) {
                    if (prop && strcmp(prop, "type") == 0) value = strdup(rel->type ? rel->type : "");
                    else value = strdup("");
                } else {
                    value = strdup("");
                }
                result->rows[result->row_count - 1][c] = value;
                free(var);
                if (prop) free(prop);
            }
            free(end_label);
            free(end_id);
        }
        free(neighbors);
        free(start_label);
    }
    for (int s = 0; s < start_count; s++) free(starts[s]);
    free(starts);

    // Free path
    for (int i = 0; i < path->count; i++) {
        free(path->nodes[i].var);
        free(path->nodes[i].label);
        free(path->nodes[i].prop_key);
        free(path->nodes[i].prop_value);
    }
    free(path->nodes);
    for (int i = 0; i < path->count - 1; i++) {
        free(path->rels[i].var);
        free(path->rels[i].type);
    }
    free(path->rels);
    free(path);
    free(where_string);
    free(where_var);
    free(where_prop);
    free(where_val);

    return result;
}

// Function to print results in a Neo4j-like tabular format
void print_cypher_result(CypherResult* result) {
    if (!result || result->row_count == 0) {
        printf("No results\n");
        return;
    }
    // Print headers
    for (int i = 0; i < result->column_count; i++) {
        printf("%s", result->columns[i]);
        if (i < result->column_count - 1) printf(" | ");
    }
    printf("\n");
    // Print rows
    for (int r = 0; r < result->row_count; r++) {
        for (int c = 0; c < result->column_count; c++) {
            printf("%s", result->rows[r][c]);
            if (c < result->column_count - 1) printf(" | ");
        }
        printf("\n");
    }
}