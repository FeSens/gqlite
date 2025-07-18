# GQLite – A Tiny Embedded Graph Database in C

GQLite is an educational, *light-weight* graph database implemented in plain C on top of [RocksDB](https://github.com/facebook/rocksdb).  
It supports a **minimal subset of Cypher** (Neo4j’s query language) and ships with a small CLI for experimentation, a public C API, and an automated test-suite.

---

## 1. Features

* RocksDB key-value storage – no server process required (fully embedded)
* Nodes & directed, typed edges
* Simple label & `id` properties per node
* Sub-set of Cypher:
  * `CREATE` – create nodes and/or a single edge in one statement
  * `MATCH`  – pattern matching on one hop (or single node) with optional `WHERE`
  * `DELETE` – delete nodes or one edge that was previously matched
  * `RETURN` – project any of  `var.id`, `var.label`, or `rel.type`
* Thread-safe internal queues for neighbor pre-fetching
* Portable Makefile (tested on macOS)
* Unity-based unit tests

> ⚠️ This project is **not** intended for production but as a minimal, hackable playground for learning how graph stores work internally.

---

## 2. Building

### 2.1 Dependencies

| Dependency | Purpose | macOS (Homebrew) | Ubuntu / Debian |
|------------|---------|------------------|-----------------|
| C compiler | build C code (C11) | `clang` (or `gcc`) | `build-essential` |
| RocksDB    | storage engine | `brew install rocksdb` | `sudo apt install librocksdb-dev` |
| pthreads   | threading | included | included |

The Makefile assumes RocksDB headers live in one of:

* `/opt/homebrew/opt/rocksdb/include` (default Homebrew prefix on Apple-Silicon)
* `/usr/include` or `/usr/local/include` (typical Linux)

If your headers / libs are in a different path, override at build time:

```bash
make INCLUDES="-I/some/other/rocksdb/include" \
     LIBS="-L/some/other/rocksdb/lib -lrocksdb"
```

### 2.2 Compile everything

```bash
# in the repository root
make             # builds: lib objects, `graph`, `benchmark`, `gqlite_cli`
```

Artifacts:

* `graph`        – tiny demo that hard-codes some graph logic (see `main.c`)
* `benchmark`    – inserts random data & measures throughput
* `gqlite_cli`   – interactive Cypher shell (see below)

---

## 3. Quick Start (CLI)

```bash
./gqlite_cli ./mydb          # opens (or creates) ./mydb directory
GQLite CLI - Enter Cypher queries (type 'exit' to quit)
> CREATE (n:Person {id:'Mark'})
> CREATE (m:Person {id:'Alex'})
> CREATE (n:Person {id:'Mark'})-[:FRIEND]->(m:Person {id:'Alex'})
> MATCH (n)-[:FRIEND]->(m) WHERE n.id='Mark' RETURN m.id, m.label
m.id | m.label
Alex | Person
> exit
```

If *db-path* is omitted the CLI defaults to `./graphdb` in the current directory.

---

## 4. Public C API (snippet)

```c
#include "graphdb.h"
#include "cypher_parser.h"

GraphDB *db = graphdb_open("./exampledb");

// Low-level helpers
graphdb_add_node(db, "Mark", "Person");
graphdb_add_edge(db, "Mark", "Alex", "FRIEND");

// Cypher interface (preferred)
CypherResult *res = execute_cypher(
    db,
    "MATCH (a)-[:FRIEND]->(b) WHERE a.id = 'Mark' RETURN b.id"
);
print_cypher_result(res);
free_cypher_result(res);

graphdb_close(db);
```

See `graphdb.h` & `cypher_parser.h` for the full API surface.

---

## 5. Cypher Grammar Supported

GQLite intentionally supports only a **single hop** (one relationship) per pattern to stay tiny.

### 5.1 CREATE

```cypher
-- Add node(s)
CREATE (n:Label {id:'NodeId'})

-- Add edge & its two nodes in one go
CREATE (a:Person {id:'Mark'})-[:FRIEND]->(b:Person {id:'Alex'})
```

### 5.2 MATCH / RETURN

```cypher
-- Simple traversal
MATCH (a)-[:FRIEND]->(b) WHERE a.id='Mark' RETURN b.id

-- Label filters & multiple projections
MATCH (a:Person)-[:FRIEND]->(b:Person)
      WHERE a.id='Mark' RETURN b.id, b.label

-- Node-only query (no relationship)
MATCH (n:Person) WHERE n.id='Alex' RETURN n.id, n.label
```

### 5.3 DELETE

```cypher
-- Delete node
MATCH (n) WHERE n.id='Mark' DELETE n

-- Delete a specific edge
MATCH (a)-[r:FRIEND]->(b) WHERE a.id='Mark' DELETE r
```

> **Limitations**  
> • Only `id` & `label` properties on nodes (no dynamic properties)  
> • Single relationship per pattern  
> • No multi-hop, `OPTIONAL MATCH`, `SET`, `MERGE`, transactions, etc.

---

## 6. Running Unit Tests

```bash
make run_tests   # builds and executes all Unity tests
```

All tests reside in `test/` and cover core graph primitives as well as the Cypher parser.

---

## 7. Benchmark

```bash
./benchmark        # uses ./benchmarkdb and prints ops/sec stats
```

The benchmark inserts random nodes & edges and continuously measures insertion throughput using multiple threads.

Results on a **MacBook Pro M1 Pro** (10-core CPU, 16-GB RAM):

```text
Time to insert 100000 nodes: 0.585501 seconds
Time to insert 350000 edges: 2.513711 seconds
```

---

## 8. Internals (very brief)

Key prefixes inside RocksDB:

| Prefix | Record | Format |
|--------|--------|--------|
| `N`    | Node   | `N<node_id>` → *label* |
| `O`    | Edge   | `O<from>:<type>:<to>` → `""` |
| `I`    | Edge (incoming) | `I<to>:<type>:<from>` → `""` |

This dual-write pattern (`O` for outgoing, `I` for incoming) allows O(1) neighbor look-ups in either direction.

---

## 9. Cleaning Up

```bash
make clean   # removes binaries & object files
```

---

## 10. License

Licensed under the WTFPL license.  See `LICENSE` for details. 