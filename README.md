# GQLite – A Tiny Embedded Graph Database in C

GQLite is an educational, *light-weight* graph database implemented in plain C on top of [RocksDB](https://github.com/facebook/rocksdb).  
It supports a **minimal subset of Cypher** (Neo4j’s query language) and ships with a small CLI for experimentation, a public C API, an automated test-suite, and a Python-based web visualizer for querying and visualizing graphs.

---

## 1. Features

* RocksDB key-value storage – no server process required (fully embedded)
* Nodes & directed, typed edges
* Simple label & `id` properties per node
* Sub-set of Cypher:
  * `CREATE` – create nodes and/or a single edge in one statement
  * `MATCH`  – pattern matching on multiple hops with optional `WHERE`
  * `DELETE` – delete nodes or one edge that was previously matched
  * `RETURN` – project any of  `var.id`, `var.label`, or `rel.type`
* Thread-safe internal queues for neighbor pre-fetching
* Portable Makefile (tested on macOS)
* Unity-based unit tests
* Python bindings and a Flask-based web visualizer for interactive querying and graph visualization using D3.js

> ⚠️ This project is **not** intended for production but as a minimal, hackable playground for learning how graph stores work internally.

---

## 2. Building

### 2.1 Dependencies

| Dependency | Purpose | macOS (Homebrew) | Ubuntu / Debian |
|------------|---------|------------------|-----------------|
| C compiler | build C code (C11) | `clang` (or `gcc`) | `build-essential` |
| RocksDB    | storage engine | `brew install rocksdb` | `sudo apt install librocksdb-dev` |
| pthreads   | threading | included | included |
| Python 3   | web visualizer | `brew install python` | `sudo apt install python3` |
| Flask      | web server | `pip install flask` | `pip install flask` |

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
make             # builds: lib objects, `graph`, `benchmark`, `gqlite_cli`, and shared library `libgqlite.so` for Python bindings
```

Artifacts:

* `graph`        – tiny demo that hard-codes some graph logic (see `main.c`)
* `benchmark`    – inserts random data & measures throughput
* `gqlite_cli`   – interactive Cypher shell (see below)
* `libgqlite.so` – shared library for Python integration

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

GQLite supports multi-hop patterns for more complex graph traversals.

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

-- Multi-hop query
MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person) WHERE a.id='Mark' RETURN c.id, c.label
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
> • No `OPTIONAL MATCH`, `SET`, `MERGE`, transactions, etc.

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

## 8. Running the Web Visualizer

GQLite includes a simple web-based visualizer built with Flask and D3.js for running Cypher queries and visualizing the resulting graphs.

### 8.1 Setup

1. Ensure Python 3 and Flask are installed (see Dependencies above).
2. Build the project to generate `libgqlite.so` (via `make`).

### 8.2 Run the Server

```bash
python app.py
```

This starts a Flask server on `http://localhost:2999`. Open this URL in your browser.

### 8.3 Usage

* Enter a Cypher query in the input box at the top (e.g., `MATCH (a)-[:FRIEND]->(b) RETURN a,b`).
* Click "Run" to execute the query and visualize the graph.
* The graph is interactive: drag nodes, zoom, etc.

Note: The web app uses `python_api.py` to interface with the GQLite shared library and assumes a database at `./graphdb` (configurable in `app.py`).

---

## 9. Internals (very brief)

Key prefixes inside RocksDB:

| Prefix | Record | Format |
|--------|--------|--------|
| `N`    | Node   | `N<node_id>` → *label* |
| `O`    | Edge   | `O<from>:<type>:<to>` → `""` |
| `I`    | Edge (incoming) | `I<to>:<type>:<from>` → `""` |

This dual-write pattern (`O` for outgoing, `I` for incoming) allows O(1) neighbor look-ups in either direction.

---

## 10. Cleaning Up

```bash
make clean   # removes binaries & object files
```

---

## 11. License

Licensed under the WTFPL license.  See `LICENSE` for details. 