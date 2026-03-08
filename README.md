# gatidb

A relational database management system written from scratch in Rust. Think MySQL/PostgreSQL, but built from zero — no dependencies on existing database engines. The goal is a fully functional, ACID-compliant SQL database with its own storage engine, query planner, and wire protocol.

## Current Status

gatidb is in early development. The in-memory B-Tree data structure is implemented with insert and search. This is the foundation that everything else will be built on top of.

### Benchmarks

```
insert 1000 keys        time:   ~206 µs  (~206 ns per insert)
search hit              time:   ~8.8 ns
search miss             time:   ~11.4 ns
delete 1000 keys        time:   ~226 µs  (~226 ns per delete)
```

Run benchmarks yourself:
```bash
cargo bench
```

## Architecture

```
┌─────────────────────────────────────────┐
│          TCP Server / Wire Protocol     │  <- client connections
├─────────────────────────────────────────┤
│            SQL Parser                   │  <- parsing SQL statements
├─────────────────────────────────────────┤
│           Query Engine                  │  <- query planning, execution
├─────────────────────────────────────────┤
│           Table / Schema                │  <- table definitions, rows, columns
├─────────────────────────────────────────┤
│         Transaction Manager             │  <- ACID, MVCC, WAL
├─────────────────────────────────────────┤
│      B-Tree Storage Engine [wip]        │  <- in-memory B-Tree
├─────────────────────────────────────────┤
│          Buffer Pool / Cache            │  <- page caching, LRU eviction
├─────────────────────────────────────────┤
│         Disk Manager / Pager            │  <- page-based file I/O
└─────────────────────────────────────────┘
```

## Roadmap

### Storage Engine
- [x] In-memory B-Tree data structure
- [x] B-Tree insert with node splitting
- [x] B-Tree search (point lookup)
- [x] Benchmarks with Criterion
- [x] B-Tree delete with rebalancing (merge/borrow)
- [ ] Support generic key/value types
- [ ] Range queries and iterators
- [ ] Bulk loading
- [ ] B+ Tree variant (data only in leaves, leaf-level linked list)

### Persistence
- [ ] Page-based storage (4KB pages)
- [ ] Disk-backed B-Tree (replace in-memory children with page offsets)
- [ ] Buffer pool with LRU eviction
- [ ] Write-Ahead Log (WAL) for crash recovery
- [ ] Serialization/deserialization of nodes to bytes

### Transactions
- [ ] ACID transactions
- [ ] MVCC (Multi-Version Concurrency Control)
- [ ] Snapshot isolation
- [ ] Deadlock detection

### Concurrency
- [ ] Reader-writer locks on B-Tree nodes
- [ ] Lock-free reads with MVCC
- [ ] Connection pooling

### Table & Schema
- [ ] Row format (fixed-length and variable-length columns)
- [ ] Data types (INT, VARCHAR, BOOL, FLOAT, TIMESTAMP)
- [ ] CREATE TABLE / DROP TABLE
- [ ] Schema catalog (system tables)
- [ ] ALTER TABLE

### SQL Parser
- [ ] Tokenizer / Lexer
- [ ] Parser (recursive descent or PEG)
- [ ] SELECT, INSERT, UPDATE, DELETE
- [ ] WHERE clauses with AND/OR
- [ ] JOINs (INNER, LEFT, RIGHT)
- [ ] ORDER BY, GROUP BY, LIMIT
- [ ] Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- [ ] Subqueries

### Query Engine
- [ ] Query planner
- [ ] Query optimizer (cost-based)
- [ ] Sequential scan
- [ ] Index scan
- [ ] Nested loop join
- [ ] Hash join
- [ ] Sort-merge join
- [ ] Prepared statements

### Indexing
- [ ] Primary key index (B-Tree)
- [ ] Secondary indexes
- [ ] Composite indexes
- [ ] Index-only scans

### Networking
- [ ] TCP server
- [ ] Wire protocol (MySQL or PostgreSQL compatible, or custom)
- [ ] Client library
- [ ] Connection handling and authentication
- [ ] TLS support

### Observability
- [ ] Logging
- [ ] Metrics and statistics
- [ ] EXPLAIN query plans

## How the B-Tree works

A B-Tree is a self-balancing tree where each node holds multiple keys. With `degree = 2`, each node holds max 3 keys. Keys are always sorted within a node, and children hold keys in the ranges between parent keys.

```
        [10,    20,    30]
       /     |      |     \
  keys<10  10<k<20  20<k<30  keys>30
```

### Insert with splitting

```
Insert 0,1,2: root = [0, 1, 2]  (full)

Insert 3: root splits
        [1]
       /    \
    [0]      [2, 3]

Insert 4,5: right child fills
        [1, 3]
       /   |   \
    [0]   [2]   [4, 5]
```

## Building

```bash
cargo build           # debug build
cargo build --release # optimized build
cargo test            # run tests
cargo bench           # run benchmarks
cargo run             # run the demo
```

## License

MIT
