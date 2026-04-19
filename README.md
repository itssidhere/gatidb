# gatidb

A relational database written from scratch in Rust — zero external dependencies, built layer by layer for learning. The goal: an ACID-compliant SQL database with its own storage engine, query planner, and recovery system.

## Status

Working today: a disk-backed B-Tree storage engine with a buffer pool, write-ahead logging, crash recovery, checkpoints, a typed schema layer, and a persistent table catalog. You can create tables, insert rows, get rows by primary key, persist across restarts, and recover from a crash by replaying the WAL.

## Architecture

```
                  ┌──────────────────────────────┐
                  │           main.rs            │   demo / entry point
                  └──────────────┬───────────────┘
                                 │
                  ┌──────────────▼───────────────┐
                  │           Catalog            │   table metadata persisted on page 0
                  └──────────────┬───────────────┘
                                 │
                  ┌──────────────▼───────────────┐
                  │            Table             │   schema + encode/decode rows
                  └──────────────┬───────────────┘
                                 │
                  ┌──────────────▼───────────────┐
                  │          DiskBtree           │   B-Tree over page IDs
                  └──────────────┬───────────────┘
                                 │
                  ┌──────────────▼───────────────┐
                  │          BufferPool          │   LRU cache + dirty tracking
                  │            ┌─────┐           │
                  │   ┌────────┤ WAL ├────────┐  │   write-ahead rule:
                  │   │        └─────┘        │  │   WAL flushed before
                  │   ▼                       ▼  │   data page flushed
                  │ ┌──────┐             ┌──────┐│
                  │ │ WAL  │             │ Disk ││
                  │ │ file │             │ Mgr  ││
                  │ └──┬───┘             └──┬───┘│
                  └────┼────────────────────┼────┘
                       ▼                    ▼
                 gatidb.wal           gatidb.db
```

### Layout: a 4 KiB page

```
 byte 0                 8                                          4096
 ┌──────────────────────┬──────────────────────────────────────────┐
 │   page LSN (u64 LE)  │   node payload                           │
 │                      │   (is_leaf, num_keys, keys, values,      │
 │   PAGE_HEADER_SIZE   │    children — see src/page.rs)           │
 └──────────────────────┴──────────────────────────────────────────┘
```

The first 8 bytes of every page hold the LSN of the WAL record that last modified it. Recovery uses this to decide which records to replay.

### Layout: a WAL record (4108 bytes, fixed)

```
 byte 0           8           12                                4108
 ┌────────────────┬───────────┬─────────────────────────────────┐
 │   lsn (u64)    │ page_id   │   page_data (full 4 KiB FPI)    │
 │                │  (u32)    │                                 │
 └────────────────┴───────────┴─────────────────────────────────┘
```

Full-page-image WAL: every page modification logs the entire page. Simple, no diff logic, idempotent on replay.

### Write path

```
  caller                BufferPool                 WAL                Disk
    │                       │                       │                   │
    │  write_page(id, p) ──▶│                       │                   │
    │                       │  log_page(id, p) ────▶│                   │
    │                       │◀──── lsn ─────────────│                   │
    │                       │ stamp lsn into page   │                   │
    │                       │ mark dirty, cache it  │                   │
    │                       │                       │                   │
    │  ... time passes, eviction triggers ...       │                   │
    │                       │                       │                   │
    │                       │  flush_to(page_lsn) ─▶│                   │
    │                       │                       │   sync to disk    │
    │                       │  write_page ─────────────────────────────▶│
```

### Recovery flow

```
  1.  read checkpoint LSN from .ckpt  (0 if missing)
  2.  read all WAL records from that LSN onward
  3.  for each record:
         disk_page = disk.read_page(record.page_id)
         if get_page_lsn(disk_page) < record.lsn:
              disk.write_page(record.page_id, record.page_data)
```

## Demo

```rust
let dm   = DiskManager::new("gatidb.db");
let wal  = Wal::new("gatidb.wal");
let pool = BufferPool::new(dm, wal, 64);
let mut catalog = Catalog::new(pool);

catalog.create_table("jobs", Schema {
    columns: vec![
        Column { name: "id".into(),    data_type: DataType::Int },
        Column { name: "title".into(), data_type: DataType::Varchar(64) },
    ],
    primary_key: 0,
}, 3);

let mut table = catalog.get_table("jobs").unwrap();
table.insert_row(&[Value::Int(1), Value::Varchar("fix bug".into())]);
catalog.update_next_page_id(table.next_page_id());
catalog.flush();
```

Run it:

```bash
cargo run
```

## Modules

| File              | Responsibility                                                  |
|-------------------|-----------------------------------------------------------------|
| `src/disk.rs`     | Page-aligned file I/O                                           |
| `src/page.rs`     | Node serialization, LSN header helpers                          |
| `src/wal.rs`      | WAL records, log writer, checkpoint, recovery                   |
| `src/buffer.rs`   | LRU buffer pool, write-ahead rule enforcement                   |
| `src/disk_btree.rs` | B-Tree operating over page IDs                                |
| `src/table.rs`    | Schema, row encode/decode, table API                            |
| `src/catalog.rs`  | Table metadata persisted to page 0                              |
| `src/main.rs`     | Demo                                                            |

## Roadmap

### Storage engine
- [x] Disk manager (4 KiB pages)
- [x] LRU buffer pool with dirty tracking
- [x] B-Tree insert / search / delete with split, borrow, merge
- [x] Node serialization to fixed-size pages
- [ ] Range scans / iterators (next)
- [ ] B+ Tree variant (data only in leaves, leaf links)

### Durability
- [x] Write-ahead log (full page images)
- [x] Page LSN header + write-ahead rule on eviction
- [x] Checkpoint (flush dirty pages, persist checkpoint LSN)
- [x] Crash recovery (redo from checkpoint LSN)
- [ ] WAL truncation after checkpoint
- [ ] Group commit / batched fsync

### Schema and catalog
- [x] Schema with `Int`, `Varchar(n)`, `Bool`
- [x] Row encode/decode
- [x] Persistent table catalog
- [ ] `Float`, `Timestamp`, `NULL` semantics
- [ ] `ALTER TABLE`, `DROP TABLE`

### Transactions
- [ ] `BEGIN` / `COMMIT` / `ABORT`
- [ ] Transaction IDs in WAL records
- [ ] Redo only committed (no-steal) — simpler than full ARIES
- [ ] Full ARIES (analysis / redo / undo with CLRs)
- [ ] MVCC + snapshot isolation

### Query layer
- [ ] REPL
- [ ] Tokenizer + parser
- [ ] `SELECT` / `INSERT` / `UPDATE` / `DELETE`
- [ ] `WHERE`, `ORDER BY`, `LIMIT`
- [ ] Joins, aggregates, subqueries
- [ ] Cost-based planner + `EXPLAIN`

### Indexing
- [x] Primary key (B-Tree on PK)
- [ ] Secondary indexes
- [ ] Composite indexes
- [ ] Index-only scans

### Concurrency
- [ ] Page latches (RwLock per page)
- [ ] Lock manager
- [ ] Deadlock detection

### Networking
- [ ] TCP server
- [ ] Wire protocol (PostgreSQL or custom)
- [ ] TLS

## Building

```bash
cargo build              # debug
cargo build --release    # optimized
cargo test               # full test suite (45 tests)
cargo run                # demo
```

## License

MIT
