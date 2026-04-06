# WAL Implementation Plan for gatidb

## Overview

```
Current flow:
  DiskBtree -> BufferPool.write_page() -> pages HashMap (dirty) -> evict/flush -> DiskManager

New flow:
  DiskBtree -> BufferPool.write_page() -> WAL.log_page() -> pages HashMap (dirty)
                                               |
                                     on evict/flush:
                                     WAL.flush_to(page_lsn)  <- write-ahead rule
                                               |
                                     DiskManager.write_page()
```

### Files to touch

- **New**: `src/wal.rs` — WAL record format, writer, recovery
- **Modify**: `src/page.rs` — add 8-byte LSN to page header
- **Modify**: `src/catalog.rs` — shift catalog serialization past LSN header
- **Modify**: `src/buffer.rs` — integrate WAL, enforce write-ahead rule
- **Modify**: `src/lib.rs` — add `pub mod wal`

### Build order

| Step | What | Files | Depends on |
|---|---|---|---|
| 1 | LSN in page header | `page.rs`, `catalog.rs` | nothing |
| 2 | WAL record format | `wal.rs` (new) | nothing |
| 3 | WAL writer + reader | `wal.rs` | step 2 |
| 4 | Integrate into BufferPool | `buffer.rs`, all callers | steps 1, 3 |
| 5 | Recovery + checkpoint | `wal.rs` | steps 1-4 |

Steps 1 and 2-3 are independent and can be built in parallel.

---

## Step 1: Add LSN to page header (`page.rs` + `catalog.rs`)

Every page on disk needs to store the LSN of the last WAL record that modified it. This is how recovery knows whether a page is stale.

Reserve the first 8 bytes of every 4096-byte page for the LSN.

### Changes to `page.rs`

Add constant and helper functions:

```rust
pub const PAGE_HEADER_SIZE: usize = 8; // 8 bytes for u64 LSN

pub fn get_page_lsn(page: &[u8; PAGE_SIZE]) -> u64 {
    u64::from_le_bytes([
        page[0], page[1], page[2], page[3],
        page[4], page[5], page[6], page[7],
    ])
}

pub fn set_page_lsn(page: &mut [u8; PAGE_SIZE], lsn: u64) {
    page[0..8].copy_from_slice(&lsn.to_le_bytes());
}
```

In `serialize_node`, change starting offset:

```rust
pub fn serialize_node(...) -> [u8; PAGE_SIZE] {
    let mut buf = [0u8; PAGE_SIZE];
    let mut offset = PAGE_HEADER_SIZE;  // was: let mut offset = 0;
    // ... rest unchanged
}
```

In `deserialize_node`, same change:

```rust
pub fn deserialize_node(buf: &[u8; PAGE_SIZE]) -> (...) {
    let mut offset = PAGE_HEADER_SIZE;  // was: let mut offset = 0;
    // ... rest unchanged
}
```

### Changes to `catalog.rs`

In `serialize_catalog`:
```rust
let mut offset = PAGE_HEADER_SIZE;  // was: let mut offset = 0;
```

In `deserialize_catalog`:
```rust
let mut offset = PAGE_HEADER_SIZE;  // was: let mut offset = 0;
```

(Import `PAGE_HEADER_SIZE` from `crate::page`)

### Tests

All 43 existing tests should still pass — round-trip serialization is consistent.

New test in `page.rs`:

```rust
#[test]
fn test_page_lsn_round_trip() {
    let mut page = serialize_node(true, &[10, 20], &[b"a".to_vec(), b"b".to_vec()], &[]);
    assert_eq!(get_page_lsn(&page), 0); // default

    set_page_lsn(&mut page, 42);
    assert_eq!(get_page_lsn(&page), 42);

    // LSN doesn't interfere with node data
    let (is_leaf, keys, values, _) = deserialize_node(&page);
    assert!(is_leaf);
    assert_eq!(keys, vec![10, 20]);
}
```

---

## Step 2: WAL record format and serialization (`wal.rs`)

We log **full page images** (like PostgreSQL's FPI). Every time a page is modified, the entire 4096-byte page goes into the WAL. Wasteful but simple and correct.

### Record format on disk

```
+------------+------------+----------------------+
| lsn: u64   | page_id:u32| page_data: 4096 bytes|
| (8 bytes)  | (4 bytes)  |                      |
+------------+------------+----------------------+
  Total: 4108 bytes per record (fixed size)
```

Fixed-size records mean you can calculate the file offset for any LSN.

### Code

```rust
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use crate::disk::PAGE_SIZE;

pub const WAL_RECORD_SIZE: usize = 8 + 4 + PAGE_SIZE; // 4108 bytes

pub struct WalRecord {
    pub lsn: u64,
    pub page_id: u32,
    pub page_data: [u8; PAGE_SIZE],
}

impl WalRecord {
    pub fn serialize(&self) -> [u8; WAL_RECORD_SIZE] {
        let mut buf = [0u8; WAL_RECORD_SIZE];
        buf[0..8].copy_from_slice(&self.lsn.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_id.to_le_bytes());
        buf[12..12 + PAGE_SIZE].copy_from_slice(&self.page_data);
        buf
    }

    pub fn deserialize(buf: &[u8; WAL_RECORD_SIZE]) -> Self {
        let lsn = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let page_id = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let mut page_data = [0u8; PAGE_SIZE];
        page_data.copy_from_slice(&buf[12..12 + PAGE_SIZE]);
        WalRecord { lsn, page_id, page_data }
    }
}
```

### Tests

```rust
#[test]
fn test_wal_record_round_trip() {
    let mut data = [0u8; PAGE_SIZE];
    data[0] = 42;
    data[100] = 99;

    let record = WalRecord { lsn: 1, page_id: 5, page_data: data };
    let bytes = record.serialize();
    let recovered = WalRecord::deserialize(&bytes);

    assert_eq!(recovered.lsn, 1);
    assert_eq!(recovered.page_id, 5);
    assert_eq!(recovered.page_data[0], 42);
    assert_eq!(recovered.page_data[100], 99);
}
```

---

## Step 3: WAL writer + reader (`wal.rs`)

The writer manages the WAL file. Two key pointers:
- `current_lsn` — the next LSN to assign (in memory)
- `flushed_lsn` — the LSN up to which the file has been fsync'd (durable on disk)

### Code

```rust
pub struct Wal {
    file: File,
    current_lsn: u64,   // next LSN to assign
    flushed_lsn: u64,   // everything before this is on disk
}

impl Wal {
    pub fn new(filename: &str) -> Self {
        let file = OpenOptions::new()
            .read(true).write(true).create(true)
            .open(filename).unwrap();

        // current_lsn = number of existing records in file
        let file_len = file.metadata().unwrap().len();
        let current_lsn = file_len / WAL_RECORD_SIZE as u64;

        Wal {
            file,
            current_lsn,
            flushed_lsn: current_lsn, // existing data on disk is durable
        }
    }

    /// Log a full page image. Returns the assigned LSN.
    pub fn log_page(&mut self, page_id: u32, page_data: &[u8; PAGE_SIZE]) -> u64 {
        let lsn = self.current_lsn;
        self.current_lsn += 1;

        let record = WalRecord {
            lsn,
            page_id,
            page_data: *page_data,
        };

        let offset = lsn * WAL_RECORD_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        self.file.write_all(&record.serialize()).unwrap();

        lsn
    }

    /// Fsync the WAL file — makes everything up to current_lsn durable.
    pub fn flush(&mut self) {
        self.file.sync_data().unwrap();
        self.flushed_lsn = self.current_lsn;
    }

    /// Ensure WAL is durable up to at least target_lsn.
    pub fn flush_to(&mut self, target_lsn: u64) {
        if self.flushed_lsn <= target_lsn {
            self.flush();
        }
    }

    pub fn flushed_lsn(&self) -> u64 {
        self.flushed_lsn
    }

    pub fn current_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Read all records starting from a given LSN (for recovery).
    pub fn read_from(&mut self, start_lsn: u64) -> Vec<WalRecord> {
        let mut records = Vec::new();
        let mut lsn = start_lsn;

        loop {
            let offset = lsn * WAL_RECORD_SIZE as u64;
            self.file.seek(SeekFrom::Start(offset)).unwrap();

            let mut buf = [0u8; WAL_RECORD_SIZE];
            if self.file.read_exact(&mut buf).is_err() {
                break; // end of file
            }

            records.push(WalRecord::deserialize(&buf));
            lsn += 1;
        }

        records
    }
}
```

LSN is a **record number** (0, 1, 2, ...) not a byte offset. Byte offset = `lsn * WAL_RECORD_SIZE`.

### Tests

```rust
#[test]
fn test_wal_writer_log_and_read() {
    let filename = "test_wal_writer.wal";
    let mut wal = Wal::new(filename);

    let mut page1 = [0u8; PAGE_SIZE];
    page1[0] = 10;
    let lsn1 = wal.log_page(0, &page1);

    let mut page2 = [0u8; PAGE_SIZE];
    page2[0] = 20;
    let lsn2 = wal.log_page(1, &page2);

    assert_eq!(lsn1, 0);
    assert_eq!(lsn2, 1);

    let records = wal.read_from(0);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].page_id, 0);
    assert_eq!(records[0].page_data[0], 10);
    assert_eq!(records[1].page_id, 1);
    assert_eq!(records[1].page_data[0], 20);

    std::fs::remove_file(filename).unwrap();
}

#[test]
fn test_wal_writer_read_from_middle() {
    let filename = "test_wal_read_mid.wal";
    let mut wal = Wal::new(filename);

    for i in 0u8..5 {
        let mut page = [0u8; PAGE_SIZE];
        page[0] = i;
        wal.log_page(i as u32, &page);
    }

    // Read from LSN 3 onward
    let records = wal.read_from(3);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].lsn, 3);
    assert_eq!(records[0].page_data[0], 3);

    std::fs::remove_file(filename).unwrap();
}

#[test]
fn test_wal_persists_across_reopen() {
    let filename = "test_wal_persist.wal";

    {
        let mut wal = Wal::new(filename);
        let mut page = [0u8; PAGE_SIZE];
        page[0] = 77;
        wal.log_page(5, &page);
        wal.flush();
    }

    {
        let mut wal = Wal::new(filename);
        assert_eq!(wal.current_lsn(), 1); // ready for next record
        let records = wal.read_from(0);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].page_data[0], 77);
    }

    std::fs::remove_file(filename).unwrap();
}
```

---

## Step 4: Integrate WAL into BufferPool (`buffer.rs`)

This is where the write-ahead rule gets enforced.

### Changes to `BufferPool` struct

```rust
use crate::wal::Wal;
use crate::page::{set_page_lsn, get_page_lsn};

pub struct BufferPool {
    disk: DiskManager,
    wal: Wal,                          // NEW
    pages: HashMap<u32, [u8; PAGE_SIZE]>,
    dirty: HashMap<u32, bool>,
    page_lsn: HashMap<u32, u64>,       // NEW: tracks LSN per page
    capacity: usize,
    lru_order: Vec<u32>,
}
```

### Updated `new`

```rust
pub fn new(disk: DiskManager, wal: Wal, capacity: usize) -> Self {
    BufferPool {
        disk,
        wal,
        pages: HashMap::new(),
        dirty: HashMap::new(),
        page_lsn: HashMap::new(),
        capacity,
        lru_order: Vec::new(),
    }
}
```

### Updated `write_page` — log to WAL before caching

```rust
pub fn write_page(&mut self, page_id: u32, mut data: [u8; PAGE_SIZE]) {
    if !self.pages.contains_key(&page_id) {
        while self.pages.len() >= self.capacity {
            self.evict();
        }
    }

    // 1. Log to WAL — get assigned LSN
    let lsn = self.wal.log_page(page_id, &data);

    // 2. Stamp the LSN into the page header
    set_page_lsn(&mut data, lsn);

    // 3. Cache the page
    self.pages.insert(page_id, data);
    self.dirty.insert(page_id, true);
    self.page_lsn.insert(page_id, lsn);
    self.touch(page_id);
}
```

### Updated `evict` — enforce write-ahead rule

```rust
fn evict(&mut self) {
    if let Some(victim_id) = self.lru_order.first().copied() {
        if self.dirty.get(&victim_id) == Some(&true) {
            // WRITE-AHEAD RULE: WAL must be durable before data page
            let page_lsn = self.page_lsn.get(&victim_id).copied().unwrap_or(0);
            self.wal.flush_to(page_lsn);

            if let Some(data) = self.pages.get(&victim_id) {
                self.disk.write_page(victim_id, data);
            }
            self.dirty.remove(&victim_id);
        }

        self.pages.remove(&victim_id);
        self.page_lsn.remove(&victim_id);
        self.lru_order.remove(0);
    }
}
```

### Updated `flush` — same write-ahead rule

```rust
pub fn flush(&mut self) {
    // Flush WAL first (all of it — covers all dirty pages)
    self.wal.flush();

    let dirty_ids: Vec<u32> = self.dirty.keys().cloned().collect();
    for page_id in dirty_ids {
        if let Some(data) = self.pages.get(&page_id) {
            self.disk.write_page(page_id, data);
        }
    }
    self.dirty.clear();
}
```

### Update all callers

Every `BufferPool::new(dm, 64)` becomes `BufferPool::new(dm, Wal::new("xxx.wal"), 64)`:

- `src/disk_btree.rs` tests
- `src/table.rs` tests
- `src/catalog.rs` tests + constructor
- `src/main.rs`
- `src/buffer.rs` tests

---

## Step 5: Recovery and Checkpoint (`wal.rs`)

### Recovery function

Called on startup, before the buffer pool is used:

```rust
use crate::page::get_page_lsn;

/// Replay WAL records from checkpoint_lsn to restore data pages.
pub fn recover(wal: &mut Wal, disk: &mut DiskManager, checkpoint_lsn: u64) {
    let records = wal.read_from(checkpoint_lsn);

    for record in &records {
        let page = disk.read_page(record.page_id);
        let page_lsn = get_page_lsn(&page);

        if page_lsn < record.lsn {
            // Page is stale — apply the WAL record (full page overwrite)
            disk.write_page(record.page_id, &record.page_data);
        }
        // else: page already has this change or newer, skip
    }
}
```

Because we log full page images, "applying" a record = overwriting the entire page. No partial diffs.

### Checkpoint function

```rust
/// Checkpoint: flush everything, return the checkpoint LSN.
pub fn checkpoint(pool: &mut BufferPool) -> u64 {
    pool.flush(); // writes WAL first (write-ahead rule), then data pages
    pool.wal.current_lsn()
}
```

### Checkpoint LSN persistence

Store in a small file (`<dbname>.ckpt`) — just 8 bytes:

```rust
pub fn write_checkpoint_lsn(filename: &str, lsn: u64) {
    let mut file = File::create(filename).unwrap();
    file.write_all(&lsn.to_le_bytes()).unwrap();
    file.sync_data().unwrap();
}

pub fn read_checkpoint_lsn(filename: &str) -> u64 {
    let mut file = match File::open(filename) {
        Ok(f) => f,
        Err(_) => return 0, // no checkpoint — replay from beginning
    };
    let mut buf = [0u8; 8];
    if file.read_exact(&mut buf).is_err() {
        return 0;
    }
    u64::from_le_bytes(buf)
}
```

### Crash simulation test

The most important test — proves WAL actually works:

```rust
#[test]
fn test_recovery_after_crash() {
    let db_file = "test_wal_crash.db";
    let wal_file = "test_wal_crash.wal";

    // --- Session 1: write data, flush WAL but NOT data pages ---
    {
        let dm = DiskManager::new(db_file);
        let wal = Wal::new(wal_file);
        let mut pool = BufferPool::new(dm, wal, 64);

        let mut page1 = [0u8; PAGE_SIZE];
        page1[PAGE_HEADER_SIZE] = 1; // is_leaf = true
        pool.write_page(1, page1);

        let mut page2 = [0u8; PAGE_SIZE];
        page2[PAGE_HEADER_SIZE] = 1;
        pool.write_page(2, page2);

        // Flush WAL only — data pages stay dirty in memory
        pool.wal.flush();

        // Simulate crash: drop pool WITHOUT calling pool.flush()
    }

    // --- Session 2: recover from WAL ---
    {
        let mut dm = DiskManager::new(db_file);
        let mut wal = Wal::new(wal_file);

        recover(&mut wal, &mut dm, 0);

        let page1 = dm.read_page(1);
        let page1_lsn = get_page_lsn(&page1);
        assert!(page1_lsn > 0, "page 1 should be recovered from WAL");

        let page2 = dm.read_page(2);
        let page2_lsn = get_page_lsn(&page2);
        assert!(page2_lsn > 0, "page 2 should be recovered from WAL");
    }

    std::fs::remove_file(db_file).unwrap();
    std::fs::remove_file(wal_file).unwrap();
}
```

### Additional tests to write

- **Recovery is idempotent** — run recover twice, same result
- **Recovery skips up-to-date pages** — flush data to disk, recover, verify pages aren't overwritten with stale WAL data
- **Checkpoint reduces recovery work** — write 10 records, checkpoint, write 5 more, crash, recover from checkpoint LSN, verify only last 5 replayed
- **End-to-end with B-tree** — insert rows via DiskBtree, flush WAL only, recover, verify rows searchable

---

## Background: Why This Design

### The write-ahead rule

The single invariant that makes crash recovery possible:

```
Before writing data page P to disk:
    assert!(wal_flushed_lsn >= page_lsn[P])
```

### What PostgreSQL does (our model)

- Single WAL stream in `pg_wal/` directory
- Full page images (FPI) for first modification after checkpoint
- `wal_sync_method = fdatasync` by default
- Recovery: redo only (no undo phase needed — MVCC handles visibility)

### What MySQL (InnoDB) does (more complex)

- Separate redo log (circular) + undo log (in tablespace)
- Doublewrite buffer for torn page protection
- Recovery: redo forward, then undo uncommitted transactions
- `innodb_flush_log_at_trx_commit = 1` for full durability

### We follow PostgreSQL's approach because:

1. Single log file — simpler than redo + undo split
2. Full page images handle torn pages automatically
3. No separate undo log needed
4. Recovery is redo-only

### Key concepts from the Transaction Processing book (Jim Gray)

- **Steal policy**: Can we evict dirty pages from uncommitted txns? YES (we do this)
- **Force policy**: Must all dirty pages be on disk at commit? Our flush() does this
- **ARIES**: The recovery algorithm — redo history, then undo uncommitted
- **CLRs**: Compensation Log Records — logging undo operations during recovery
