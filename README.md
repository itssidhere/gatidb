# gatidb

A small relational database storage engine written in modern C++ with no external runtime dependencies.

## Status

Working today:

- 4 KiB page storage
- Disk-backed B-Tree with insert, lookup, delete, and range scan
- LRU buffer pool with dirty tracking
- Full-page-image write-ahead log
- Page LSNs, checkpoints, and crash recovery
- Typed schemas and row encoding
- Persistent table catalog
- Minimal SQL tokenizer and parser for `CREATE TABLE`, `INSERT`, and `SELECT`

## Build

```bash
make test
make demo
```

CMake is also supported:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

Run the demo:

```bash
./build/gatidb
```

## Example

```cpp
#include "gatidb/gatidb.h"

using namespace gatidb;

DiskManager disk("gatidb.db");
Wal wal("gatidb.wal");
BufferPool pool(std::move(disk), std::move(wal), 64);
Catalog catalog(std::move(pool));

catalog.create_table(
    "jobs",
    Schema{
        {
            Column{"id", DataType::Int()},
            Column{"title", DataType::Varchar(64)},
            Column{"done", DataType::Bool()},
        },
        0,
    },
    3
);

auto table = catalog.get_table("jobs").value();
table.insert_row({Value::Int(1), Value::Varchar("fix bug"), Value::Bool(false)});
catalog.update_table_storage(table);
catalog.flush();
```

## Layout

```text
include/gatidb/gatidb.h   Public C++ API
src/gatidb.cpp            Storage engine, catalog, SQL tokenizer/parser
src/main.cpp              Demo executable
tests/test_gatidb.cpp     Self-contained C++ test binary
CMakeLists.txt            Build configuration
Makefile                  Local compiler build path
```

## Storage Format

Every database page is 4096 bytes. The first 8 bytes store the page LSN in little-endian order. B-Tree node payload starts after that header.

WAL records are fixed-size full page images:

```text
8 bytes  lsn
4 bytes  page_id
4096     page data
```

Recovery reads WAL records from the checkpoint LSN and replays records whose LSN is at least as new as the page currently on disk.
