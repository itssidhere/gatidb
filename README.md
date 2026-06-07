<p align="center">
  <img src="assets/gatidb.png" alt="GatiDB logo" width="560">
</p>

# GatiDB

GatiDB is a small C++ database project being built from first principles.

Current focus: an in-memory B-tree with insert, find, erase, and invariant tests. This is not a production database yet.

## Build

Requirements:

- CMake
- Ninja
- A C++20 compiler

```sh
cmake --preset debug
cmake --build --preset debug
```

## Test

```sh
./build/debug/gatidb_tests
```

Or with CTest:

```sh
ctest --test-dir build/debug --output-on-failure
```

## Layout

```text
include/gatidb/btree.hpp   B-tree public interface
src/btree.cpp              B-tree implementation
tests/btree_tests.cpp      Unit and invariant tests
assets/gatidb.png          Project logo
```

## Status

- B-tree minimum degree is currently `4`.
- Keys and values are `int`.
- Duplicate inserts update the existing value.
- Delete is covered by targeted tests and deterministic hammer tests.
