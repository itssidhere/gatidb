# gatidb

A simple key-value database built from scratch in Rust, using a B-Tree as the storage engine.

## What is a B-Tree?

A B-Tree is a self-balancing tree data structure used in databases and file systems. Unlike a binary tree (which has max 2 children per node), a B-Tree node can have **many keys and many children**.

### Why B-Trees for databases?

- They keep data **sorted**, so range queries are fast
- They stay **balanced** — every leaf is at the same depth
- They're optimized for systems that read/write large blocks of data (like disks)

## How our B-Tree works

### The structs

```
BTree
├── root: BTreeNode
└── degree: usize        (controls how many keys fit in a node)

BTreeNode
├── keys: Vec<i32>       (sorted list of keys)
├── values: Vec<String>  (value for each key)
├── children: Vec<BTreeNode>  (child nodes, empty if leaf)
└── is_leaf: bool
```

With `degree = 2`, each node can hold **max 3 keys** (formula: `2 * degree - 1`).

### Search

1. Start at the root node
2. Scan through the node's keys to find a match
3. If found, return the value
4. If not found and it's a leaf, return None
5. If not found and it's an internal node, recurse into the correct child

### Insert

1. Find the right leaf node where the key should go
2. If the node is full (has `2 * degree - 1` keys), **split** it:
   - The node breaks into two halves
   - The middle key gets pushed up to the parent
3. If the root itself is full, a new root is created (this is how the tree grows taller)

### Example: inserting keys 0 through 6 with degree=2

```
Insert 0,1,2: root = [0, 1, 2]  (root is now full, 3 keys)

Insert 3: root is full, split!
        [1]              <-- median pushed up as new root
       /    \
    [0]      [2, 3]      <-- split into two children

Insert 4,5: fills up the right child
        [1]
       /    \
    [0]      [2, 3, 4]   <-- right child is full now

Insert 5: right child full, split!
        [1, 3]
       /   |   \
    [0]   [2]   [4, 5]

...and so on
```

## Rust concepts used

| Concept | Where we used it |
|---------|-----------------|
| `struct` | `BTreeNode`, `BTree` — grouping data together |
| `Vec<T>` | Storing keys, values, children — growable arrays |
| `impl` | Adding methods to our structs |
| `&self` | Borrowing — reading data without taking ownership |
| `&mut self` | Mutable borrow — modifying data |
| `Option<T>` | `search` returns `Some(value)` or `None` |
| `match` | Pattern matching on `Option` and with guards |
| Closures | `\|k\| *k >= key` — anonymous functions for iterators |
| `std::mem::replace` | Swapping out the root when splitting |
| Modules | `mod btree` — organizing code into files |
| `split_off` | Splitting a Vec into two halves |
| Iterators | `.iter().position()` — scanning through elements |

## Running

```bash
cargo run
```

## What's next

- [ ] Delete operation
- [ ] Disk-backed storage (persist to file)
- [ ] Support generic key/value types
- [ ] Range queries
