use crate::buffer::BufferPool;
use crate::page::{deserialize_node, serialize_node};

pub struct DiskBtree {
    pool: BufferPool,
    root_page_id: u32,
    next_page_id: u32,
    degree: usize,
}

impl DiskBtree {
    pub fn new(pool: BufferPool, degree: usize) -> Self {
        let root_page_id = 0;
        let next_page_id = 1;

        // write an empty root (leaf, no keys)
        let data = serialize_node(true, &[], &[], &[]);
        let mut tree = DiskBtree {
            pool,
            root_page_id,
            next_page_id,
            degree,
        };
        tree.pool.write_page(root_page_id, data);
        tree
    }

    pub fn search(&mut self, key: i32) -> Option<String> {
        self.search_node(self.root_page_id, key)
    }

    pub fn insert(&mut self, key: i32, value: String) {
        let root_id = self.root_page_id;
        let page = self.pool.get_page(root_id);
        let (is_leaf, keys, values, children) = deserialize_node(page);
        let max_keys = 2 * self.degree - 1;

        if keys.len() == max_keys {
            // root is full - create new root and split

            let new_root_id = self.allocate_page();
            let data = serialize_node(false, &[], &[], &[root_id]);
            self.pool.write_page(new_root_id, data);
            self.root_page_id = new_root_id;
            self.split_child(new_root_id, 0);
            self.insert_non_full(new_root_id, key, value);
        } else {
            self.insert_non_full(root_id, key, value);
        }
    }

    fn search_node(&mut self, page_id: u32, key: i32) -> Option<String> {
        let page = self.pool.get_page(page_id);
        let (is_leaf, keys, values, children) = deserialize_node(page);

        match keys.binary_search(&key) {
            Ok(i) => Some(values[i].clone()),
            Err(i) if !is_leaf => self.search_node(children[i], key),
            _ => None,
        }
    }

    fn insert_non_full(&mut self, page_id: u32, key: i32, value: String) {
        let page = self.pool.get_page(page_id);
        let (is_leaf, mut keys, mut values, mut children) = deserialize_node(page);

        let pos = keys.binary_search(&key).unwrap_or_else(|i| i);

        if is_leaf {
            keys.insert(pos, key);
            values.insert(pos, value);
            let data = serialize_node(true, &keys, &values, &children);
            self.pool.write_page(page_id, data);
        } else {
            // check if child is full
            let child_id = children[pos];
            let child_page = self.pool.get_page(child_id);
            let (_, child_keys, _, _) = deserialize_node(child_page);
            let max_keys = 2 * self.degree - 1;

            let target = if child_keys.len() == max_keys {
                self.split_child(page_id, pos);
                //re-read parent after split
                let page = self.pool.get_page(page_id);
                let (_, keys, _, children) = deserialize_node(page);
                if key > keys[pos] {
                    children[pos + 1]
                } else {
                    children[pos]
                }
            } else {
                child_id
            };

            self.insert_non_full(target, key, value);
        }
    }

    fn split_child(&mut self, parent_id: u32, idx: usize) {
        let page = self.pool.get_page(parent_id);
        let (p_leaf, mut p_keys, mut p_values, mut p_children) = deserialize_node(page);

        let child_id = p_children[idx];
        let child_page = self.pool.get_page(child_id);
        let (c_leaf, c_keys, c_values, c_children) = deserialize_node(child_page);

        let mid = self.degree - 1;

        let left_keys = c_keys[..mid].to_vec();
        let left_values = c_values[..mid].to_vec();
        let right_keys = c_keys[mid + 1..].to_vec();
        let right_values = c_values[mid + 1..].to_vec();

        let (left_children, right_children) = if !c_leaf {
            (
                c_children[..mid + 1].to_vec(),
                c_children[mid + 1..].to_vec(),
            )
        } else {
            (vec![], vec![])
        };

        let median_key = c_keys[mid];
        let median_value = c_values[mid].clone();

        let data = serialize_node(c_leaf, &left_keys, &left_values, &left_children);

        self.pool.write_page(child_id, data);

        let right_id = self.allocate_page();
        let data = serialize_node(c_leaf, &right_keys, &right_values, &right_children);

        self.pool.write_page(right_id, data);

        p_keys.insert(idx, median_key);
        p_values.insert(idx, median_value);
        p_children.insert(idx + 1, right_id);
        let data = serialize_node(p_leaf, &p_keys, &p_values, &p_children);

        self.pool.write_page(parent_id, data);
    }

    pub fn flush(&mut self) {
        self.pool.flush();
    }

    fn allocate_page(&mut self) -> u32 {
        let id = self.next_page_id;
        self.next_page_id += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::DiskManager;

    fn make_tree(filename: &str, degree: usize) -> DiskBtree {
        let dm = DiskManager::new(filename);
        let pool = BufferPool::new(dm);
        DiskBtree::new(pool, degree)
    }

    #[test]
    fn test_insert_and_search() {
        let filename = "test_disk_btree.db";
        let mut tree = make_tree(filename, 2);

        tree.insert(10, "ten".to_string());
        tree.insert(20, "twenty".to_string());
        tree.insert(5, "five".to_string());

        assert_eq!(tree.search(10), Some("ten".to_string()));
        assert_eq!(tree.search(20), Some("twenty".to_string()));
        assert_eq!(tree.search(5), Some("five".to_string()));
        assert_eq!(tree.search(99), None);

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_insert_triggers_split() {
        let filename = "test_disk_btree_split.db";
        let mut tree = make_tree(filename, 2);

        // degree 2 = max 3 keys per node, will trigger splits
        for i in 0..10 {
            tree.insert(i, format!("val_{}", i));
        }

        for i in 0..10 {
            assert_eq!(tree.search(i), Some(format!("val_{}", i)));
        }
        assert_eq!(tree.search(99), None);

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_persistence() {
        let filename = "test_disk_btree_persist.db";

        // insert and flush to disk
        {
            let mut tree = make_tree(filename, 2);
            tree.insert(1, "one".to_string());
            tree.insert(2, "two".to_string());
            tree.insert(3, "three".to_string());
            tree.flush();
        }

        // read back from disk with a fresh buffer pool
        {
            let dm = DiskManager::new(filename);
            let mut pool = BufferPool::new(dm);

            // root is page 0 — read it and verify data survived
            let page = pool.get_page(0);
            let (_, keys, values, _) = deserialize_node(page);
            // with degree 2, inserting 1,2,3 fills root and splits
            // just verify we can read valid data from disk
            assert!(!keys.is_empty());
        }

        std::fs::remove_file(filename).unwrap();
    }
}
