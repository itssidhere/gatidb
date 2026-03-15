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

    pub fn search(&mut self, key: i32) -> Option<Vec<u8>> {
        self.search_node(self.root_page_id, key)
    }

    pub fn insert(&mut self, key: i32, value: Vec<u8>) {
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

    fn search_node(&mut self, page_id: u32, key: i32) -> Option<Vec<u8>> {
        let page = self.pool.get_page(page_id);
        let (is_leaf, keys, values, children) = deserialize_node(page);

        match keys.binary_search(&key) {
            Ok(i) => Some(values[i].clone()),
            Err(i) if !is_leaf => self.search_node(children[i], key),
            _ => None,
        }
    }

    fn insert_non_full(&mut self, page_id: u32, key: i32, value: Vec<u8>) {
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

    pub fn delete(&mut self, key: i32) {
        let root_id = self.root_page_id;
        self.delete_key(root_id, key);

        // if root has no keys but has children, shrink the tree
        let page = self.pool.get_page(self.root_page_id);
        let (is_leaf, keys, _, children) = deserialize_node(page);
        if keys.is_empty() && !is_leaf {
            self.root_page_id = children[0];
        }
    }

    fn delete_key(&mut self, page_id: u32, key: i32) {
        let page = self.pool.get_page(page_id);
        let (is_leaf, keys, values, children) = deserialize_node(page);

        let idx = keys.binary_search(&key).unwrap_or_else(|i| i);

        if idx < keys.len() && keys[idx] == key {
            if is_leaf {
                // case 1: key is in a leaf — just remove it
                let mut keys = keys;
                let mut values = values;
                keys.remove(idx);
                values.remove(idx);
                let data = serialize_node(true, &keys, &values, &children);
                self.pool.write_page(page_id, data);
            } else {
                // case 2: key is in an internal node
                let left_id = children[idx];
                let left_page = self.pool.get_page(left_id);
                let (_, left_keys, _, _) = deserialize_node(left_page);

                let right_id = children[idx + 1];
                let right_page = self.pool.get_page(right_id);
                let (_, right_keys, _, _) = deserialize_node(right_page);

                if left_keys.len() >= self.degree {
                    // case 2a: left child has enough keys — use predecessor
                    let (pred_key, pred_value) = self.get_predecessor(left_id);
                    let mut keys = keys;
                    let mut values = values;
                    keys[idx] = pred_key;
                    values[idx] = pred_value;
                    let data = serialize_node(is_leaf, &keys, &values, &children);
                    self.pool.write_page(page_id, data);
                    // re-read the key we just wrote (it changed)
                    let page = self.pool.get_page(page_id);
                    let (_, keys, _, children) = deserialize_node(page);
                    self.delete_key(children[idx], keys[idx]);
                } else if right_keys.len() >= self.degree {
                    // case 2b: right child has enough keys — use successor
                    let (succ_key, succ_value) = self.get_successor(right_id);
                    let mut keys = keys;
                    let mut values = values;
                    keys[idx] = succ_key;
                    values[idx] = succ_value;
                    let data = serialize_node(is_leaf, &keys, &values, &children);
                    self.pool.write_page(page_id, data);
                    let page = self.pool.get_page(page_id);
                    let (_, keys, _, children) = deserialize_node(page);
                    self.delete_key(children[idx + 1], keys[idx]);
                } else {
                    // case 2c: both children have minimum keys — merge
                    self.merge(page_id, idx);
                    self.delete_key(left_id, key);
                }
            }
        } else {
            // case 3: key is not in this node
            if is_leaf {
                return; // key doesn't exist
            }

            // ensure the child we descend into has enough keys
            let child_id = children[idx];
            let child_page = self.pool.get_page(child_id);
            let (_, child_keys, _, _) = deserialize_node(child_page);

            if child_keys.len() < self.degree {
                self.fill(page_id, idx);
            }

            // re-read parent after fill (it may have changed)
            let page = self.pool.get_page(page_id);
            let (_, _, _, children) = deserialize_node(page);

            if idx >= children.len() {
                self.delete_key(children[idx - 1], key);
            } else {
                self.delete_key(children[idx], key);
            }
        }
    }

    fn get_predecessor(&mut self, page_id: u32) -> (i32, Vec<u8>) {
        let page = self.pool.get_page(page_id);
        let (is_leaf, keys, values, children) = deserialize_node(page);

        if is_leaf {
            let last = keys.len() - 1;
            (keys[last], values[last].clone())
        } else {
            let last_child = *children.last().unwrap();
            self.get_predecessor(last_child)
        }
    }

    fn get_successor(&mut self, page_id: u32) -> (i32, Vec<u8>) {
        let page = self.pool.get_page(page_id);
        let (is_leaf, keys, values, children) = deserialize_node(page);

        if is_leaf {
            (keys[0], values[0].clone())
        } else {
            self.get_successor(children[0])
        }
    }

    fn merge(&mut self, parent_id: u32, idx: usize) {
        let page = self.pool.get_page(parent_id);
        let (p_leaf, mut p_keys, mut p_values, mut p_children) = deserialize_node(page);

        let left_id = p_children[idx];
        let right_id = p_children[idx + 1];

        let left_page = self.pool.get_page(left_id);
        let (l_leaf, mut l_keys, mut l_values, mut l_children) = deserialize_node(left_page);

        let right_page = self.pool.get_page(right_id);
        let (_, mut r_keys, mut r_values, mut r_children) = deserialize_node(right_page);

        // pull parent key down into left child
        l_keys.push(p_keys.remove(idx));
        l_values.push(p_values.remove(idx));
        p_children.remove(idx + 1);

        // append right child's data to left child
        l_keys.append(&mut r_keys);
        l_values.append(&mut r_values);
        l_children.append(&mut r_children);

        // write updated left child
        let data = serialize_node(l_leaf, &l_keys, &l_values, &l_children);
        self.pool.write_page(left_id, data);

        // write updated parent
        let data = serialize_node(p_leaf, &p_keys, &p_values, &p_children);
        self.pool.write_page(parent_id, data);
    }

    fn fill(&mut self, parent_id: u32, idx: usize) {
        let page = self.pool.get_page(parent_id);
        let (_, _, _, children) = deserialize_node(page);

        // check if left sibling has enough keys
        if idx > 0 {
            let left_sib = self.pool.get_page(children[idx - 1]);
            let (_, left_keys, _, _) = deserialize_node(left_sib);
            if left_keys.len() >= self.degree {
                self.borrow_from_prev(parent_id, idx);
                return;
            }
        }

        // check if right sibling has enough keys
        if idx < children.len() - 1 {
            let right_sib = self.pool.get_page(children[idx + 1]);
            let (_, right_keys, _, _) = deserialize_node(right_sib);
            if right_keys.len() >= self.degree {
                self.borrow_from_next(parent_id, idx);
                return;
            }
        }

        // merge with a sibling
        if idx < children.len() - 1 {
            self.merge(parent_id, idx);
        } else {
            self.merge(parent_id, idx - 1);
        }
    }

    fn borrow_from_prev(&mut self, parent_id: u32, idx: usize) {
        let page = self.pool.get_page(parent_id);
        let (p_leaf, mut p_keys, mut p_values, p_children) = deserialize_node(page);

        let child_id = p_children[idx];
        let sib_id = p_children[idx - 1];

        let child_page = self.pool.get_page(child_id);
        let (c_leaf, mut c_keys, mut c_values, mut c_children) = deserialize_node(child_page);

        let sib_page = self.pool.get_page(sib_id);
        let (s_leaf, mut s_keys, mut s_values, mut s_children) = deserialize_node(sib_page);

        // move parent key down to child
        c_keys.insert(0, p_keys[idx - 1]);
        c_values.insert(0, p_values[idx - 1].clone());

        // move last key from sibling up to parent
        p_keys[idx - 1] = s_keys.pop().unwrap();
        p_values[idx - 1] = s_values.pop().unwrap();

        // move last child from sibling to child
        if !s_leaf {
            c_children.insert(0, s_children.pop().unwrap());
        }

        // write all three nodes back
        let data = serialize_node(p_leaf, &p_keys, &p_values, &p_children);
        self.pool.write_page(parent_id, data);
        let data = serialize_node(c_leaf, &c_keys, &c_values, &c_children);
        self.pool.write_page(child_id, data);
        let data = serialize_node(s_leaf, &s_keys, &s_values, &s_children);
        self.pool.write_page(sib_id, data);
    }

    fn borrow_from_next(&mut self, parent_id: u32, idx: usize) {
        let page = self.pool.get_page(parent_id);
        let (p_leaf, mut p_keys, mut p_values, p_children) = deserialize_node(page);

        let child_id = p_children[idx];
        let sib_id = p_children[idx + 1];

        let child_page = self.pool.get_page(child_id);
        let (c_leaf, mut c_keys, mut c_values, mut c_children) = deserialize_node(child_page);

        let sib_page = self.pool.get_page(sib_id);
        let (s_leaf, mut s_keys, mut s_values, mut s_children) = deserialize_node(sib_page);

        // move parent key down to child
        c_keys.push(p_keys[idx]);
        c_values.push(p_values[idx].clone());

        // move first key from sibling up to parent
        p_keys[idx] = s_keys.remove(0);
        p_values[idx] = s_values.remove(0);

        // move first child from sibling to child
        if !s_leaf {
            c_children.push(s_children.remove(0));
        }

        // write all three nodes back
        let data = serialize_node(p_leaf, &p_keys, &p_values, &p_children);
        self.pool.write_page(parent_id, data);
        let data = serialize_node(c_leaf, &c_keys, &c_values, &c_children);
        self.pool.write_page(child_id, data);
        let data = serialize_node(s_leaf, &s_keys, &s_values, &s_children);
        self.pool.write_page(sib_id, data);
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

        tree.insert(10, b"ten".to_vec());
        tree.insert(20, b"twenty".to_vec());
        tree.insert(5, b"five".to_vec());

        assert_eq!(tree.search(10), Some(b"ten".to_vec()));
        assert_eq!(tree.search(20), Some(b"twenty".to_vec()));
        assert_eq!(tree.search(5), Some(b"five".to_vec()));
        assert_eq!(tree.search(99), None);

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_insert_triggers_split() {
        let filename = "test_disk_btree_split.db";
        let mut tree = make_tree(filename, 2);

        // degree 2 = max 3 keys per node, will trigger splits
        for i in 0..10 {
            tree.insert(i, format!("val_{}", i).into_bytes());
        }

        for i in 0..10 {
            assert_eq!(tree.search(i), Some(format!("val_{}", i).into_bytes()));
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
            tree.insert(1, b"one".to_vec());
            tree.insert(2, b"two".to_vec());
            tree.insert(3, b"three".to_vec());
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

    #[test]
    fn test_delete_from_leaf() {
        let filename = "test_disk_delete_leaf.db";
        let mut tree = make_tree(filename, 2);
        tree.insert(1, b"one".to_vec());
        tree.insert(2, b"two".to_vec());
        tree.insert(3, b"three".to_vec());
        tree.delete(2);
        assert_eq!(tree.search(2), None);
        assert_eq!(tree.search(1), Some(b"one".to_vec()));
        assert_eq!(tree.search(3), Some(b"three".to_vec()));
        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_delete_nonexistent() {
        let filename = "test_disk_delete_nonexist.db";
        let mut tree = make_tree(filename, 2);
        tree.insert(1, b"one".to_vec());
        tree.delete(99);
        assert_eq!(tree.search(1), Some(b"one".to_vec()));
        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_delete_all_keys() {
        let filename = "test_disk_delete_all.db";
        let mut tree = make_tree(filename, 2);
        for i in 0..10 {
            tree.insert(i, format!("v{}", i).into_bytes());
        }
        for i in 0..10 {
            tree.delete(i);
        }
        for i in 0..10 {
            assert_eq!(tree.search(i), None);
        }
        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_delete_triggers_merge() {
        let filename = "test_disk_delete_merge.db";
        let mut tree = make_tree(filename, 2);
        for i in 0..7 {
            tree.insert(i, format!("v{}", i).into_bytes());
        }
        tree.delete(0);
        tree.delete(1);
        tree.delete(2);
        for i in 3..7 {
            assert_eq!(tree.search(i), Some(format!("v{}", i).into_bytes()));
        }
        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_delete_triggers_borrow() {
        let filename = "test_disk_delete_borrow.db";
        let mut tree = make_tree(filename, 2);
        for i in 0..10 {
            tree.insert(i, format!("v{}", i).into_bytes());
        }
        tree.delete(0);
        for i in 1..10 {
            assert_eq!(tree.search(i), Some(format!("v{}", i).into_bytes()));
        }
        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_delete_many_keys_different_degrees() {
        for degree in 2..=5 {
            let filename = format!("test_disk_delete_deg{}.db", degree);
            let mut tree = make_tree(&filename, degree);
            for i in 0..100 {
                tree.insert(i, format!("v{}", i).into_bytes());
            }
            for i in 0..100 {
                tree.delete(i);
                assert_eq!(tree.search(i), None);
            }
            std::fs::remove_file(&filename).unwrap();
        }
    }
}
