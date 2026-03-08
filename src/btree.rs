pub struct BTreeNode {
    keys: Vec<i32>,
    values: Vec<String>,
    children: Vec<BTreeNode>,
    is_leaf: bool,
}

pub struct BTree {
    root: BTreeNode,
    degree: usize,
}

impl BTree {
    pub fn new(degree: usize) -> BTree {
        BTree {
            root: BTreeNode::new(true),
            degree,
        }
    }

    pub fn insert(&mut self, key: i32, value: String) {
        let degree = self.degree;

        if self.root.keys.len() == 2 * degree - 1 {
            // root is full - create a new root
            let mut new_root = BTreeNode::new(false);
            let old_root = std::mem::replace(&mut self.root, BTreeNode::new(true));

            new_root.children.push(old_root);
            new_root.split_child(0, degree);
            new_root.insert_non_full(key, value, degree);
            self.root = new_root;
        } else {
            self.root.insert_non_full(key, value, degree);
        }
    }

    pub fn search(&self, key: i32) -> Option<&String> {
        Self::search_node(&self.root, key)
    }

    fn search_node(node: &BTreeNode, key: i32) -> Option<&String> {
        let pos = node.keys.iter().position(|k| *k >= key);

        match pos {
            Some(i) if node.keys[i] == key => Some(&node.values[i]),
            Some(i) if !node.is_leaf => Self::search_node(&node.children[i], key),
            None if !node.is_leaf => Self::search_node(node.children.last().unwrap(), key),
            _ => None,
        }
    }

    pub fn delete(&mut self, key: i32) {
        self.root.delete_key(key, self.degree);

        // if root has no keys but has children, make the first child the new root

        if self.root.keys.is_empty() && !self.root.is_leaf {
            self.root = self.root.children.remove(0);
        }
    }
}

impl BTreeNode {
    fn new(is_leaf: bool) -> BTreeNode {
        BTreeNode {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            is_leaf,
        }
    }

    fn split_child(&mut self, i: usize, degree: usize) {
        let full_child = &mut self.children[i];
        let mid = degree - 1; // index of median key

        // create a new node with the right half of the full child
        let mut new_node = BTreeNode::new(full_child.is_leaf);
        new_node.keys = full_child.keys.split_off(mid + 1);
        new_node.values = full_child.values.split_off(mid + 1);

        if !full_child.is_leaf {
            new_node.children = full_child.children.split_off(mid + 1);
        }

        // pop the median key/value from the full child
        let median_key = full_child.keys.pop().unwrap();
        let median_value = full_child.values.pop().unwrap();

        // insert median into parent (self)
        self.keys.insert(i, median_key);
        self.values.insert(i, median_value);
        self.children.insert(i + 1, new_node);
    }

    fn insert_non_full(&mut self, key: i32, value: String, degree: usize) {
        if self.is_leaf {
            // find position and insert directly
            let pos = self
                .keys
                .iter()
                .position(|k| *k >= key)
                .unwrap_or(self.keys.len());
            self.keys.insert(pos, key);
            self.values.insert(pos, value);
        } else {
            let mut i = self
                .keys
                .iter()
                .position(|k| *k >= key)
                .unwrap_or(self.keys.len());

            // if that child is full split it first
            if self.children[i].keys.len() == 2 * degree - 1 {
                self.split_child(i, degree);

                if key > self.keys[i] {
                    i += 1;
                }
            }

            self.children[i].insert_non_full(key, value, degree);
        }
    }

    fn delete_key(&mut self, key: i32, degree: usize) {
        let idx = self.find_key(key);

        if idx < self.keys.len() && self.keys[idx] == key {
            // key is in this node

            if self.is_leaf {
                // case 1 : key is in a leaf - just remove it
                self.keys.remove(idx);
                self.values.remove(idx);
            } else {
                // case 2 : key is in an internal node

                if self.children[idx].keys.len() >= degree {
                    // case 2a: left child has enough keys - use predecessor
                    let (pred_key, pred_value) = self.children[idx].get_predecessor_key();
                    self.keys[idx] = pred_key;
                    self.values[idx] = pred_value;
                    self.children[idx].delete_key(self.keys[idx], degree);
                } else if self.children[idx + 1].keys.len() >= degree {
                    // case 2b: right child has enough keys - use successor
                    let (succ_key, succ_value) = self.children[idx + 1].get_successor_key();
                    self.keys[idx] = succ_key;
                    self.values[idx] = succ_value;
                    self.children[idx + 1].delete_key(self.keys[idx], degree);
                } else {
                    // case 2c: both children have minimum keys - merge them
                    self.merge(idx);
                    self.children[idx].delete_key(key, degree);
                }
            }
        } else {
            // case 3: key is not in this node, must be in a child
            if self.is_leaf {
                return; // key doesn't exist in tree
            }

            // check if child we need to descend into has minimum keys
            if self.children[idx].keys.len() < degree {
                self.fill(idx, degree);
            }

            // after fill, idx might have changed if we merged
            // if we merged the last child, go to idx - 1
            if idx >= self.children.len() {
                self.children[idx - 1].delete_key(key, degree);
            } else {
                self.children[idx].delete_key(key, degree);
            }
        }
    }

    fn find_key(&self, key: i32) -> usize {
        self.keys
            .iter()
            .position(|k| *k >= key)
            .unwrap_or(self.keys.len())
    }

    fn get_predecessor_key(&self) -> (i32, String) {
        let mut current = self;
        while !current.is_leaf {
            current = current.children.last().unwrap();
        }

        let last = current.keys.len() - 1;
        (current.keys[last].clone(), current.values[last].clone())
    }

    fn get_successor_key(&self) -> (i32, String) {
        let mut current = self;
        while !current.is_leaf {
            current = &current.children[0];
        }
        (current.keys[0].clone(), current.values[0].clone())
    }

    fn merge(&mut self, idx: usize) {
        let key = self.keys.remove(idx);
        let value = self.values.remove(idx);

        let mut right = self.children.remove(idx + 1);
        let left = &mut self.children[idx];

        left.keys.push(key);
        left.values.push(value);
        left.keys.append(&mut right.keys);
        left.values.append(&mut right.values);
        left.children.append(&mut right.children);
    }

    fn fill(&mut self, idx: usize, degree: usize) {
        if idx > 0 && self.children[idx - 1].keys.len() >= degree {
            // case 3a: borrow from left sibling
            self.borrow_from_prev(idx);
        } else if idx < self.children.len() - 1 && self.children[idx + 1].keys.len() >= degree {
            // case 3b: borrow from right sibling
            self.borrow_from_next(idx);
        } else {
            // case 3c: merge with a sibling
            if idx < self.children.len() - 1 {
                self.merge(idx);
            } else {
                self.merge(idx - 1);
            }
        }
    }

    fn borrow_from_prev(&mut self, idx: usize) {
        let prev_len = self.children[idx - 1].keys.len();

        // take last key/value from left sibling
        let sibling_key = self.children[idx - 1].keys.remove(prev_len - 1);
        let sibling_value = self.children[idx - 1].values.remove(prev_len - 1);

        // take last child from  left sibling (if  not leaf)
        let sibling_child = if !self.children[idx - 1].is_leaf {
            Some(self.children[idx - 1].children.remove(prev_len))
        } else {
            None
        };

        // pull parent key down to front of child
        let parent_key = std::mem::replace(&mut self.keys[idx - 1], sibling_key);
        let parent_value = std::mem::replace(&mut self.values[idx - 1], sibling_value);
        self.children[idx].keys.insert(0, parent_key);
        self.children[idx].values.insert(0, parent_value);

        if let Some(child) = sibling_child {
            self.children[idx].children.insert(0, child);
        }
    }

    fn borrow_from_next(&mut self, idx: usize) {
        // take first key/value from right sibling
        let sibling_key = self.children[idx + 1].keys.remove(0);
        let sibling_value = self.children[idx + 1].values.remove(0);

        // take first child from right sibling (if not leaf)
        let sibling_child = if !self.children[idx + 1].is_leaf {
            Some(self.children[idx + 1].children.remove(0))
        } else {
            None
        };

        // pull parent key down to end of child
        let parent_key = std::mem::replace(&mut self.keys[idx], sibling_key);
        let parent_value = std::mem::replace(&mut self.values[idx], sibling_value);

        self.children[idx].keys.push(parent_key);
        self.children[idx].values.push(parent_value);

        if let Some(child) = sibling_child {
            self.children[idx].children.push(child);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_tree_is_empty() {
        let tree = BTree::new(2);
        assert_eq!(tree.search(1), None);
    }

    #[test]
    fn test_insert_and_search_single() {
        let mut tree = BTree::new(2);
        tree.insert(5, String::from("five"));
        assert_eq!(tree.search(5), Some(&String::from("five")));
    }

    #[test]
    fn test_search_miss() {
        let mut tree = BTree::new(2);
        tree.insert(5, String::from("five"));
        assert_eq!(tree.search(99), None);
    }

    #[test]
    fn test_insert_multiple_sorted() {
        let mut tree = BTree::new(2);
        tree.insert(1, String::from("one"));
        tree.insert(2, String::from("two"));
        tree.insert(3, String::from("three"));
        assert_eq!(tree.search(1), Some(&String::from("one")));
        assert_eq!(tree.search(2), Some(&String::from("two")));
        assert_eq!(tree.search(3), Some(&String::from("three")));
    }

    #[test]
    fn test_insert_reverse_order() {
        let mut tree = BTree::new(2);
        tree.insert(10, String::from("ten"));
        tree.insert(5, String::from("five"));
        tree.insert(1, String::from("one"));
        assert_eq!(tree.search(1), Some(&String::from("one")));
        assert_eq!(tree.search(5), Some(&String::from("five")));
        assert_eq!(tree.search(10), Some(&String::from("ten")));
    }

    #[test]
    fn test_insert_triggers_split() {
        let mut tree = BTree::new(2); // max 3 keys per node
        tree.insert(1, String::from("one"));
        tree.insert(2, String::from("two"));
        tree.insert(3, String::from("three"));
        tree.insert(4, String::from("four")); // triggers root split
        assert_eq!(tree.search(1), Some(&String::from("one")));
        assert_eq!(tree.search(2), Some(&String::from("two")));
        assert_eq!(tree.search(3), Some(&String::from("three")));
        assert_eq!(tree.search(4), Some(&String::from("four")));
    }

    #[test]
    fn test_insert_many_keys() {
        let mut tree = BTree::new(3);
        for i in 0..100 {
            tree.insert(i, format!("value_{}", i));
        }
        for i in 0..100 {
            assert_eq!(tree.search(i), Some(&format!("value_{}", i)));
        }
        assert_eq!(tree.search(100), None);
    }

    #[test]
    fn test_insert_random_order() {
        let mut tree = BTree::new(2);
        let keys = vec![50, 20, 80, 10, 30, 70, 90, 5, 15, 25];
        for k in &keys {
            tree.insert(*k, format!("val_{}", k));
        }
        for k in &keys {
            assert_eq!(tree.search(*k), Some(&format!("val_{}", k)));
        }
    }

    #[test]
    fn test_different_degrees() {
        for degree in 2..=5 {
            let mut tree = BTree::new(degree);
            for i in 0..50 {
                tree.insert(i, format!("v{}", i));
            }
            for i in 0..50 {
                assert_eq!(tree.search(i), Some(&format!("v{}", i)));
            }
        }
    }

    #[test]
    fn test_delete_from_leaf() {
        let mut tree = BTree::new(2);
        tree.insert(1, String::from("one"));
        tree.insert(2, String::from("two"));
        tree.insert(3, String::from("three"));
        tree.delete(2);
        assert_eq!(tree.search(2), None);
        assert_eq!(tree.search(1), Some(&String::from("one")));
        assert_eq!(tree.search(3), Some(&String::from("three")));
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut tree = BTree::new(2);
        tree.insert(1, String::from("one"));
        tree.delete(99); // should not panic
        assert_eq!(tree.search(1), Some(&String::from("one")));
    }

    #[test]
    fn test_delete_only_key() {
        let mut tree = BTree::new(2);
        tree.insert(5, String::from("five"));
        tree.delete(5);
        assert_eq!(tree.search(5), None);
    }

    #[test]
    fn test_delete_from_internal_node() {
        let mut tree = BTree::new(2);
        for i in 0..10 {
            tree.insert(i, format!("v{}", i));
        }
        // delete a key that's likely in an internal node
        tree.delete(3);
        assert_eq!(tree.search(3), None);
        // verify other keys still exist
        for i in 0..10 {
            if i != 3 {
                assert_eq!(tree.search(i), Some(&format!("v{}", i)));
            }
        }
    }

    #[test]
    fn test_delete_all_keys() {
        let mut tree = BTree::new(2);
        for i in 0..10 {
            tree.insert(i, format!("v{}", i));
        }
        for i in 0..10 {
            tree.delete(i);
        }
        for i in 0..10 {
            assert_eq!(tree.search(i), None);
        }
    }

    #[test]
    fn test_delete_reverse_order() {
        let mut tree = BTree::new(2);
        for i in 0..10 {
            tree.insert(i, format!("v{}", i));
        }
        for i in (0..10).rev() {
            tree.delete(i);
            assert_eq!(tree.search(i), None);
            // remaining keys should still be found
            for j in 0..i {
                assert_eq!(tree.search(j), Some(&format!("v{}", j)));
            }
        }
    }

    #[test]
    fn test_delete_triggers_merge() {
        let mut tree = BTree::new(2);
        for i in 0..7 {
            tree.insert(i, format!("v{}", i));
        }
        // delete enough keys to trigger merges
        tree.delete(0);
        tree.delete(1);
        tree.delete(2);
        for i in 3..7 {
            assert_eq!(tree.search(i), Some(&format!("v{}", i)));
        }
    }

    #[test]
    fn test_delete_triggers_borrow() {
        let mut tree = BTree::new(2);
        for i in 0..10 {
            tree.insert(i, format!("v{}", i));
        }
        tree.delete(0);
        for i in 1..10 {
            assert_eq!(tree.search(i), Some(&format!("v{}", i)));
        }
    }

    #[test]
    fn test_delete_many_keys_different_degrees() {
        for degree in 2..=5 {
            let mut tree = BTree::new(degree);
            for i in 0..100 {
                tree.insert(i, format!("v{}", i));
            }
            for i in 0..100 {
                tree.delete(i);
                assert_eq!(tree.search(i), None);
            }
        }
    }
}
