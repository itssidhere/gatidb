pub struct BTreeNode{
    keys: Vec<i32>,
    values: Vec<String>,
    children: Vec<BTreeNode>,
    is_leaf: bool
}

pub struct BTree{
    root: BTreeNode,
    degree: usize,
}

impl BTree{
    pub fn new(degree: usize) -> BTree {
        BTree { root: BTreeNode::new(true), degree }
    }

    pub fn insert(&mut self, key: i32, value: String){
        let degree = self.degree;

        if self.root.keys.len() == 2 * degree - 1{
            // root is full - create a new root
            let mut new_root = BTreeNode::new(false);
            let old_root = std::mem::replace(&mut self.root, BTreeNode::new(true));

            new_root.children.push(old_root);
            new_root.split_child(0, degree);
            new_root.insert_non_full(key, value, degree);
            self.root = new_root;
        }else{
            self.root.insert_non_full(key, value, degree);
        }
    }

    pub fn search(&self, key: i32) -> Option<&String>{
        Self::search_node(&self.root, key)
    }

    fn search_node(node: &BTreeNode, key:i32) -> Option<&String>{
        let pos = node.keys.iter().position(|k| *k >= key);

        match pos{
            Some(i) if node.keys[i] == key => Some(&node.values[i]),
            Some(i) if !node.is_leaf => Self::search_node(&node.children[i], key),
            None if !node.is_leaf => Self::search_node(node.children.last().unwrap(), key),
            _=>None,
        }
    }

}

impl BTreeNode {
    fn new(is_leaf: bool) -> BTreeNode {
        BTreeNode { keys: Vec::new(), values: Vec::new(), children: Vec::new(), is_leaf }
    }

    fn split_child(&mut self, i:usize, degree:usize){
        let full_child = &mut self.children[i];
        let mid = degree - 1; // index of median key

        // create a new node with the right half of the full child
        let mut new_node = BTreeNode::new(full_child.is_leaf);
        new_node.keys = full_child.keys.split_off(mid+1);
        new_node.values = full_child.values.split_off(mid+1);

        if !full_child.is_leaf{
            new_node.children = full_child.children.split_off(mid+1);
        }

        // pop the median key/value from the full child
        let median_key = full_child.keys.pop().unwrap();
        let median_value = full_child.values.pop().unwrap();

        // insert median into parent (self)
        self.keys.insert(i , median_key);
        self.values.insert(i, median_value);
        self.children.insert(i+1, new_node);

    }

    fn insert_non_full(&mut self, key:i32, value: String, degree:usize){
        if self.is_leaf {
            // find position and insert directly
            let pos = self.keys.iter().position(|k| *k >= key).unwrap_or(self.keys.len());
            self.keys.insert(pos, key);
            self.values.insert(pos, value);
        } else{
            let mut i = self.keys.iter().position(|k| *k >= key).unwrap_or(self.keys.len());

            // if that child is full split it first
            if self.children[i].keys.len() == 2 * degree - 1{
                self.split_child(i, degree);

                if key > self.keys[i]{
                    i += 1;
                }
            }

            self.children[i].insert_non_full(key, value, degree);
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
}