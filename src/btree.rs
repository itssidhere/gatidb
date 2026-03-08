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