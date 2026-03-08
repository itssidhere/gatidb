mod btree;
fn main() {
    let mut tree = btree::BTree::new(2);

    for i in 0..20 {
        tree.insert(i, format!("value_{}", i));
    }

    println!("{:?}", tree.search(0));
    println!("{:?}", tree.search(10));
    println!("{:?}", tree.search(19));
    println!("{:?}", tree.search(99));
}
