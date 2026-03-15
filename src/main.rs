use gatidb::{
    btree::BTree,
    buffer::BufferPool,
    catalog::Catalog,
    disk::{self, DiskManager},
    table::{Column, DataType, Schema, Table, Value},
};

fn main() {
    let dm = DiskManager::new("gatidb.db");
    let pool = BufferPool::new(dm);
    let mut catalog = Catalog::new(pool);

    let schema = Schema {
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            Column {
                name: "comment".to_string(),
                data_type: DataType::Varchar(128),
            },
        ],
        primary_key: 0,
    };

    catalog.create_table("jobs", schema, 64);

    let mut jobs = catalog.get_table("jobs").unwrap();
    jobs.insert_row(&[Value::Int(1), Value::Varchar("fix bug".to_string())]);

    println!("{:?}", jobs.get_row(1))
}
