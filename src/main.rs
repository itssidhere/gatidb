use gatidb::{
    btree::BTree,
    buffer::BufferPool,
    disk::{self, DiskManager},
    table::{Column, DataType, Schema, Table, Value},
};

fn main() {
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
    let disk_manager = DiskManager::new("disk_mgr");
    let pool = BufferPool::new(disk_manager);
    let mut table = Table::new("jobs", schema, pool, 64);

    table.insert_row(&[Value::Int(1), Value::Varchar("fix the bug".to_string())]);
    table.insert_row(&[Value::Int(2), Value::Varchar("deploy to prod".to_string())]);

    if let Some(row) = table.get_row(1) {
        println!("{:?}", row)
    }

    table.flush();
}
