use gatidb::{
    buffer::BufferPool,
    catalog::Catalog,
    disk::DiskManager,
    table::{Column, DataType, Schema, Value},
    wal::Wal,
};

fn main() {
    {
        let dm = DiskManager::new("gatidb.db");
        let wal = Wal::new("gatidb.wal");
        let pool = BufferPool::new(dm, wal, 64);
        let mut catalog = Catalog::new(pool);

        catalog.create_table(
            "jobs",
            Schema {
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        data_type: DataType::Int,
                    },
                    Column {
                        name: "title".to_string(),
                        data_type: DataType::Varchar(64),
                    },
                ],
                primary_key: 0,
            },
            3,
        );

        let mut table = catalog.get_table("jobs").unwrap();
        table.insert_row(&[Value::Int(1), Value::Varchar("fix bug".to_string())]);
        catalog.update_next_page_id(table.next_page_id());
        catalog.flush();
        println!("Written!");
    }

    {
        let dm = DiskManager::new("gatidb.db");
        let wal = Wal::new("gatidb.wal");
        let pool = BufferPool::new(dm, wal, 64);
        let catalog = Catalog::new(pool);

        let mut table = catalog.get_table("jobs").unwrap();
        let row = table.get_row(1);
        println!("Read back: {:?}", row);
    }

    let _ = std::fs::remove_file("gatidb.db");
    let _ = std::fs::remove_file("gatidb.wal");
}
