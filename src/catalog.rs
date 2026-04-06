use std::cell::RefCell;
use std::rc::Rc;

use crate::buffer::BufferPool;
use crate::disk::PAGE_SIZE;
use crate::page::{PAGE_HEADER_SIZE, serialize_node};
use crate::table::{Column, DataType, Schema, Table};

pub struct Catalog {
    pool: Rc<RefCell<BufferPool>>,
    tables: Vec<TableMeta>,
    next_page_id: u32,
}

pub struct TableMeta {
    pub name: String,
    pub schema: Schema,
    pub root_page_id: u32,
    pub degree: usize,
}

impl Catalog {
    pub fn new(pool: BufferPool) -> Self {
        let pool = Rc::new(RefCell::new(pool));
        let page = pool.borrow_mut().get_page(0).clone();
        let (tables, next_page_id) = Catalog::deserialize_catalog(&page);

        if next_page_id > 0 {
            Catalog {
                pool,
                tables,
                next_page_id,
            }
        } else {
            Catalog {
                pool,
                tables: Vec::new(),
                next_page_id: 1,
            }
        }
    }
    pub fn create_table(&mut self, name: &str, schema: Schema, degree: usize) {
        let root_page_id = self.next_page_id;
        self.next_page_id += 1;

        let data = serialize_node(true, &[], &[], &[]);
        self.pool.borrow_mut().write_page(root_page_id, data);

        self.tables.push(TableMeta {
            name: name.to_string(),
            schema,
            root_page_id,
            degree,
        });
    }

    pub fn get_table_meta(&self, name: &str) -> Option<&TableMeta> {
        self.tables.iter().find(|t| t.name == name)
    }

    pub fn get_table(&self, name: &str) -> Option<Table> {
        let meta = self.tables.iter().find(|t| t.name == name)?;
        Some(Table::open(
            &meta.name,
            meta.schema.clone(),
            self.pool.clone(),
            meta.root_page_id,
            self.next_page_id,
            meta.degree,
        ))
    }

    pub fn serialize_catalog(tables: &[TableMeta], next_page_id: u32) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        let mut offset = PAGE_HEADER_SIZE;

        buf[offset..offset + 4].copy_from_slice(&next_page_id.to_le_bytes());
        offset += 4;

        // write num_tables
        let num_tables = tables.len() as u16;
        buf[offset..offset + 2].copy_from_slice(&num_tables.to_le_bytes());
        offset += 2;

        for table in tables {
            let name_bytes = table.name.as_bytes();
            let name_len = name_bytes.len() as u16;
            buf[offset..offset + 2].copy_from_slice(&name_len.to_le_bytes());
            offset += 2;
            buf[offset..offset + name_bytes.len()].copy_from_slice(name_bytes);
            offset += name_bytes.len();

            buf[offset..offset + 4].copy_from_slice(&table.root_page_id.to_le_bytes());
            offset += 4;

            buf[offset..offset + 4].copy_from_slice(&(table.degree as u32).to_le_bytes());
            offset += 4;

            buf[offset..offset + 4]
                .copy_from_slice(&(table.schema.primary_key as u32).to_le_bytes());
            offset += 4;

            let num_cols = table.schema.columns.len() as u16;
            buf[offset..offset + 2].copy_from_slice(&num_cols.to_le_bytes());
            offset += 2;

            for col in &table.schema.columns {
                let col_bytes = col.name.as_bytes();
                let col_len = col_bytes.len() as u16;
                buf[offset..offset + 2].copy_from_slice(&col_len.to_le_bytes());
                offset += 2;
                buf[offset..offset + col_bytes.len()].copy_from_slice(col_bytes);
                offset += col_bytes.len();

                match col.data_type {
                    DataType::Int => {
                        buf[offset] = 0;
                        offset += 1;
                        buf[offset..offset + 4].copy_from_slice(&0u32.to_le_bytes());
                        offset += 4;
                    }
                    DataType::Varchar(max_len) => {
                        buf[offset] = 1;
                        offset += 1;
                        buf[offset..offset + 4].copy_from_slice(&(max_len as u32).to_le_bytes());
                        offset += 4;
                    }
                    DataType::Bool => {
                        buf[offset] = 2;
                        offset += 1;
                        buf[offset..offset + 4].copy_from_slice(&0u32.to_le_bytes());
                        offset += 4;
                    }
                }
            }
        }
        buf
    }

    pub fn deserialize_catalog(buf: &[u8; PAGE_SIZE]) -> (Vec<TableMeta>, u32) {
        let mut offset = PAGE_HEADER_SIZE;
        let mut tables = Vec::new();

        let next_page_id = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        offset += 4;

        let num_tables = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        for _ in 0..num_tables {
            let name_len = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
            offset += 2;

            let name = String::from_utf8_lossy(&buf[offset..offset + name_len]).to_string();
            offset += name_len;

            let root_page_id = u32::from_le_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]);

            offset += 4;

            let degree = u32::from_le_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]) as usize;

            offset += 4;

            let pk = u32::from_le_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]) as usize;

            offset += 4;

            let num_cols = u16::from_le_bytes([buf[offset], buf[offset + 1]]);

            offset += 2;

            let mut columns: Vec<Column> = Vec::new();
            for _ in 0..num_cols {
                let column_len = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
                offset += 2;
                let column_name =
                    String::from_utf8_lossy(&buf[offset..offset + column_len]).to_string();

                offset += column_len;

                let tag = buf[offset];
                offset += 1;

                let max_len = u32::from_le_bytes([
                    buf[offset],
                    buf[offset + 1],
                    buf[offset + 2],
                    buf[offset + 3],
                ]) as usize;
                offset += 4;

                let data_type = match tag {
                    0 => DataType::Int,
                    1 => DataType::Varchar(max_len),
                    2 => DataType::Bool,
                    _ => panic!("unknown data type tag"),
                };

                columns.push(Column {
                    name: column_name,
                    data_type,
                });
            }

            let schema = Schema {
                columns,
                primary_key: pk,
            };
            tables.push(TableMeta {
                name,
                schema,
                root_page_id,
                degree,
            });
        }

        (tables, next_page_id)
    }

    pub fn update_next_page_id(&mut self, id: u32) {
        if id > self.next_page_id {
            self.next_page_id = id;
        }
    }

    pub fn flush(&mut self) {
        let data = Catalog::serialize_catalog(&self.tables, self.next_page_id);
        self.pool.borrow_mut().write_page(0, data);
        self.pool.borrow_mut().flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::DiskManager;
    use crate::table::Value;

    #[test]
    fn test_catalog_persistence() {
        let filename = "test_catalog_persist.db";

        {
            let dm = DiskManager::new(filename);
            let pool = BufferPool::new(dm, 64);
            let mut catalog = Catalog::new(pool);

            catalog.create_table("jobs", Schema {
                columns: vec![
                    Column { name: "id".to_string(), data_type: DataType::Int },
                    Column { name: "title".to_string(), data_type: DataType::Varchar(64) },
                ],
                primary_key: 0,
            }, 3);

            let mut table = catalog.get_table("jobs").unwrap();
            table.insert_row(&[Value::Int(1), Value::Varchar("fix bug".to_string())]);
            table.insert_row(&[Value::Int(2), Value::Varchar("add tests".to_string())]);
            catalog.update_next_page_id(table.next_page_id());
            catalog.flush();
        }

        {
            let dm = DiskManager::new(filename);
            let pool = BufferPool::new(dm, 64);
            let mut catalog = Catalog::new(pool);

            let mut table = catalog.get_table("jobs").unwrap();

            let row1 = table.get_row(1).unwrap();
            match &row1[0] { Value::Int(n) => assert_eq!(*n, 1), _ => panic!("wrong type") }
            match &row1[1] { Value::Varchar(s) => assert_eq!(s, "fix bug"), _ => panic!("wrong type") }

            let row2 = table.get_row(2).unwrap();
            match &row2[0] { Value::Int(n) => assert_eq!(*n, 2), _ => panic!("wrong type") }
            match &row2[1] { Value::Varchar(s) => assert_eq!(s, "add tests"), _ => panic!("wrong type") }

            assert!(table.get_row(99).is_none());
        }

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_serialize_deserialize_catalog() {
        let tables = vec![
            TableMeta {
                name: "users".to_string(),
                schema: Schema {
                    columns: vec![
                        Column { name: "id".to_string(), data_type: DataType::Int },
                        Column { name: "name".to_string(), data_type: DataType::Varchar(32) },
                        Column { name: "active".to_string(), data_type: DataType::Bool },
                    ],
                    primary_key: 0,
                },
                root_page_id: 1,
                degree: 3,
            },
            TableMeta {
                name: "jobs".to_string(),
                schema: Schema {
                    columns: vec![
                        Column { name: "id".to_string(), data_type: DataType::Int },
                        Column { name: "title".to_string(), data_type: DataType::Varchar(64) },
                    ],
                    primary_key: 0,
                },
                root_page_id: 5,
                degree: 4,
            },
        ];

        let buf = Catalog::serialize_catalog(&tables, 10);
        let (result, next_page_id) = Catalog::deserialize_catalog(&buf);

        assert_eq!(next_page_id, 10);
        assert_eq!(result.len(), 2);

        assert_eq!(result[0].name, "users");
        assert_eq!(result[0].root_page_id, 1);
        assert_eq!(result[0].degree, 3);
        assert_eq!(result[0].schema.primary_key, 0);
        assert_eq!(result[0].schema.columns.len(), 3);
        assert_eq!(result[0].schema.columns[1].name, "name");

        assert_eq!(result[1].name, "jobs");
        assert_eq!(result[1].root_page_id, 5);
        assert_eq!(result[1].degree, 4);
        assert_eq!(result[1].schema.columns.len(), 2);
    }
}
