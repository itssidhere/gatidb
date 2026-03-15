use std::cell::RefCell;
use std::rc::Rc;

use crate::buffer::BufferPool;
use crate::disk::PAGE_SIZE;
use crate::page::serialize_node;
use crate::table::{self, DataType, Schema, Table, Value};

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
        Catalog {
            pool: Rc::new(RefCell::new(pool)),
            tables: Vec::new(),
            next_page_id: 1,
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

    pub fn serialize_catalog(tables: &[TableMeta]) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        let mut offset = 0;

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

    pub fn deserialize_node(buf: &[u8; PAGE_SIZE]) -> Vec<TableMeta> {
        let mut offset = 0;
        let mut tables = Vec::new();

        tables
    }

    pub fn flush(&mut self) {
        self.pool.borrow_mut().flush();
    }
}
