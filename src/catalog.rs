use std::cell::RefCell;
use std::rc::Rc;

use crate::buffer::BufferPool;
use crate::page::serialize_node;
use crate::table::{Schema, Table, Value};

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

    pub fn flush(&mut self) {
        self.pool.borrow_mut().flush();
    }
}
