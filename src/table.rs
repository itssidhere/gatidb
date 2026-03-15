use crate::buffer::BufferPool;
use crate::disk_btree::DiskBtree;

#[derive(Debug, Clone)]
pub enum DataType {
    Int,
    Varchar(usize),
    Bool,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub enum Value {
    Int(i32),
    Varchar(String),
    Bool(bool),
    Null,
}

pub struct Schema {
    pub columns: Vec<Column>,
    pub primary_key: usize,
}

impl Schema {
    pub fn encode_row(&self, row: &[Value]) -> Vec<u8> {
        let mut buf = Vec::new();

        for (col, val) in self.columns.iter().zip(row.iter()) {
            match (&col.data_type, val) {
                (DataType::Int, Value::Int(n)) => {
                    buf.extend_from_slice(&n.to_le_bytes());
                }
                (DataType::Varchar(max_len), Value::Varchar(s)) => {
                    let bytes = s.as_bytes();
                    let len = bytes.len().min(*max_len);
                    buf.extend_from_slice(&bytes[..len]);
                    // pad remaining with zeros
                    for _ in len..*max_len {
                        buf.push(0);
                    }
                }
                (DataType::Bool, Value::Bool(b)) => {
                    buf.push(*b as u8);
                }
                _ => panic!("type mismatch"),
            }
        }

        buf
    }
    pub fn decode_row(&self, data: &[u8]) -> Vec<Value> {
        let mut row = Vec::new();
        let mut offset = 0;

        for col in &self.columns {
            match col.data_type {
                DataType::Int => {
                    let n = i32::from_le_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    row.push(Value::Int(n));
                    offset += 4;
                }
                DataType::Varchar(max_len) => {
                    let end = data[offset..offset + max_len]
                        .iter()
                        .position(|&b| b == 0)
                        .unwrap_or(max_len);

                    let s = String::from_utf8_lossy(&data[offset..offset + end]).to_string();
                    row.push(Value::Varchar(s));
                    offset += max_len;
                }
                DataType::Bool => {
                    row.push(Value::Bool(data[offset] != 0));
                    offset += 1
                }
            }
        }
        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn users_schema() -> Schema {
        Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Varchar(64),
                },
                Column {
                    name: "age".to_string(),
                    data_type: DataType::Int,
                },
                Column {
                    name: "active".to_string(),
                    data_type: DataType::Bool,
                },
            ],
            primary_key: 0,
        }
    }

    #[test]
    fn test_encode_decode_row() {
        let schema = users_schema();
        let row = vec![
            Value::Int(1),
            Value::Varchar("Alice".to_string()),
            Value::Int(25),
            Value::Bool(true),
        ];

        let bytes = schema.encode_row(&row);
        let decoded = schema.decode_row(&bytes);

        assert_eq!(decoded.len(), 4);
        match &decoded[0] {
            Value::Int(n) => assert_eq!(*n, 1),
            _ => panic!("wrong type"),
        }
        match &decoded[1] {
            Value::Varchar(s) => assert_eq!(s, "Alice"),
            _ => panic!("wrong type"),
        }
        match &decoded[2] {
            Value::Int(n) => assert_eq!(*n, 25),
            _ => panic!("wrong type"),
        }
        match &decoded[3] {
            Value::Bool(b) => assert_eq!(*b, true),
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn test_table_insert_and_get() {
        use crate::disk::DiskManager;

        let filename = "test_table.db";
        let dm = DiskManager::new(filename);
        let pool = BufferPool::new(dm);
        let schema = users_schema();
        let mut table = Table::new("users", schema, pool, 2);

        table.insert_row(&[
            Value::Int(1),
            Value::Varchar("Alice".to_string()),
            Value::Int(25),
            Value::Bool(true),
        ]);

        table.insert_row(&[
            Value::Int(2),
            Value::Varchar("Bob".to_string()),
            Value::Int(30),
            Value::Bool(false),
        ]);

        // get row by primary key
        let row = table.get_row(1).unwrap();
        match &row[0] { Value::Int(n) => assert_eq!(*n, 1), _ => panic!() }
        match &row[1] { Value::Varchar(s) => assert_eq!(s, "Alice"), _ => panic!() }
        match &row[2] { Value::Int(n) => assert_eq!(*n, 25), _ => panic!() }
        match &row[3] { Value::Bool(b) => assert_eq!(*b, true), _ => panic!() }

        let row = table.get_row(2).unwrap();
        match &row[1] { Value::Varchar(s) => assert_eq!(s, "Bob"), _ => panic!() }

        // missing row
        assert!(table.get_row(99).is_none());

        // delete
        table.delete_row(1);
        assert!(table.get_row(1).is_none());
        assert!(table.get_row(2).is_some());

        std::fs::remove_file(filename).unwrap();
    }
}

pub struct Table {
    pub name: String,
    pub schema: Schema,
    tree: DiskBtree,
}

impl Table {
    pub fn new(name: &str, schema: Schema, pool: BufferPool, degree: usize) -> Self {
        Table {
            name: name.to_string(),
            schema,
            tree: DiskBtree::new(pool, degree),
        }
    }

    pub fn insert_row(&mut self, row: &[Value]) {
        let pk = match &row[self.schema.primary_key] {
            Value::Int(n) => *n,
            _ => panic!("primary key must be an Int"),
        };

        let encoded = self.schema.encode_row(row);
        self.tree.insert(pk, encoded);
    }

    pub fn get_row(&mut self, pk: i32) -> Option<Vec<Value>> {
        self.tree.search(pk).map(|bytes| self.schema.decode_row(&bytes))
    }

    pub fn delete_row(&mut self, pk: i32) {
        self.tree.delete(pk);
    }
    pub fn flush(&mut self) {
        self.tree.flush();
    }
}
