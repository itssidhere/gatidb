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
