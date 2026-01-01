use crate::table::model::{Column, IndexType, Value};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
    pub primary_key: Option<Vec<String>>,
    pub indexes: HashMap<String, IndexType>,
    pub transaction_backup: Option<Vec<Vec<Value>>>,
}

impl Table {
    pub fn clone_data_only(&self) -> Table {
        Table {
            name: self.name.clone(),
            columns: self.columns.clone(),
            rows: self.rows.clone(),
            primary_key: self.primary_key.clone(),
            indexes: HashMap::new(),
            transaction_backup: None,
        }
    }
}