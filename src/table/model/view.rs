use crate::table::model::{Column, IndexType, Table, Value};
use std::collections::HashMap;

pub struct View<'a> {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
    pub primary_key: Option<Vec<String>>,
    pub indexes: HashMap<String, IndexType>,
    pub transaction_backup: Option<Vec<Vec<Value>>>,
    pub builder: Box<dyn Fn() -> Result<Table, String> + 'a>,
}

impl<'a> View<'a> {
    pub fn new(
        name: &str,
        builder: Box<dyn Fn() -> Result<Table, String> + 'a>,
    ) -> Result<Self, String> {
        if name.trim().is_empty() {
            return Err("View name cannot be empty".to_string());
        }

        let built = builder()?;

        Ok(Self {
            name: name.to_string(),
            columns: built.columns.clone(),
            rows: Vec::new(),
            primary_key: built.primary_key.clone(),
            indexes: HashMap::new(),
            transaction_backup: None,
            builder,
        })
    }

    pub fn materialize(&mut self) -> Result<(), String> {
        let built = (self.builder)()?;
        self.columns = built.columns;
        self.rows = built.rows;
        self.primary_key = built.primary_key;
        self.indexes.clear();
        Ok(())
    }
}
