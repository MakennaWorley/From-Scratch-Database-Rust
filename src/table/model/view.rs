use std::collections::HashMap;
use crate::table::model::{Column, IndexType, Value, Table};

pub struct View<'a> {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
    pub primary_key: Option<Vec<String>>,
    pub indexes: HashMap<String, IndexType>,
    pub transaction_backup: Option<Vec<Vec<Value>>>,
    pub builder: Box<dyn Fn() -> Result<Table, String> + 'a>,
}