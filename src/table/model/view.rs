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