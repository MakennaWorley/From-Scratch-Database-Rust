use std::collections::HashMap;
use crate::table::model::value::Value;
use crate::table::model::column::Column;
use crate::table::model::index::IndexType;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
    pub primary_key: Option<Vec<String>>,
    pub indexes: HashMap<String, IndexType>,
    pub transaction_backup: Option<Vec<Vec<Value>>>,
}
