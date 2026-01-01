use std::collections::{BTreeMap, HashMap};
use crate::table::model::Value;

#[derive(Debug, Clone)]
pub enum IndexType {
    Hash(HashMap<Value, Vec<usize>>),
    BTree(BTreeMap<Value, Vec<usize>>),
}

impl IndexType {
    pub fn get(&self, key: &Value) -> Option<&Vec<usize>> {
        match self {
            IndexType::Hash(map) => map.get(key),
            IndexType::BTree(map) => map.get(key),
        }
    }
}