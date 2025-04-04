use crate::table::data::{DataType, Table, Value};
use std::collections::{HashMap, HashSet};

impl Table {
    pub fn truncate(&mut self) {
        self.rows.clear();
        self.indexes.clear();
    }

    pub fn value_matches_type(val: &Value, dtype: &DataType) -> bool {
        match (val, dtype) {
            (Value::Char(_), DataType::Char) => true,
            (Value::Varchar(_), DataType::Varchar) => true,
            (Value::Text(_), DataType::Text) => true,
            (Value::Enum(_, _), DataType::Enum) => true,
            (Value::Set(_, _), DataType::Set) => true,
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Int(_), DataType::Int) => true,
            (Value::BigInt(_), DataType::BigInt) => true,
            (Value::Float(_), DataType::Float) => true,
            (Value::Double(_), DataType::Double) => true,
            (Value::Date(_), DataType::Date) => true,
            (Value::Time(_), DataType::Time) => true,
            (Value::DateTime(_), DataType::DateTime) => true,
            (Value::Null, _) => true, // Allow null everywhere for now
            _ => false,
        }
    }

    pub fn union(&self, other: &Table) -> Result<Table, String> {
        if self.columns.len() != other.columns.len() {
            return Err("Tables have different number of columns".to_string());
        }
        for (col1, col2) in self.columns.iter().zip(other.columns.iter()) {
            if col1.name != col2.name || col1.datatype != col2.datatype {
                return Err("Table schemas do not match".to_string());
            }
        }
        let mut new_rows = self.rows.clone();
        new_rows.extend(other.rows.clone());
        let mut seen = HashSet::new();
        new_rows.retain(|row| {
            let key = row.iter()
                .map(|v| v.to_display_string())
                .collect::<Vec<_>>()
                .join(",");
            if seen.contains(&key) { false } else { seen.insert(key); true }
        });
        Ok(Table {
            name: format!("union_{}_{}", self.name, other.name),
            columns: self.columns.clone(),
            rows: new_rows,
            primary_key: None,
            indexes: HashMap::new(),
            transaction_backup: None,
        })
    }

    pub fn intersect(&self, other: &Table) -> Result<Table, String> {
        if self.columns.len() != other.columns.len() {
            return Err("Tables have different number of columns".to_string());
        }
        let other_set: HashSet<String> = other.rows.iter().map(|row| {
            row.iter().map(|v| v.to_display_string()).collect::<Vec<_>>().join(",")
        }).collect();
        let new_rows: Vec<Vec<Value>> = self.rows.iter().filter(|row| {
            let key = row.iter().map(|v| v.to_display_string()).collect::<Vec<_>>().join(",");
            other_set.contains(&key)
        }).cloned().collect();
        Ok(Table {
            name: format!("intersect_{}_{}", self.name, other.name),
            columns: self.columns.clone(),
            rows: new_rows,
            primary_key: None,
            indexes: HashMap::new(),
            transaction_backup: None,
        })
    }

    pub fn except(&self, other: &Table) -> Result<Table, String> {
        if self.columns.len() != other.columns.len() {
            return Err("Tables have different number of columns".to_string());
        }
        let other_set: HashSet<String> = other.rows.iter().map(|row| {
            row.iter().map(|v| v.to_display_string()).collect::<Vec<_>>().join(",")
        }).collect();
        let new_rows: Vec<Vec<Value>> = self.rows.iter().filter(|row| {
            let key = row.iter().map(|v| v.to_display_string()).collect::<Vec<_>>().join(",");
            !other_set.contains(&key)
        }).cloned().collect();
        Ok(Table {
            name: format!("except_{}_{}", self.name, other.name),
            columns: self.columns.clone(),
            rows: new_rows,
            primary_key: None,
            indexes: HashMap::new(),
            transaction_backup: None,
        })
    }
}
