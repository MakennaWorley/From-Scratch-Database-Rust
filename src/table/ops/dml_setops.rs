use crate::table::model::{Column, Table, Value};
use std::collections::{HashMap, HashSet};

impl Table {
    // redo this to be like select with all parameters as optional for like where, order by, limit/offset, aggregations, etc
    // note: this db will not excute deletes or updates unless a where is given, but users can still nuke a db if they forget to add one, so auto place with where 1 = 1
    pub fn with_alias(&self, alias: &str) -> Table {
        use std::collections::HashSet;

        let mut seen_names = HashSet::new();
        let mut columns = vec![];

        for col in &self.columns {
            let mut col_name = format!("{}.{}", alias, col.name);
            while seen_names.contains(&col_name) {
                col_name.push('_');
            }
            seen_names.insert(col_name.clone());

            columns.push(Column {
                name: col_name,
                datatype: col.datatype.clone(),
                options: col.options.clone(),
            });
        }

        let rows = self.rows.clone(); // shallow clone
        Table {
            name: format!("{}_alias", self.name),
            columns,
            rows,
            primary_key: self.primary_key.clone(),
            indexes: HashMap::new(),
            transaction_backup: None,
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
                .map(|v| v.to_string())
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
            row.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",")
        }).collect();
        let new_rows: Vec<Vec<Value>> = self.rows.iter().filter(|row| {
            let key = row.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
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
            row.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",")
        }).collect();
        let new_rows: Vec<Vec<Value>> = self.rows.iter().filter(|row| {
            let key = row.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
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