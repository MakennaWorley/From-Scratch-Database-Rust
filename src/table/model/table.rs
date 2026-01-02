use crate::table::model::{Column, IndexType, Value};
use std::collections::{BTreeMap, HashMap};

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

    pub fn col_index(&self, name: &str) -> Result<usize, String> {
        self.columns
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| format!("Column '{}' not found in table '{}'", name, self.name))
    }

    pub fn rebuild_index_for_column(&mut self, col_name: &str) -> Result<(), String> {
        let col_idx = self.col_index(col_name)?;

        let Some(existing) = self.indexes.get(col_name).cloned() else {
            return Ok(());
        };

        let mut positions: Vec<(Value, usize)> = Vec::with_capacity(self.rows.len());
        for (row_i, row) in self.rows.iter().enumerate() {
            let key = row
                .get(col_idx)
                .cloned()
                .ok_or_else(|| format!("Row {} missing value for column '{}'", row_i, col_name))?;
            positions.push((key, row_i));
        }

        let rebuilt = match existing {
            IndexType::Hash(_) => {
                let mut map: HashMap<Value, Vec<usize>> = HashMap::new();
                for (k, row_i) in positions {
                    map.entry(k).or_default().push(row_i);
                }
                IndexType::Hash(map)
            }
            IndexType::BTree(_) => {
                let mut map: BTreeMap<Value, Vec<usize>> = BTreeMap::new();
                for (k, row_i) in positions {
                    map.entry(k).or_default().push(row_i);
                }
                IndexType::BTree(map)
            }
        };

        self.indexes.insert(col_name.to_string(), rebuilt);
        Ok(())
    }
}