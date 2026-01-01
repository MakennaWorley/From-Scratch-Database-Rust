use std::collections::{BTreeMap, HashMap};
use crate::table::model::{Table, Value, IndexType};

impl Table {
    // redo this to be like select with all parameters as optional for like where, order by, limit/offset, aggregations, etc
    // note: this db will not excute deletes or updates unless a where is given, but users can still nuke a db if they forget to add one, so auto place with where 1 = 1
    pub fn create_index(&mut self, column_name: &str, use_btree: bool) -> Result<(), String> {
        let col_index = self
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| format!("Column '{}' does not exist", column_name))?;

        if use_btree {
            let mut index_map: BTreeMap<Value, Vec<usize>> = BTreeMap::new();
            for (i, row) in self.rows.iter().enumerate() {
                let key = row[col_index].clone();
                index_map.entry(key).or_default().push(i);
            }
            self.indexes
                .insert(column_name.to_string(), IndexType::BTree(index_map));
        } else {
            let mut index_map: HashMap<Value, Vec<usize>> = HashMap::new();
            for (i, row) in self.rows.iter().enumerate() {
                let key = row[col_index].clone();
                index_map.entry(key).or_default().push(i);
            }
            self.indexes
                .insert(column_name.to_string(), IndexType::Hash(index_map));
        }

        Ok(())
    }

    pub fn update_indexes_for_row(&mut self, row_idx: usize) {
        for (col_name, index_map) in &mut self.indexes {
            if let Some(col_idx) = self.columns.iter().position(|c| &c.name == col_name) {
                let value = self.rows[row_idx][col_idx].clone();
                match index_map {
                    IndexType::Hash(map) => map.entry(value).or_default().push(row_idx),
                    IndexType::BTree(map) => map.entry(value).or_default().push(row_idx),
                }
            }
        }
    }

    pub fn rebuild_all_indexes(&mut self) {
        let column_names: Vec<String> = self.indexes.keys().cloned().collect();
        self.indexes.clear();
        for name in column_names {
            let _ = self.create_index(&name, false);
        }
    }

    pub fn drop_indexes() { unimplemented!() }
}