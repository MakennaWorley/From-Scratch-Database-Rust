use crate::table::data::{Column, Value, Table};
use crate::table::filters::FilterExpr;
use std::collections::HashMap;

impl Table {
    pub fn new(name: &str, columns: Vec<Column>, pk: Option<Vec<String>>) -> Self {
        let mut table = Table {
            name: name.to_string(),
            columns,
            rows: Vec::new(),
            primary_key: pk.clone(),
            indexes: HashMap::new(),
            transaction_backup: None,
        };

        if let Some(pk_cols) = &pk {
            for col in pk_cols {
                let _ = table.create_index(col, false);
            }
        }

        table
    }

    pub fn insert(&mut self, values: Vec<Value>) -> Result<(), String> {
        if values.len() != self.columns.len() {
            return Err("Column count does not match".to_string());
        }

        for (i, value) in values.iter().enumerate() {
            let col_type = &self.columns[i].datatype;
            if !Table::value_matches_type(value, col_type) {
                return Err(format!(
                    "Type mismatch at column {}: expected {:?}, got {:?}",
                    self.columns[i].name, col_type, value
                ));
            }
        }

        let full_row = self.apply_defaults(&values)?;
        self.validate_row(&full_row)?;

        self.rows.push(full_row);
        let i = self.rows.len() - 1;
        self.update_indexes_for_row(i);

        Ok(())
    }

    pub fn update_where(
        &mut self,
        expr: &FilterExpr,
        updates: Vec<Option<Value>>,
    ) -> Result<(), String> {
        let predicate = expr.to_predicate(self);
        let mut updated_rows = vec![];
        let mut indices = vec![];

        if let Some(index) = self.indexes.get(expr.column().as_str()) {
            if let Some(v) = expr.value() {
                if let Some(row_indices) = index.get(v) {
                    for &i in row_indices {
                        if predicate(&self.rows[i]) {
                            let mut new_row = self.rows[i].clone();
                            for (j, update) in updates.iter().enumerate() {
                                if let Some(val) = update {
                                    new_row[j] = val.clone();
                                }
                            }
                            self.validate_row(&new_row)?;
                            updated_rows.push(new_row);
                            indices.push(i);
                        }
                    }
                }
            }
        }

        for (&i, new_row) in indices.iter().zip(updated_rows.into_iter()) {
            self.rows[i] = new_row;
            self.update_indexes_for_row(i);
        }

        Ok(())
    }

    pub fn delete_where(&mut self, expr: &FilterExpr) {
        let predicate = expr.to_predicate(self);

        if let Some(index) = self.indexes.get(expr.column().as_str()) {
            if let Some(v) = expr.value() {
                if let Some(row_indices) = index.get(v) {
                    let to_remove: HashMap<usize, ()> = row_indices
                        .iter()
                        .filter(|&&i| predicate(&self.rows[i]))
                        .map(|&i| (i, ()))
                        .collect();

                    self.rows = self
                        .rows
                        .iter()
                        .enumerate()
                        .filter_map(|(i, row)| {
                            if to_remove.contains_key(&i) {
                                None
                            } else {
                                Some(row.clone())
                            }
                        })
                        .collect();

                    self.rebuild_all_indexes();
                }
            }
        }
    }

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
}
