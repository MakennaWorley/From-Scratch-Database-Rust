use crate::table::model::{AggregationResult, FilterExpr, Table, Value};
use std::collections::HashMap;

impl Table {
    // redo this to be like select with all parameters as optional for like where, order by, limit/offset, aggregations, etc
    // note: this db will not excute deletes or updates unless a where is given, but users can still nuke a db if they forget to add one, so auto place with where 1 = 1
    pub fn insert(&mut self, values: Vec<Value>) -> Result<(), String> {
        if values.len() != self.columns.len() {
            return Err("Column count does not match".to_string());
        }

        for (i, value) in values.iter().enumerate() {
            let col_type = &self.columns[i].datatype;
            if !Value::value_matches_type(value, col_type) {
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

    pub fn insert_select() {
        unimplemented!()
    }

    pub fn update_where(
        &mut self,
        expr: &FilterExpr,
        updates: Vec<Option<Value>>,
    ) -> Result<(), String> {
        let predicate = expr.to_predicate(self);
        let mut indices = vec![];

        if let Some(index) = self.indexes.get(expr.column().as_str()) {
            if let Some(v) = expr.value() {
                if let Some(row_indices) = index.get(v) {
                    for &i in row_indices {
                        if predicate(&self.rows[i]) {
                            indices.push(i);
                        }
                    }
                }
            }
        } else {
            // Fallback: scan all rows if no index exists.
            for (i, row) in self.rows.iter().enumerate() {
                if predicate(row) {
                    indices.push(i);
                }
            }
        }

        // Update rows at the collected indices.
        for &i in &indices {
            let mut new_row = self.rows[i].clone();
            for (j, update) in updates.iter().enumerate() {
                if let Some(val) = update {
                    new_row[j] = val.clone();
                }
            }
            self.validate_row(&new_row)?;
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
                    let to_remove: std::collections::HashMap<usize, ()> = row_indices
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
        } else {
            // Fallback: remove rows by scanning all rows.
            self.rows.retain(|row| !predicate(row));
            self.rebuild_all_indexes();
        }
    }

    pub fn merge() {
        unimplemented!()
    }

    pub fn upsert() {
        unimplemented!()
    }








    pub fn group_by(
        &self,
        by_col: &str,
        filter: Option<&dyn Fn(&Vec<Value>) -> bool>,
    ) -> Result<HashMap<Value, Vec<&Vec<Value>>>, String> {
        let col_idx = self
            .columns
            .iter()
            .position(|c| c.name == by_col)
            .ok_or_else(|| format!("Column '{}' not found", by_col))?;

        let mut groups: HashMap<Value, Vec<&Vec<Value>>> = HashMap::new();
        for row in &self.rows {
            if let Some(f) = filter {
                if !f(row) {
                    continue;
                }
            }
            let key = row[col_idx].clone();
            groups.entry(key).or_default().push(row);
        }

        Ok(groups)
    }

    pub fn having() {
        unimplemented!()
    }

    pub fn aggregate_by(
        &self,
        group_col: &str,
        agg_col: &str,
        func: &str,
    ) -> Result<HashMap<Value, AggregationResult>, String> {
        let groups = self.group_by(group_col, None)?;
        let agg_idx = self
            .columns
            .iter()
            .position(|c| c.name == agg_col)
            .ok_or_else(|| format!("Column '{}' not found", agg_col))?;

        let mut result = HashMap::new();

        for (key, rows) in groups {
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|row| match &row[agg_idx] {
                    Value::Int(i) => Some(*i as f64),
                    Value::BigInt(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f as f64),
                    Value::Double(f) => Some(*f),
                    _ => None,
                })
                .collect();

            let agg = match func {
                "sum" => AggregationResult::Sum(values.iter().sum()),
                "avg" => {
                    let total: f64 = values.iter().sum();
                    let count = values.len();
                    AggregationResult::Avg(if count == 0 {
                        0.0
                    } else {
                        total / count as f64
                    })
                }
                "count" => AggregationResult::Count(rows.len()),
                "min" => {
                    let min = rows
                        .iter()
                        .map(|r| r[agg_idx].clone())
                        .min()
                        .unwrap_or(Value::NULL);
                    AggregationResult::Min(min)
                }
                "max" => {
                    let max = rows
                        .iter()
                        .map(|r| r[agg_idx].clone())
                        .max()
                        .unwrap_or(Value::NULL);
                    AggregationResult::Max(max)
                }
                _ => return Err("Unknown aggregation function".into()),
            };

            result.insert(key, agg);
        }

        Ok(result)
    }

    pub fn arithmetic_expr() { unimplemented!() } // +, *, /. -

    pub fn logical_expr() { unimplemented!() } // AND, NAND, OR, NOR, XOR, NOT, IS NULL, IN

    pub fn comparison_expr() { unimplemented!() } // =, !=, <, <=, >, >=, BETWEEN, LIKE

    pub fn case_expr() { unimplemented!() } // WHEN ___ THEN ____ END
}