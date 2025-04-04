use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use crate::table::data::{FilterExpr, IndexType, Value, Table, AggregationResult};

impl Table {
    pub fn select_all(&self) -> Vec<&Vec<Value>> {
        self.rows.iter().collect()
    }

    pub fn select_where_expr(&self, expr: &FilterExpr) -> Vec<&Vec<Value>> {
        let predicate = expr.to_predicate(self);

        let col = expr.column();
        if let Some(_col_idx) = self.columns.iter().position(|c| &c.name == col) {
            if let Some(index) = self.indexes.get(col.as_str()) {
                match (index, expr) {
                    (IndexType::Hash(map), FilterExpr::Eq(_, val)) => {
                        if let Some(indices) = map.get(val) {
                            return indices
                                .iter()
                                .filter_map(|&i| self.rows.get(i))
                                .filter(|row| predicate(row))
                                .collect();
                        }
                    }
                    (IndexType::BTree(map), FilterExpr::Lt(_, val)) => {
                        return map
                            .range(..val.clone())
                            .flat_map(|(_, idxs)| idxs.iter())
                            .filter_map(|&i| self.rows.get(i))
                            .filter(|row| predicate(row))
                            .collect();
                    }
                    (IndexType::BTree(map), FilterExpr::Gt(_, val)) => {
                        return map
                            .range(val.clone()..)
                            .flat_map(|(_, idxs)| idxs.iter())
                            .filter_map(|&i| self.rows.get(i))
                            .filter(|row| predicate(row))
                            .collect();
                    }
                    _ => {}
                }
            }
        }

        self.rows.iter().filter(|row| predicate(row)).collect()
    }

    pub fn select_order_by(&self, order_cols: &[&str]) -> Result<Vec<&Vec<Value>>, String> {
        let mut indices = Vec::new();
        for &col in order_cols {
            let idx = self.columns.iter().position(|c| c.name == col)
                .ok_or_else(|| format!("Column {} not found", col))?;
            indices.push(idx);
        }
        let mut rows: Vec<&Vec<Value>> = self.rows.iter().collect();
        rows.sort_by(|a, b| {
            for &i in &indices {
                match a[i].cmp(&b[i]) {
                    Ordering::Equal => continue,
                    non_eq => return non_eq,
                }
            }
            Ordering::Equal
        });
        Ok(rows)
    }

    pub fn select_distinct(&self) -> Vec<&Vec<Value>> {
        let mut seen = HashSet::new();
        self.rows.iter().filter(|row| {
            let key = row.iter()
                .map(|v| v.to_display_string())
                .collect::<Vec<_>>()
                .join(",");
            if seen.contains(&key) {
                false
            } else {
                seen.insert(key);
                true
            }
        }).collect()
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

    pub fn aggregate(
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
                        .unwrap_or(Value::Null);
                    AggregationResult::Min(min)
                }
                "max" => {
                    let max = rows
                        .iter()
                        .map(|r| r[agg_idx].clone())
                        .max()
                        .unwrap_or(Value::Null);
                    AggregationResult::Max(max)
                }
                _ => return Err("Unknown aggregation function".into()),
            };

            result.insert(key, agg);
        }

        Ok(result)
    }



    pub fn aggregate_group(
        &self,
        group_col: &str,
        agg_cols: &[(&str, &str)], // (column name, function name)
        filter: Option<&dyn Fn(&Vec<Value>) -> bool>,
    ) -> Result<HashMap<Value, Vec<AggregationResult>>, String> {
        let groups = self.group_by(group_col, filter)?;
        let mut col_indices = vec![];

        for (col_name, _) in agg_cols {
            let idx = self
                .columns
                .iter()
                .position(|c| c.name == *col_name)
                .ok_or_else(|| format!("Column '{}' not found", col_name))?;
            col_indices.push(idx);
        }

        let mut result = HashMap::new();

        for (key, rows) in groups {
            let mut agg_results = vec![];
            for ((_, func), &idx) in agg_cols.iter().zip(&col_indices) {
                let values: Vec<f64> = rows
                    .iter()
                    .filter_map(|row| match &row[idx] {
                        Value::Int(i) => Some(*i as f64),
                        Value::BigInt(i) => Some(*i as f64),
                        Value::Float(f) => Some(*f as f64),
                        Value::Double(f) => Some(*f),
                        _ => None,
                    })
                    .collect();

                let agg = match *func {
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
                            .map(|r| r[idx].clone())
                            .min()
                            .unwrap_or(Value::Null);
                        AggregationResult::Min(min)
                    }
                    "max" => {
                        let max = rows
                            .iter()
                            .map(|r| r[idx].clone())
                            .max()
                            .unwrap_or(Value::Null);
                        AggregationResult::Max(max)
                    }
                    _ => return Err("Unknown aggregation function".into()),
                };

                agg_results.push(agg);
            }

            result.insert(key, agg_results);
        }

        Ok(result)
    }
}