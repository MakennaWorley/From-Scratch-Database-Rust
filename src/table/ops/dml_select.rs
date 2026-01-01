use crate::table::model::{FilterExpr, IndexType, Table, Value};
use std::collections::HashSet;
use std::cmp::Ordering;

impl Table {
    // redo this to be like select with all parameters as optional for like where, order by, limit/offset, aggregations, etc
    // note: this db will not excute deletes or updates unless a where is given, but users can still nuke a db if they forget to add one, so auto place with where 1 = 1
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

    pub fn select_limit_by() {
        unimplemented!()
    }

    pub fn select_offset_by() {
        unimplemented!()
    }

    pub fn select_distinct(&self) -> Vec<&Vec<Value>> {
        let mut seen = HashSet::new();
        self.rows.iter().filter(|row| {
            let key = row.iter()
                .map(|v| v.to_string())
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
}