use std::collections::HashMap;
use crate::table::data::{Column, Table, Value};

impl Table {
    pub fn inner_join<'a>(
        &'a self,
        other: &'a Table,
        on: (&str, &str),
    ) -> Result<Vec<(Vec<&'a Value>, Vec<Option<&'a Value>>)>, String> {
        let self_idx = self
            .columns
            .iter()
            .position(|c| c.name == on.0)
            .ok_or_else(|| format!("Column '{}' not found in '{}'", on.0, self.name))?;
        let other_idx = other
            .columns
            .iter()
            .position(|c| c.name == on.1)
            .ok_or_else(|| format!("Column '{}' not found in '{}'", on.1, other.name))?;

        let mut result = vec![];

        for left_row in &self.rows {
            let left_val = &left_row[self_idx];
            for right_row in &other.rows {
                if &right_row[other_idx] == left_val {
                    result.push((
                        left_row.iter().collect(),
                        right_row.iter().map(Some).collect(),
                    ));
                }
            }
        }

        Ok(result)
    }

    pub fn left_join<'a>(
        &'a self,
        other: &'a Table,
        on: (&str, &str),
    ) -> Result<Vec<(Vec<&'a Value>, Vec<Option<&'a Value>>)>, String> {
        let self_idx = self
            .columns
            .iter()
            .position(|c| c.name == on.0)
            .ok_or_else(|| format!("Column '{}' not found in '{}'", on.0, self.name))?;
        let other_idx = other
            .columns
            .iter()
            .position(|c| c.name == on.1)
            .ok_or_else(|| format!("Column '{}' not found in '{}'", on.1, other.name))?;

        let mut result = vec![];

        for left_row in &self.rows {
            let left_val = &left_row[self_idx];
            let mut matched = false;

            for right_row in &other.rows {
                if &right_row[other_idx] == left_val {
                    result.push((
                        left_row.iter().collect(),
                        right_row.iter().map(Some).collect(),
                    ));
                    matched = true;
                }
            }

            if !matched {
                result.push((left_row.iter().collect(), vec![None; other.columns.len()]));
            }
        }

        Ok(result)
    }

    pub fn right_join<'a>(
        &'a self,
        other: &'a Table,
        on: (&str, &str),
    ) -> Result<Vec<(Vec<Option<&'a Value>>, Vec<&'a Value>)>, String> {
        let self_idx = self
            .columns
            .iter()
            .position(|c| c.name == on.0)
            .ok_or_else(|| format!("Column '{}' not found in '{}'", on.0, self.name))?;
        let other_idx = other
            .columns
            .iter()
            .position(|c| c.name == on.1)
            .ok_or_else(|| format!("Column '{}' not found in '{}'", on.1, other.name))?;

        let mut result = vec![];

        for right_row in &other.rows {
            let right_val = &right_row[other_idx];
            let mut matched = false;

            for left_row in &self.rows {
                if &left_row[self_idx] == right_val {
                    result.push((
                        left_row.iter().map(Some).collect(),
                        right_row.iter().collect(),
                    ));
                    matched = true;
                }
            }

            if !matched {
                result.push((vec![None; self.columns.len()], right_row.iter().collect()));
            }
        }

        Ok(result)
    }

    pub fn full_outer_join<'a>(
        &'a self,
        other: &'a Table,
        on: (&str, &str),
    ) -> Result<Vec<(Vec<Option<&'a Value>>, Vec<Option<&'a Value>>)>, String> {
        let self_idx = self.columns.iter().position(|c| c.name == on.0)
            .ok_or_else(|| format!("Column '{}' not found in '{}'", on.0, self.name))?;
        let other_idx = other.columns.iter().position(|c| c.name == on.1)
            .ok_or_else(|| format!("Column '{}' not found in '{}'", on.1, other.name))?;

        let mut left_matched = vec![false; self.rows.len()];
        let mut right_matched = vec![false; other.rows.len()];
        let mut results = vec![];

        for (i, left_row) in self.rows.iter().enumerate() {
            let mut match_found = false;
            for (j, right_row) in other.rows.iter().enumerate() {
                if left_row[self_idx] == right_row[other_idx] {
                    results.push((
                        left_row.iter().map(|v| Some(v)).collect(),
                        right_row.iter().map(|v| Some(v)).collect(),
                    ));
                    left_matched[i] = true;
                    right_matched[j] = true;
                    match_found = true;
                }
            }
            if !match_found {
                results.push((
                    left_row.iter().map(|v| Some(v)).collect(),
                    vec![None; other.columns.len()],
                ));
            }
        }

        for (j, right_row) in other.rows.iter().enumerate() {
            if !right_matched[j] {
                results.push((
                    vec![None; self.columns.len()],
                    right_row.iter().map(|v| Some(v)).collect(),
                ));
            }
        }
        Ok(results)
    }

    pub fn select_join_where<'a, F>(
        &'a self,
        other: &'a Table,
        on: (&str, &str),
        filter: F,
    ) -> Result<Vec<(Vec<&'a Value>, Vec<&'a Value>)>, String>
    where
        F: Fn(&[&Value], &[&Value]) -> bool,
    {
        let joined = self.inner_join(other, on)?;

        Ok(joined
            .into_iter()
            .filter_map(|(left, right)| {
                // Convert Option<&Value> to &Value for filtering
                if right.iter().any(|v| v.is_none()) {
                    return None;
                }
                let right_vals: Vec<&Value> = right.into_iter().map(|v| v.unwrap()).collect();
                if filter(&left, &right_vals) {
                    Some((left, right_vals))
                } else {
                    None
                }
            })
            .collect())
    }

    pub fn inner_join_multi<'a>(
        &'a self,
        other: &'a Table,
        on: &[(&str, &str)],
    ) -> Result<Vec<(Vec<&'a Value>, Vec<&'a Value>)>, String> {
        let self_indices: Vec<_> = on
            .iter()
            .map(|(left, _)| {
                self.columns
                    .iter()
                    .position(|c| &c.name == left)
                    .ok_or_else(|| format!("Column '{}' not found in {}", left, self.name))
            })
            .collect::<Result<_, _>>()?;

        let other_indices: Vec<_> = on
            .iter()
            .map(|(_, right)| {
                other
                    .columns
                    .iter()
                    .position(|c| &c.name == right)
                    .ok_or_else(|| format!("Column '{}' not found in {}", right, other.name))
            })
            .collect::<Result<_, _>>()?;

        let mut results = vec![];

        for left_row in &self.rows {
            for right_row in &other.rows {
                let matches = self_indices
                    .iter()
                    .zip(&other_indices)
                    .all(|(&i, &j)| left_row[i] == right_row[j]);

                if matches {
                    results.push((left_row.iter().collect(), right_row.iter().collect()));
                }
            }
        }

        Ok(results)
    }

    pub fn join_to_table(
        name: &str,
        left_columns: &[Column],
        right_columns: &[Column],
        results: Vec<(Vec<&Value>, Vec<&Value>)>,
    ) -> Table {
        let mut columns = vec![];

        for c in left_columns {
            columns.push(Column {
                name: format!("left.{}", c.name),
                datatype: c.datatype.clone(),
                options: vec![],
            });
        }

        for c in right_columns {
            columns.push(Column {
                name: format!("right.{}", c.name),
                datatype: c.datatype.clone(),
                options: vec![],
            });
        }

        let rows = results
            .into_iter()
            .map(|(l, r)| {
                let mut merged = vec![];
                merged.extend(l.into_iter().cloned());
                merged.extend(r.into_iter().cloned());
                merged
            })
            .collect();

        Table {
            name: name.to_string(),
            columns,
            rows,
            primary_key: None,
            indexes: HashMap::new(),
            transaction_backup: None,
        }
    }

    pub fn left_join_multi<'a>(
        &'a self,
        other: &'a Table,
        on: &[(&str, &str)],
    ) -> Result<Vec<(Vec<&'a Value>, Vec<Option<&'a Value>>)>, String> {
        let self_indices = on
            .iter()
            .map(|(l, _)| {
                self.columns
                    .iter()
                    .position(|c| &c.name == l)
                    .ok_or_else(|| format!("Column '{}' not in {}", l, self.name))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let other_indices = on
            .iter()
            .map(|(_, r)| {
                other
                    .columns
                    .iter()
                    .position(|c| &c.name == r)
                    .ok_or_else(|| format!("Column '{}' not in {}", r, other.name))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut results = vec![];

        for left_row in &self.rows {
            let mut matched = false;

            for right_row in &other.rows {
                let is_match = self_indices
                    .iter()
                    .zip(&other_indices)
                    .all(|(&i, &j)| left_row[i] == right_row[j]);

                if is_match {
                    results.push((
                        left_row.iter().collect(),
                        right_row.iter().map(Some).collect(),
                    ));
                    matched = true;
                }
            }

            if !matched {
                results.push((left_row.iter().collect(), vec![None; other.columns.len()]));
            }
        }

        Ok(results)
    }

    pub fn right_join_multi<'a>(
        &'a self,
        other: &'a Table,
        on: &[(&str, &str)],
    ) -> Result<Vec<(Vec<Option<&'a Value>>, Vec<&'a Value>)>, String> {
        let self_indices = on
            .iter()
            .map(|(l, _)| {
                self.columns
                    .iter()
                    .position(|c| &c.name == l)
                    .ok_or_else(|| format!("Column '{}' not in {}", l, self.name))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let other_indices = on
            .iter()
            .map(|(_, r)| {
                other
                    .columns
                    .iter()
                    .position(|c| &c.name == r)
                    .ok_or_else(|| format!("Column '{}' not in {}", r, other.name))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut results = vec![];

        for right_row in &other.rows {
            let mut matched = false;

            for left_row in &self.rows {
                let is_match = self_indices
                    .iter()
                    .zip(&other_indices)
                    .all(|(&i, &j)| left_row[i] == right_row[j]);

                if is_match {
                    results.push((
                        left_row.iter().map(Some).collect(),
                        right_row.iter().collect(),
                    ));
                    matched = true;
                }
            }

            if !matched {
                results.push((vec![None; self.columns.len()], right_row.iter().collect()));
            }
        }

        Ok(results)
    }

    pub fn select_join_where_multi<'a, F>(
        &'a self,
        other: &'a Table,
        on: &[(&str, &str)],
        filter: F,
    ) -> Result<Vec<(Vec<&'a Value>, Vec<&'a Value>)>, String>
    where
        F: Fn(&[&Value], &[&Value]) -> bool,
    {
        let joined = self.inner_join_multi(other, on)?;
        Ok(joined.into_iter().filter(|(l, r)| filter(l, r)).collect())
    }

    pub fn join_to_table_with_aliases(
        name: &str,
        left_table: &Table,
        right_table: &Table,
        left_alias: &str,
        right_alias: &str,
        results: Vec<(Vec<&Value>, Vec<&Value>)>,
    ) -> Table {
        use std::collections::HashSet;

        let mut seen_names = HashSet::new();
        let mut columns = vec![];

        for c in &left_table.columns {
            let mut col_name = format!("{}.{}", left_alias, c.name);
            while seen_names.contains(&col_name) {
                col_name.push('_');
            }
            seen_names.insert(col_name.clone());

            columns.push(Column {
                name: col_name,
                datatype: c.datatype.clone(),
                options: vec![],
            });
        }

        for c in &right_table.columns {
            let mut col_name = format!("{}.{}", right_alias, c.name);
            while seen_names.contains(&col_name) {
                col_name.push('_');
            }
            seen_names.insert(col_name.clone());

            columns.push(Column {
                name: col_name,
                datatype: c.datatype.clone(),
                options: vec![],
            });
        }

        let rows = results
            .into_iter()
            .map(|(l, r)| {
                let mut merged = vec![];
                merged.extend(l.into_iter().cloned());
                merged.extend(r.into_iter().cloned());
                merged
            })
            .collect();

        Table {
            name: name.to_string(),
            columns,
            rows,
            primary_key: None,
            indexes: HashMap::new(),
            transaction_backup: None,
        }
    }

    pub fn merge_tables_with_aliases(
        name: &str,
        left: &Table,
        right: &Table,
        left_alias: &str,
        right_alias: &str,
        results: Vec<(Vec<&Value>, Vec<&Value>)>,
    ) -> Table {
        let aliased_left = left.with_alias(left_alias);
        let aliased_right = right.with_alias(right_alias);

        Table::join_to_table_with_aliases(
            name,
            &aliased_left,
            &aliased_right,
            left_alias,
            right_alias,
            results,
        )
    }
}