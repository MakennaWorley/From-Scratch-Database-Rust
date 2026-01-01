use crate::table::model::{Table, Value};

impl Table {
    // redo this to be like select with all parameters as optional for like where, order by, limit/offset, aggregations, etc
    // note: this db will not excute deletes or updates unless a where is given, but users can still nuke a db if they forget to add one, so auto place with where 1 = 1
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

    pub fn cross_join() { unimplemented!() }
}