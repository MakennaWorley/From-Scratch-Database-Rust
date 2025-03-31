use crate::table::data::{AggregationResult, Column, DataType, IndexType, Options, Table, Value};
use crate::table::filters::FilterExpr;
use csv::ReaderBuilder;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

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

        // Basic type check (can expand to enforce options)
        for (i, value) in values.iter().enumerate() {
            let col_type = &self.columns[i].datatype;
            if !Self::value_matches_type(value, col_type) {
                return Err(format!(
                    "Type mismatch at column {}: expected {:?}, got {:?}",
                    self.columns[i].name, col_type, value
                ));
            }
        }

        // Apply defaults
        let full_row = self.apply_defaults(&values)?;

        // Validate the fully constructed row
        self.validate_row(&full_row)?;

        self.rows.push(full_row);
        let i = self.rows.len() - 1;
        self.update_indexes_for_row(i);

        Ok(())
    }

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

    pub fn update_where(
        &mut self,
        expr: &FilterExpr,
        updates: Vec<Option<Value>>,
    ) -> Result<(), String> {
        let predicate = expr.to_predicate(self);
        let _col_index = self
            .columns
            .iter()
            .position(|c| c.name.as_str() == expr.column().as_str())
            .ok_or_else(|| format!("Column '{}' not found", expr.column()))?;

        let mut updated_rows = vec![];
        let mut indices = vec![];

        if let Some(index) = self.indexes.get(expr.column().as_str()) {
            if let Some(row_indices) = index.get(&expr.value()) {
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

        for (&i, new_row) in indices.iter().zip(updated_rows.into_iter()) {
            self.rows[i] = new_row;
            self.update_indexes_for_row(i);
        }

        Ok(())
    }

    pub fn delete_where(&mut self, expr: &FilterExpr) {
        let predicate = expr.to_predicate(self);
        let _col_index = self
            .columns
            .iter()
            .position(|c| c.name.as_str() == expr.column().as_str())
            .unwrap();

        if let Some(index) = self.indexes.get(expr.column().as_str()) {
            if let Some(row_indices) = index.get(&expr.value()) {
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

                self.rebuild_all_indexes(); // simple for now
            }
        }
    }

    pub fn print_table(&self) {
        println!("\nTable: {}", self.name);
        for col in &self.columns {
            print!("| {:<15} ", col.name);
        }
        println!("|");

        for row in &self.rows {
            for val in row {
                print!("| {:<15} ", val.to_display_string());
            }
            println!("|");
        }
    }

    pub fn save_to_file(&self, db_name: &str) -> Result<(), String> {
        let dir_path = Path::new("db");
        if !dir_path.exists() {
            fs::create_dir_all(dir_path)
                .map_err(|e| format!("Failed to create db directory: {}", e))?;
        }

        let file_path = dir_path.join(format!("{}.{}.csv", db_name, self.name));

        let file = File::create(&file_path).map_err(|e| format!("Failed to create file: {}", e))?;
        let mut writer = BufWriter::new(file);

        // Write header
        let header = self
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
            .join(",");
        writeln!(writer, "{}", header).map_err(|e| e.to_string())?;

        // Write rows
        for row in &self.rows {
            let line = row
                .iter()
                .map(|v| match v {
                    Value::Set(items, _) => {
                        let inner = items.join(",");
                        format!("\"{{{}}}\"", inner)
                    }
                    Value::Enum(val, _) => format!("\"{}\"", val),
                    Value::Varchar(s) | Value::Text(s) => format!("\"{}\"", s),
                    Value::Char(c) => format!("\"{}\"", c),
                    Value::Boolean(b) => format!("\"{}\"", b),
                    Value::Int(i) => format!("\"{}\"", i),
                    Value::BigInt(i) => format!("\"{}\"", i),
                    Value::Float(f) => format!("\"{}\"", f),
                    Value::Double(f) => format!("\"{}\"", f),
                    Value::Date(d) => format!("\"{}\"", d),
                    Value::Time(t) => format!("\"{}\"", t),
                    Value::DateTime(dt) => format!("\"{}\"", dt),
                    Value::Null => "\"NULL\"".to_string(),
                })
                .collect::<Vec<_>>()
                .join(",");
            writeln!(writer, "{}", line).map_err(|e| e.to_string())?;
        }

        Ok(())
    }

    pub fn load_from_file(
        dir: &str,
        name: &str,
        columns: Vec<Column>,
        primary_key: Option<Vec<String>>,
    ) -> Result<Self, String> {
        let file_path = format!("db/{}.{}.csv", dir, name);
        let file = File::open(&file_path).map_err(|e| format!("Failed to open file: {}", e))?;

        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

        let mut rows = Vec::new();

        for (line_num, result) in rdr.records().enumerate() {
            let record = result.map_err(|e| format!("CSV parse error: {}", e))?;

            if record.len() != columns.len() {
                return Err(format!(
                    "Row {} has wrong number of fields: expected {}, got {}",
                    line_num + 1,
                    columns.len(),
                    record.len()
                ));
            }

            let mut row = Vec::new();
            for (i, col) in columns.iter().enumerate() {
                let raw = &record[i];
                let value = Value::from_str(raw, &col.datatype).map_err(|e| {
                    format!(
                        "Error parsing value '{}' for column '{}': {}",
                        raw, col.name, e
                    )
                })?;
                row.push(value);
            }

            rows.push(row);
        }

        let mut table = Table::new(name, columns.clone(), primary_key.clone());
        table.rows = rows;
        let column_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
        for col in column_names {
            let _ = table.create_index(&col, false);
        }

        Ok(table)
    }

    fn value_matches_type(val: &Value, dtype: &DataType) -> bool {
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
    pub fn alter_add_column(&mut self, new_column: Column) -> Result<(), String> {
        if self.columns.iter().any(|col| col.name == new_column.name) {
            return Err(format!(
                "Column '{}' already exists in table '{}'",
                new_column.name, self.name
            ));
        }

        new_column.validate()?;

        let default_val = new_column.options.iter().find_map(|opt| {
            if let Options::Default(ref val) = opt {
                Some(val.clone())
            } else {
                None
            }
        });

        let default = if new_column.options.contains(&Options::NotNull) {
            default_val.ok_or_else(|| {
                format!(
                    "Cannot add NOT NULL column '{}' without a default value",
                    new_column.name
                )
            })?
        } else {
            default_val.unwrap_or(Value::Null)
        };

        for row in &mut self.rows {
            row.push(default.clone());
        }

        self.columns.push(new_column);

        Ok(())
    }

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

    fn update_indexes_for_row(&mut self, row_idx: usize) {
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

    fn rebuild_all_indexes(&mut self) {
        let column_names: Vec<String> = self.indexes.keys().cloned().collect();
        self.indexes.clear();
        for name in column_names {
            let _ = self.create_index(&name, false);
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

    pub fn print_join_results(
        left_headers: &[String],
        right_headers: &[String],
        results: &[(Vec<&Value>, Vec<&Value>)],
    ) {
        let total_headers = left_headers
            .iter()
            .chain(right_headers.iter())
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        println!("{}", total_headers.join(" | "));

        for (left, right) in results {
            let row = left
                .iter()
                .chain(right.iter())
                .map(|v| v.to_display_string())
                .collect::<Vec<_>>();
            println!("{}", row.join(" | "));
        }
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

    pub fn save_join_table_to_file(
        db_name: &str,
        view_name: &str,
        join_table: &Table,
    ) -> Result<(), String> {
        join_table.save_as_view(db_name, view_name)
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

    pub fn save_join_table_to_file_with_aliases(
        db_name: &str,
        left_alias: &str,
        right_alias: &str,
        view_name: &str,
        join_table: &Table,
    ) -> Result<(), String> {
        let view_name_combined = format!("{}.{}.{}", left_alias, right_alias, view_name);
        join_table.save_as_view(db_name, &view_name_combined)
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

    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<(), String> {
        if self.columns.iter().any(|c| c.name == new_name) {
            return Err(format!("Column '{}' already exists", new_name));
        }

        let idx = self
            .columns
            .iter()
            .position(|c| c.name == old_name)
            .ok_or_else(|| format!("Column '{}' not found", old_name))?;

        self.columns[idx].name = new_name.to_string();

        // Update index if present
        if let Some(index) = self.indexes.remove(old_name) {
            self.indexes.insert(new_name.to_string(), index);
        }

        // Update primary key name if needed
        if let Some(pk) = &mut self.primary_key {
            for key in pk.iter_mut() {
                if key == old_name {
                    *key = new_name.to_string();
                }
            }
        }

        Ok(())
    }

    pub fn drop_column(&mut self, name: &str) -> Result<(), String> {
        let idx = self
            .columns
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| format!("Column '{}' not found", name))?;

        // Disallow dropping primary key columns
        if let Some(pk) = &self.primary_key {
            if pk.contains(&name.to_string()) {
                return Err(format!("Cannot drop primary key column '{}'", name));
            }
        }

        self.columns.remove(idx);
        for row in &mut self.rows {
            row.remove(idx);
        }

        self.indexes.remove(name);

        Ok(())
    }

    pub fn begin_transaction(&mut self) -> Result<(), String> {
        if self.transaction_backup.is_some() {
            return Err("Transaction already in progress".into());
        }
        self.transaction_backup = Some(self.rows.clone());
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> Result<(), String> {
        if let Some(backup) = self.transaction_backup.take() {
            self.rows = backup;
            self.rebuild_all_indexes(); // restore consistency
            Ok(())
        } else {
            Err("No transaction to rollback".into())
        }
    }

    pub fn commit_transaction(&mut self) -> Result<(), String> {
        if self.transaction_backup.is_some() {
            self.transaction_backup = None;
            Ok(())
        } else {
            Err("No transaction to commit".into())
        }
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

    pub fn save_as_view(&self, db_name: &str, view_name: &str) -> Result<(), String> {
        let dir_path = Path::new("db");
        if !dir_path.exists() {
            fs::create_dir_all(dir_path)
                .map_err(|e| format!("Failed to create db directory: {}", e))?;
        }

        let file_path = dir_path.join(format!("{}.{}.view.csv", db_name, view_name));

        let file = File::create(&file_path).map_err(|e| format!("Failed to create file: {}", e))?;
        let mut writer = BufWriter::new(file);

        // Write headers
        let header = self
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
            .join(",");
        writeln!(writer, "{}", header).map_err(|e| e.to_string())?;

        // Write rows
        for row in &self.rows {
            let line = row
                .iter()
                .map(|v| v.to_display_string())
                .collect::<Vec<_>>()
                .join(",");
            writeln!(writer, "{}", line).map_err(|e| e.to_string())?;
        }

        Ok(())
    }

    pub fn load_view_from_file(
        db_name: &str,
        view_name: &str,
        columns: Vec<Column>,
    ) -> Result<Self, String> {
        let file_path = format!("db/{}.{}.view.csv", db_name, view_name);
        let file =
            File::open(&file_path).map_err(|e| format!("Failed to open view file: {}", e))?;

        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

        let mut rows = Vec::new();

        for (line_num, result) in rdr.records().enumerate() {
            let record = result.map_err(|e| format!("CSV parse error: {}", e))?;

            if record.len() != columns.len() {
                return Err(format!(
                    "Row {} has wrong number of fields: expected {}, got {}",
                    line_num + 1,
                    columns.len(),
                    record.len()
                ));
            }

            let mut row = Vec::new();
            for (i, col) in columns.iter().enumerate() {
                let raw = &record[i];
                let value = Value::from_str(raw, &col.datatype).map_err(|e| {
                    format!(
                        "Error parsing value '{}' for column '{}': {}",
                        raw, col.name, e
                    )
                })?;
                row.push(value);
            }

            rows.push(row);
        }

        Ok(Table {
            name: view_name.to_string(),
            columns,
            rows,
            primary_key: None,
            indexes: HashMap::new(),
            transaction_backup: None,
        })
    }
}
