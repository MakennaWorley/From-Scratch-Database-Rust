use crate::table::data::{Table, Column, Value, DataType};
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write, BufReader, BufRead, Read};
use std::path::Path;
use csv::ReaderBuilder;

impl Table {
    pub fn new(name: &str, columns: Vec<Column>, pk: Option<Vec<String>>) -> Self {
        Table {
            name: name.to_string(),
            columns,
            rows: Vec::new(),
            primary_key: pk,
        }
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
        Ok(())
    }

    pub fn select_all(&self) -> Vec<&Vec<Value>> {
        self.rows.iter().collect()
    }

    pub fn select_where<F>(&self, predicate: F) -> Vec<&Vec<Value>>
    where
        F: Fn(&Vec<Value>) -> bool,
    {
        self.rows.iter().filter(|row| predicate(row)).collect()
    }

    pub fn update_where<F>(&mut self, predicate: F, updates: Vec<Option<Value>>) -> Result<(), String>
    where
        F: Fn(&Vec<Value>) -> bool,
    {
        let mut updated_rows = vec![];

        for row in self.rows.iter().filter(|row| predicate(row)) {
            let mut new_row = row.clone();
            for (i, update) in updates.iter().enumerate() {
                if let Some(val) = update {
                    new_row[i] = val.clone();
                }
            }
            self.validate_row(&new_row)?;
            updated_rows.push((row.clone(), new_row));
        }

        for (old_row, new_row) in updated_rows {
            if let Some(existing_row) = self.rows.iter_mut().find(|r| **r == old_row) {
                *existing_row = new_row;
            }
        }

        Ok(())
    }

    pub fn delete_where<F>(&mut self, predicate: F)
    where
        F: Fn(&Vec<Value>) -> bool,
    {
        self.rows.retain(|row| !predicate(row));
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
            fs::create_dir_all(dir_path).map_err(|e| format!("Failed to create db directory: {}", e))?;
        }

        let file_path = dir_path.join(format!("{}.{}.csv", db_name, self.name));

        let file = File::create(&file_path).map_err(|e| format!("Failed to create file: {}", e))?;
        let mut writer = BufWriter::new(file);

        // Write header
        let header = self.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>().join(",");
        writeln!(writer, "{}", header).map_err(|e| e.to_string())?;

        // Write rows
        for row in &self.rows {
            let line = row.iter().map(|v| match v {
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
            }).collect::<Vec<_>>().join(",");
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

        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);

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
                    format!("Error parsing value '{}' for column '{}': {}", raw, col.name, e)
                })?;
                row.push(value);
            }

            rows.push(row);
        }

        Ok(Table {
            name: name.to_string(),
            columns,
            rows,
            primary_key,
        })
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
}