use crate::table::data::{Column, Table, Value};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use csv::ReaderBuilder;

impl Table {
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

    pub fn save_join_table_to_file(
        db_name: &str,
        view_name: &str,
        join_table: &Table,
    ) -> Result<(), String> {
        join_table.save_as_view(db_name, view_name)
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