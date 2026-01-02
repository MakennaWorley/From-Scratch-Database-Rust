use crate::table::model::{Column, DataType, Options, Table, Value, View};
use crate::table::validators::row;
use std::collections::HashMap;

impl Table {
    pub fn create_table(name: &str, columns: Vec<Column>, pk: Option<Vec<String>>) -> Self {
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

        // logic for fks is in the database not table

        table
    }

    pub fn alter_add_column(&mut self, new_column: Column) -> Result<(), String> {
        if self.columns.iter().any(|col| col.name == new_column.name) {
            return Err(format!(
                "Column '{}' already exists in table '{}'",
                new_column.name, self.name
            ));
        }

        new_column.validate()?;

        let default = Self::default_for_new_column(&new_column)?;

        for row in &mut self.rows {
            row.push(default.clone());
        }

        self.columns.push(new_column);
        Ok(())
    }

    pub fn alter_drop_column(&mut self, name: &str) -> Result<(), String> {
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

    pub fn alter_column_datatype(&mut self, col_name: &str, new_type: DataType) -> Result<(), String> {
        let idx = self.col_index(col_name)?;

        let mut converted: Vec<Value> = Vec::with_capacity(self.rows.len());
        for (row_i, row) in self.rows.iter().enumerate() {
            let old_v = row
                .get(idx)
                .cloned()
                .ok_or_else(|| format!("Row {} missing value for column '{}'", row_i, col_name))?;

            let new_v = Value::cast_value(old_v, &new_type)
                .map_err(|e| format!("Cannot convert row {} column '{}': {}", row_i, col_name, e))?;

            converted.push(new_v);
        }

        for (row, new_v) in self.rows.iter_mut().zip(converted.into_iter()) {
            row[idx] = new_v;
        }

        self.columns[idx].datatype = new_type;

        self.rebuild_index_for_column(col_name)?;

        Ok(())
    }

    pub fn alter_add_constraint(
        &mut self,
        col_name: &str,
        constraint: Options,
    ) -> Result<(), String> {
        let idx = self.col_index(col_name)?;

        match &constraint {
            Options::NotNull => {
                for (row_i, row) in self.rows.iter().enumerate() {
                    if let Some(v) = row.get(idx) {
                        if Value::is_null(v) {
                            return Err(format!(
                                "Cannot add NOT NULL: row {} has NULL in column '{}'",
                                row_i, col_name
                            ));
                        }
                    }
                }

                if !Column::has_option(&self.columns[idx], |o| matches!(o, Options::NotNull)) {
                    self.columns[idx].options.push(Options::NotNull);
                }

                Ok(())
            }

            Options::Unique => {
                row::check_unique_no_dupes(&self.rows, idx, col_name)?;

                if !Column::has_option(&self.columns[idx], |o| matches!(o, Options::Unique)) {
                    self.columns[idx].options.push(Options::Unique);
                }

                if !self.indexes.contains_key(col_name) {
                    let _ = self.create_index(col_name, true);
                }

                Ok(())
            }

            Options::Default(_v) => {
                Column::remove_options_matching(&mut self.columns[idx], |o| matches!(o, Options::Default(_)));

                self.columns[idx].options.push(constraint);
                Ok(())
            }

            Options::Check(_expr) => {
                self.columns[idx].options.push(constraint);
                Ok(())
            }

            Options::PrimaryKey => Err("Use table-level primary_key, not column option".to_string()),
            Options::ForeignKey(_) => Err("Foreign keys are handled in Database, not Table".to_string()),

            Options::AutoIncrement => {
                self.columns[idx].options.push(constraint);
                Ok(())
            }

            Options::OnDelete | Options::OnUpdate => {
                Err("ON DELETE/ON UPDATE are FK actions; handled in Database".to_string())
            }
        }
    }

    pub fn alter_remove_constraint(
        &mut self,
        col_name: &str,
        constraint: Options,
    ) -> Result<(), String> {
        let idx = self.col_index(col_name)?;

        match constraint {
            Options::NotNull => {
                Column::remove_options_matching(&mut self.columns[idx], |o| matches!(o, Options::NotNull));
                Ok(())
            }
            Options::Unique => {
                Column::remove_options_matching(&mut self.columns[idx], |o| matches!(o, Options::Unique));
                self.indexes.remove(col_name);
                Ok(())
            }
            Options::Default(_) => {
                Column::remove_options_matching(&mut self.columns[idx], |o| matches!(o, Options::Default(_)));
                Ok(())
            }
            Options::Check(_) => {
                Column::remove_options_matching(&mut self.columns[idx], |o| matches!(o, Options::Check(_)));
                Ok(())
            }
            Options::AutoIncrement => {
                Column::remove_options_matching(&mut self.columns[idx], |o| matches!(o, Options::AutoIncrement));
                Ok(())
            }
            Options::PrimaryKey => Err("Primary key is table-level; remove via table.primary_key".to_string()),
            Options::ForeignKey(_) => Err("Foreign keys are handled in Database, not Table".to_string()),
            Options::OnDelete | Options::OnUpdate => {
                Err("ON DELETE/ON UPDATE are FK actions; handled in Database".to_string())
            }
        }
    }


    pub fn alter_rename_column(&mut self, old_name: &str, new_name: &str) -> Result<(), String> {
        if self.columns.iter().any(|c| c.name == new_name) {
            return Err(format!("Column '{}' already exists", new_name));
        }

        let idx = self
            .columns
            .iter()
            .position(|c| c.name == old_name)
            .ok_or_else(|| format!("Column '{}' not found", old_name))?;

        self.columns[idx].name = new_name.to_string();

        if let Some(index) = self.indexes.remove(old_name) {
            self.indexes.insert(new_name.to_string(), index);
        }

        if let Some(pk) = &mut self.primary_key {
            for key in pk.iter_mut() {
                if key == old_name {
                    *key = new_name.to_string();
                }
            }
        }

        Ok(())
    }

    pub fn alter_rename_table(&mut self, new_name: &str) -> Result<(), String> {
        if new_name.trim().is_empty() {
            return Err("New table name cannot be empty".to_string());
        }
        self.name = new_name.to_string();
        Ok(())
    }

    pub fn drop_table(&mut self) {
        self.rows.clear();
        self.indexes.clear();
    }

    // should somehow save to new place outside of database
    pub fn truncate_table(&mut self) {
        self.rows.clear();
        self.indexes.clear();
    }

    // Because Views are just tables, they are treated like queries that are saved but the outputs written to file
    pub fn create_view() { unimplemented!() }

    pub fn drop_view() { unimplemented!() }

    // these are run per session, temp tables do NOT persist between db restarts
    pub fn create_temp_table() { unimplemented!() }

    pub fn drop_temp_table() { unimplemented!() }
}