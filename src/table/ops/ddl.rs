use crate::table::model::{Column, Table};
use std::collections::HashMap;

impl Table {

    // redo this to be like select with all parameters as optional for like where, order by, limit/offset, aggregations, etc
    // note: this db will not excute deletes or updates unless a where is given, but users can still nuke a db if they forget to add one, so auto place with where 1 = 1
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

    pub fn alter_column_datatype() { unimplemented!() }

    pub fn alter_add_constraint() { unimplemented!() }

    pub fn alter_remove_constraint() { unimplemented!() }

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

    pub fn alter_rename_table() { unimplemented!() }

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

    pub fn create_temp_table() { unimplemented!() }

    pub fn drop_temp_table() { unimplemented!() }
}