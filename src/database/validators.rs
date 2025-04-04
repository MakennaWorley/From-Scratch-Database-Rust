use std::collections::HashMap;
use crate::table::data::{Table, Options, Column};

#[derive(Debug)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, table: Table) -> Result<(), String> {
        if self.tables.contains_key(&table.name) {
            return Err(format!("Table '{}' already exists", table.name));
        }
        self.tables.insert(table.name.clone(), table);
        Ok(())
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<(), String> {
        if self.tables.remove(table_name).is_some() {
            Ok(())
        } else {
            Err(format!("Table '{}' does not exist", table_name))
        }
    }

    pub fn alter_add_column(&mut self, table_name: &str, new_column: Column) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        table.alter_add_column(new_column)
    }

    pub fn rename_column(
        &mut self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        table.rename_column(old_name, new_name)
    }

    pub fn drop_column(&mut self, table_name: &str, col_name: &str) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        table.drop_column(col_name)
    }

    pub fn validate_foreign_keys(&self) -> Result<(), String> {
        for table in self.tables.values() {
            for column in &table.columns {
                for opt in &column.options {
                    if let Options::FK(ref foreign_table_name) = opt {
                        if !self.tables.contains_key(foreign_table_name) {
                            return Err(format!(
                                "Table '{}' has a foreign key to missing table '{}'.",
                                table.name, foreign_table_name
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}