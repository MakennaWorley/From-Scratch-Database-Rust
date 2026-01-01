use crate::table::model::{Table, Options};
use std::collections::HashMap;

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

    pub fn create(&mut self, table: Table) -> Result<(), String> {
        if self.tables.contains_key(&table.name) {
            Err(format!("Table '{}' already exists", table.name))
        } else {
            self.tables.insert(table.name.clone(), table);
            Ok(())
        }
    }

    fn get_table_mut(&mut self, table_name: &str) -> Result<&mut Table, String> {
        self.tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))
    }

    pub fn validate_foreign_keys(&self) -> Result<(), String> {
        for table in self.tables.values() {
            for column in &table.columns {
                for opt in &column.options {
                    if let Options::ForeignKey(ref foreign_table_name) = opt {
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

    pub fn inherit_from() { unimplemented!() }
}