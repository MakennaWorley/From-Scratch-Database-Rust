use std::collections::HashMap;
use crate::table_data::{Table, Options};

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