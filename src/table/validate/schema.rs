use crate::table::model::{Table, Options, DataType, Value, Column};
use std::collections::HashSet;

impl Table {
    pub fn validate_schema(&self) -> Result<(), String> {
        // Check for duplicate column names
        let mut seen = HashSet::new();
        for col in &self.columns {
            if !seen.insert(&col.name) {
                return Err(format!("Duplicate column name found: '{}'", col.name));
            }
        }

        // Check that primary key columns exist
        if let Some(pk_cols) = &self.primary_key {
            for pk in pk_cols {
                if !self.columns.iter().any(|c| &c.name == pk) {
                    return Err(format!(
                        "Primary key column '{}' not found in table '{}'",
                        pk, self.name
                    ));
                }
            }
        }

        // Validate each column individually
        for col in &self.columns {
            col.validate()?;
        }

        Ok(())
    }
}

impl Column {
    pub fn validate(&self) -> Result<(), String> {
        let mut has_not_null = false;
        let mut has_default_null = false;
        let mut has_autoincrement = false;

        for opt in &self.options {
            match opt {
                Options::NotNull => has_not_null = true,
                Options::Default(Value::NULL) => has_default_null = true,
                Options::AutoIncrement => has_autoincrement = true,
                _ => {}
            }
        }

        if has_default_null && has_not_null {
            return Err(format!(
                "Column '{}' cannot have both DEFAULT NULL and NOT NULL",
                self.name
            ));
        }

        if has_autoincrement {
            if !(self.datatype == DataType::Int || self.datatype == DataType::BigInt) {
                return Err(format!(
                    "Column '{}' has AUTOINCREMENT but is not Int or BigInt.",
                    self.name
                ));
            }
            if !has_not_null {
                return Err(format!(
                    "Column '{}' has AUTOINCREMENT but is not marked NOT NULL.",
                    self.name
                ));
            }
        }

        for opt in &self.options {
            if let Options::Default(Value::Enum(val, allowed)) = opt {
                if !allowed.contains(val) {
                    return Err(format!(
                        "Default enum value '{}' not in allowed list for column '{}'",
                        val, self.name
                    ));
                }
            }

            if let Options::Default(Value::Set(vals, allowed)) = opt {
                for v in vals {
                    if !allowed.contains(v) {
                        return Err(format!(
                            "Default set value '{}' not in allowed list for column '{}'",
                            v, self.name
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}