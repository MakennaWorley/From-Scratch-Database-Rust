use crate::table::model::{Column, DataType, Options, Table, Value};
use crate::table::DBRows;

impl Table {
    pub fn validate_row(&self, row: &DBRows) -> Result<(), String> {
        let row = self.apply_defaults(row)?;

        if row.len() != self.columns.len() {
            return Err("Row length does not match table column count".to_string());
        }

        // Column-level validation
        for (i, value) in row.iter().enumerate() {
            let column = &self.columns[i];

            // 1) Type compatibility (NULL allowed here; NotNull handled below)
            if !value.is_type_compatible_with(&column.datatype) {
                return Err(format!(
                    "Value at column '{}' does not match declared type {:?}",
                    column.name, column.datatype
                ));
            }

            // 2) NOT NULL check
            if matches!(value, Value::NULL) && column.options.contains(&Options::NotNull) {
                return Err(format!(
                    "Column '{}' is NOT NULL but received NULL",
                    column.name
                ));
            }

            // 3) Enum/Set constraints (only enforce allowed list if it exists)
            match value {
                Value::Enum(val, allowed) => {
                    if !allowed.is_empty() && !allowed.contains(val) {
                        return Err(format!(
                            "Invalid enum value '{}' in column '{}'",
                            val, column.name
                        ));
                    }
                }
                Value::Set(vals, allowed) => {
                    if !allowed.is_empty() {
                        for v in vals {
                            if !allowed.contains(v) {
                                return Err(format!(
                                    "Invalid set value '{}' in column '{}'",
                                    v, column.name
                                ));
                            }
                        }
                    }
                }
                _ => {}
            }

            // 3.25) JSON validation (always-on since you have serde_json in Cargo.toml)
            if let Value::JSON(s) = value {
                serde_json::from_str::<serde_json::Value>(s).map_err(|e| {
                    format!("Invalid JSON in column '{}': {}", column.name, e)
                })?;
            }

            // 3.5) Generated column rule: should not be directly assigned by user
            if column.datatype == DataType::Generated {
                if !matches!(value, Value::NULL) {
                    return Err(format!(
                        "Column '{}' is GENERATED and cannot be directly assigned",
                        column.name
                    ));
                }
            }

            // 4) CHECK constraint (basic "col = value" syntax)
            for opt in &column.options {
                if let Options::Check(expr) = opt {
                    if let Some((col_name, expected_val)) = expr.split_once(" = ") {
                        if col_name.trim() == column.name {
                            let expected = expected_val.trim().trim_matches('"');

                            match value {
                                Value::Varchar(actual) | Value::Text(actual) => {
                                    if actual != expected {
                                        return Err(format!(
                                            "CHECK failed: column '{}' must equal '{}'",
                                            column.name, expected
                                        ));
                                    }
                                }
                                Value::Char(c) => {
                                    if expected.len() != 1
                                        || expected.chars().next().unwrap() != *c
                                    {
                                        return Err(format!(
                                            "CHECK failed: column '{}' must equal '{}'",
                                            column.name, expected
                                        ));
                                    }
                                }
                                // You can expand this later for numeric comparisons
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        // 5) Unique constraint
        for (i, column) in self.columns.iter().enumerate() {
            if column.options.contains(&Options::Unique) {
                let value = &row[i];
                for existing in &self.rows {
                    if &existing[i] == value {
                        return Err(format!(
                            "Unique constraint violated in column '{}' for value '{}'",
                            column.name,
                            value.to_display_string()
                        ));
                    }
                }
            }
        }

        // 6) Primary key uniqueness check
        if let Some(pk_cols) = &self.primary_key {
            let pk_indices: Vec<_> = pk_cols
                .iter()
                .filter_map(|pk| self.columns.iter().position(|c| &c.name == pk))
                .collect();

            for existing in &self.rows {
                let is_duplicate = pk_indices.iter().all(|&i| row[i] == existing[i]);
                if is_duplicate {
                    return Err("Primary key constraint violated: duplicate entry".to_string());
                }
            }
        }

        Ok(())
    }

    pub fn apply_defaults(&self, partial_row: &DBRows) -> Result<DBRows, String> {
        let mut full_row = Vec::new();
        for (i, col) in self.columns.iter().enumerate() {
            let val = partial_row.get(i).cloned().unwrap_or(Value::NULL);

            if matches!(val, Value::NULL) {
                // Default
                if let Some(default) = col.options.iter().find_map(|opt| {
                    if let Options::Default(v) = opt { Some(v.clone()) } else { None }
                }) {
                    full_row.push(default);
                    continue;
                }

                // AutoIncrement
                if col.options.contains(&Options::AutoIncrement) {
                    let id = self.generate_next_autoincrement(i)?;
                    full_row.push(Value::Int(id));
                    continue;
                }

                // Generated columns should remain NULL here; computed later in execution layer
                // Hashed columns can remain NULL unless default provided
            }

            full_row.push(val);
        }
        Ok(full_row)
    }

    pub fn default_for_new_column(col: &Column) -> Result<Value, String> {
        let default_val = col.options.iter().find_map(|opt| {
            if let Options::Default(val) = opt { Some(val.clone()) } else { None }
        });

        if col.options.contains(&Options::NotNull) {
            default_val.ok_or_else(|| {
                format!("Cannot add NOT NULL column '{}' without a default value", col.name)
            })
        } else {
            Ok(default_val.unwrap_or(Value::NULL))
        }
    }

    fn generate_next_autoincrement(&self, column_index: usize) -> Result<i32, String> {
        let mut max_val = 0;
        for row in &self.rows {
            if let Some(Value::Int(v)) = row.get(column_index) {
                if *v > max_val {
                    max_val = *v;
                }
            }
        }
        Ok(max_val + 1)
    }
}
