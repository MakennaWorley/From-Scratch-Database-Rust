use crate::table::model::{Column, DataType, Options, Value};

impl Column {
    pub fn validate_value(&self, value: &Value) -> Result<(), String> {
        // 1) Type compatibility (NULL allowed here; NOT NULL handled below)
        if !value.is_type_compatible_with(&self.datatype) {
            return Err(format!(
                "Value for column '{}' does not match declared type {:?}",
                self.name, self.datatype
            ));
        }

        // 2) NOT NULL
        if matches!(value, Value::NULL) && self.options.contains(&Options::NotNull) {
            return Err(format!(
                "Column '{}' is NOT NULL but received NULL",
                self.name
            ));
        }

        // 3) Enum/Set allowed list enforcement
        match value {
            Value::Enum(val, allowed) => {
                if !allowed.is_empty() && !allowed.contains(val) {
                    return Err(format!(
                        "Invalid enum value '{}' in column '{}'",
                        val, self.name
                    ));
                }
            }
            Value::Set(vals, allowed) => {
                if !allowed.is_empty() {
                    for v in vals {
                        if !allowed.contains(v) {
                            return Err(format!(
                                "Invalid set value '{}' in column '{}'",
                                v, self.name
                            ));
                        }
                    }
                }
            }
            _ => {}
        }

        // 4) JSON validation
        if let Value::JSON(s) = value {
            serde_json::from_str::<serde_json::Value>(s)
                .map_err(|e| format!("Invalid JSON in column '{}': {}", self.name, e))?;
        }

        // 5) Generated column rules
        if self.datatype == DataType::Generated {
            match value {
                Value::NULL => {}
                _ => {
                    return Err(format!(
                        "Column '{}' is GENERATED and cannot be directly assigned",
                        self.name
                    ));
                }
            }
        }

        // 6) CHECK constraint (your current simple parser: "col = value")
        for opt in &self.options {
            if let Options::Check(expr) = opt {
                if let Some((col_name, expected_val)) = expr.split_once(" = ") {
                    if col_name.trim() == self.name {
                        let expected = expected_val.trim().trim_matches('"');

                        match value {
                            Value::Varchar(actual) | Value::Text(actual) => {
                                if actual != expected {
                                    return Err(format!(
                                        "CHECK failed: column '{}' must equal '{}'",
                                        self.name, expected
                                    ));
                                }
                            }
                            Value::Char(c) => {
                                if expected.len() != 1 || expected.chars().next().unwrap() != *c {
                                    return Err(format!(
                                        "CHECK failed: column '{}' must equal '{}'",
                                        self.name, expected
                                    ));
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        // 7) Default sanity
        for opt in &self.options {
            if let Options::Default(def) = opt {
                if !def.is_type_compatible_with(&self.datatype) {
                    return Err(format!(
                        "Column '{}' has DEFAULT that does not match datatype {:?}",
                        self.name, self.datatype
                    ));
                }
            }
        }

        Ok(())
    }
}
