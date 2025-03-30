use chrono::{NaiveDate, NaiveTime, NaiveDateTime};
use std::collections::HashSet;
use crate::table::data::{Table, Column, Value, Options, DataType, DBRows};

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

    pub fn validate_row(&self, row: &DBRows) -> Result<(), String> {
        if row.len() != self.columns.len() {
            return Err("Row length does not match table column count".to_string());
        }

        for (i, value) in row.iter().enumerate() {
            let column = &self.columns[i];

            // 1. Type compatibility
            if !value.is_type_compatible_with(&column.datatype) {
                return Err(format!(
                    "Value at column '{}' does not match declared type {:?}",
                    column.name, column.datatype
                ));
            }

            // 2. NOT NULL check
            if let Value::Null = value {
                if column.options.contains(&Options::NotNull) {
                    return Err(format!(
                        "Column '{}' is NOT NULL but received NULL",
                        column.name
                    ));
                }
            }

            // 3. Enum/Set constraints
            match value {
                Value::Enum(val, allowed) => {
                    if !allowed.contains(val) {
                        return Err(format!(
                            "Invalid enum value '{}' in column '{}'",
                            val, column.name
                        ));
                    }
                }
                Value::Set(vals, allowed) => {
                    for v in vals {
                        if !allowed.contains(v) {
                            return Err(format!(
                                "Invalid set value '{}' in column '{}'",
                                v, column.name
                            ));
                        }
                    }
                }
                _ => {}
            }

            // 4. CHECK constraint (basic "col = value" syntax)
            for opt in &column.options {
                if let Options::Check(expr) = opt {
                    if let Some((col_name, expected_val)) = expr.split_once(" = ") {
                        if col_name.trim() == column.name {
                            if let Value::Varchar(actual) = value {
                                if actual != &expected_val.trim().to_string() {
                                    return Err(format!(
                                        "CHECK failed: column '{}' must equal '{}'",
                                        column.name, expected_val.trim()
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // 5. Unique constraint
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

        // 6. Primary key uniqueness check
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
            let val = partial_row.get(i).cloned().unwrap_or(Value::Null);
            if let Value::Null = val {
                if let Some(default) = col.options.iter().find_map(|opt| {
                    if let Options::Default(v) = opt {
                        Some(v.clone())
                    } else {
                        None
                    }
                }) {
                    full_row.push(default);
                    continue;
                }

                if col.options.contains(&Options::Autoincrement) {
                    let id = self.generate_next_autoincrement(i)?;
                    full_row.push(Value::Int(id));
                    continue;
                }
            }
            full_row.push(val);
        }
        Ok(full_row)
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

impl Column {
    pub fn validate(&self) -> Result<(), String> {
        let mut has_not_null = false;
        let mut has_default_null = false;
        let mut has_autoincrement = false;

        for opt in &self.options {
            match opt {
                Options::NotNull => has_not_null = true,
                Options::Default(Value::Null) => has_default_null = true,
                Options::Autoincrement => has_autoincrement = true,
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

impl Value {
    pub fn from_date_str(s: &str) -> Result<Self, chrono::ParseError> {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").map(Value::Date)
    }

    pub fn from_time_str(s: &str) -> Result<Self, chrono::ParseError> {
        NaiveTime::parse_from_str(s, "%H:%M:%S").map(Value::Time)
    }

    pub fn from_datetime_str(s: &str) -> Result<Self, chrono::ParseError> {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(Value::DateTime)
    }

    pub fn is_type_compatible_with(&self, dtype: &DataType) -> bool {
        match (self, dtype) {
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
            (Value::Null, _) => true, // null is allowed type-wise (check nullability separately)
            _ => false,
        }
    }

    pub fn to_display_string(&self) -> String {
        match self {
            Value::Char(c) => c.to_string(),
            Value::Varchar(s) | Value::Text(s) => s.clone(),
            Value::Enum(val, _) => val.clone(),
            Value::Set(vals, _) => format!("{{{}}}", vals.join(",")),
            Value::Boolean(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::BigInt(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Double(f) => f.to_string(),
            Value::Date(d) => d.to_string(),
            Value::Time(t) => t.to_string(),
            Value::DateTime(dt) => dt.to_string(),
            Value::Null => "NULL".to_string(),
        }
    }

    pub fn from_str(s: &str, dtype: &DataType) -> Result<Self, String> {
        let unquoted = s.trim().trim_matches('"');

        match dtype {
            DataType::Char => {
                if unquoted.len() == 1 {
                    Ok(Value::Char(unquoted.chars().next().unwrap()))
                } else {
                    Err("Expected a single character".to_string())
                }
            }
            DataType::Varchar | DataType::Text => Ok(Value::Varchar(unquoted.to_string())),
            DataType::Boolean => match unquoted {
                "true" => Ok(Value::Boolean(true)),
                "false" => Ok(Value::Boolean(false)),
                _ => Err("Invalid boolean value".to_string()),
            },
            DataType::Int => unquoted.parse().map(Value::Int).map_err(|_| "Invalid int".to_string()),
            DataType::BigInt => unquoted.parse().map(Value::BigInt).map_err(|_| "Invalid bigint".to_string()),
            DataType::Float => unquoted.parse().map(Value::Float).map_err(|_| "Invalid float".to_string()),
            DataType::Double => unquoted.parse().map(Value::Double).map_err(|_| "Invalid double".to_string()),
            DataType::Date => Value::from_date_str(unquoted).map_err(|e| format!("Invalid date: {}", e)),
            DataType::Time => Value::from_time_str(unquoted).map_err(|e| format!("Invalid time: {}", e)),
            DataType::DateTime => Value::from_datetime_str(unquoted).map_err(|e| format!("Invalid datetime: {}", e)),
            DataType::Enum => Ok(Value::Enum(unquoted.to_string(), vec![])), // assumes schema re-validates
            DataType::Set => {
                let inner = unquoted.trim_matches(|c| c == '{' || c == '}');
                let items = if inner.is_empty() {
                    vec![]
                } else {
                    inner.split(',').map(|s| s.trim().to_string()).collect()
                };
                Ok(Value::Set(items, vec![])) // again, assumes schema re-validates
            }
        }
    }
}
