use crate::table_data::{Table, Column, Value, DataType};

impl Table {
    pub fn new(name: &str, columns: Vec<Column>, pk: Option<Vec<String>>) -> Self {
        Table {
            name: name.to_string(),
            columns,
            rows: Vec::new(),
            primary_key: pk,
        }
    }

    pub fn insert(&mut self, values: Vec<Value>) -> Result<(), String> {
        if values.len() != self.columns.len() {
            return Err("Column count does not match".to_string());
        }

        // Basic type check (can expand to enforce options)
        for (i, value) in values.iter().enumerate() {
            let col_type = &self.columns[i].datatype;
            if !Self::value_matches_type(value, col_type) {
                return Err(format!(
                    "Type mismatch at column {}: expected {:?}, got {:?}",
                    self.columns[i].name, col_type, value
                ));
            }
        }

        self.rows.push(values);
        Ok(())
    }

    pub fn select_all(&self) -> Vec<&Vec<Value>> {
        self.rows.iter().collect()
    }

    pub fn select_where<F>(&self, predicate: F) -> Vec<&Vec<Value>>
    where
        F: Fn(&Vec<Value>) -> bool,
    {
        self.rows.iter().filter(|row| predicate(row)).collect()
    }

    pub fn update_where<F>(&mut self, predicate: F, updates: Vec<Option<Value>>)
    where
        F: Fn(&Vec<Value>) -> bool,
    {
        for row in self.rows.iter_mut().filter(|row| predicate(row)) {
            for (i, update) in updates.iter().enumerate() {
                if let Some(val) = update {
                    row[i] = val.clone();
                }
            }
        }
    }

    pub fn delete_where<F>(&mut self, predicate: F)
    where
        F: Fn(&Vec<Value>) -> bool,
    {
        self.rows.retain(|row| !predicate(row));
    }

    fn value_matches_type(val: &Value, dtype: &DataType) -> bool {
        match (val, dtype) {
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
            (Value::Null, _) => true, // Allow null everywhere for now
            _ => false,
        }
    }
}
