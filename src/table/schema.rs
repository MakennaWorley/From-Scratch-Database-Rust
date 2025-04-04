use crate::table::data::{Column, Options, Table, Value};

impl Table {
    pub fn alter_add_column(&mut self, new_column: Column) -> Result<(), String> {
        if self.columns.iter().any(|col| col.name == new_column.name) {
            return Err(format!(
                "Column '{}' already exists in table '{}'",
                new_column.name, self.name
            ));
        }

        new_column.validate()?;

        let default_val = new_column.options.iter().find_map(|opt| {
            if let Options::Default(ref val) = opt {
                Some(val.clone())
            } else {
                None
            }
        });

        let default = if new_column.options.contains(&Options::NotNull) {
            default_val.ok_or_else(|| {
                format!(
                    "Cannot add NOT NULL column '{}' without a default value",
                    new_column.name
                )
            })?
        } else {
            default_val.unwrap_or(Value::Null)
        };

        for row in &mut self.rows {
            row.push(default.clone());
        }

        self.columns.push(new_column);

        Ok(())
    }

    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<(), String> {
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

    pub fn drop_column(&mut self, name: &str) -> Result<(), String> {
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
}