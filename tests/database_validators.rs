#[cfg(test)]
mod tests {
    use super::*; // Import Database from validators.rs
    use database::database::validators::Database;
    use database::table::model::{Table, Column, DataType, Options, Value};
    use std::collections::HashMap;

    // Since Table only holds the data structure, we add an extension trait in tests to implement
    // the functions that Database::alter_add_column, rename_column, and drop_column rely on.
    trait TableExt {
        fn alter_add_column(&mut self, new_column: Column) -> Result<(), String>;
        fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<(), String>;
        fn drop_column(&mut self, col_name: &str) -> Result<(), String>;
    }

    impl TableExt for Table {
        fn alter_add_column(&mut self, new_column: Column) -> Result<(), String> {
            // Return an error if the column already exists.
            if self.columns.iter().any(|c| c.name == new_column.name) {
                return Err(format!("Column '{}' already exists in table '{}'", new_column.name, self.name));
            }
            self.columns.push(new_column);
            // For each existing row, add a Null value for the new column.
            for row in &mut self.rows {
                row.push(Value::NULL);
            }
            Ok(())
        }

        fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<(), String> {
            // Check if a column with the old name exists.
            let index = self.columns.iter().position(|c| c.name == old_name)
                .ok_or_else(|| format!("Column '{}' does not exist in table '{}'", old_name, self.name))?;
            // Check if a column with the new name already exists.
            if self.columns.iter().any(|c| c.name == new_name) {
                return Err(format!("Column '{}' already exists in table '{}'", new_name, self.name));
            }
            self.columns[index].name = new_name.to_string();
            Ok(())
        }

        fn drop_column(&mut self, col_name: &str) -> Result<(), String> {
            // Find the index of the column to remove.
            let index = self.columns.iter().position(|c| c.name == col_name)
                .ok_or_else(|| format!("Column '{}' does not exist in table '{}'", col_name, self.name))?;
            self.columns.remove(index);
            // Remove the corresponding value from every row.
            for row in &mut self.rows {
                if row.len() > index {
                    row.remove(index);
                }
            }
            Ok(())
        }
    }

    // Helper function to create a dummy table.
    fn create_dummy_table(name: &str, columns: Vec<Column>, rows: Vec<Vec<Value>>) -> Table {
        Table {
            name: name.to_string(),
            columns,
            rows,
            primary_key: None,
            indexes: HashMap::new(),
            transaction_backup: None,
        }
    }

    #[test]
    fn test_database_new() {
        let db = Database::new();
        assert!(db.tables.is_empty(), "Database should be initialized with no tables");
    }

    #[test]
    fn test_create_success() {
        let mut db = Database::new();
        let table = create_dummy_table(
            "users",
            vec![
                Column { name: "id".to_string(), datatype: DataType::Int, options: vec![Options::NotNull, Options::Unique] },
                Column { name: "name".to_string(), datatype: DataType::Varchar, options: vec![] },
            ],
            vec![],
        );
        assert!(db.create(table).is_ok(), "Table creation should succeed");
        assert!(db.tables.contains_key("users"), "Database should contain the created table");
    }

    #[test]
    fn test_create_duplicate() {
        let mut db = Database::new();
        let table1 = create_dummy_table("users", vec![], vec![]);
        let table2 = create_dummy_table("users", vec![], vec![]);
        assert!(db.create(table1).is_ok(), "First table creation should succeed");
        let result = db.create(table2);
        assert!(result.is_err(), "Creating duplicate table should return an error");
        if let Err(msg) = result {
            assert!(msg.contains("already exists"), "Error message should mention table already exists");
        }
    }

    #[test]
    fn test_validate_foreign_keys_success() {
        let mut db = Database::new();
        // Create a parent table.
        let parent_table = create_dummy_table(
            "parents",
            vec![Column { name: "id".to_string(), datatype: DataType::Int, options: vec![Options::NotNull] }],
            vec![],
        );
        // Create a child table with a foreign key referencing 'parents'.
        let child_table = create_dummy_table(
            "children",
            vec![Column {
                name: "parent_id".to_string(),
                datatype: DataType::Int,
                options: vec![Options::ForeignKey("parents".to_string())],
            }],
            vec![],
        );
        db.create(parent_table).unwrap();
        db.create(child_table).unwrap();

        assert!(db.validate_foreign_keys().is_ok(), "Foreign keys should be valid when referenced table exists");
    }

    #[test]
    fn test_validate_foreign_keys_failure() {
        let mut db = Database::new();
        // Create a child table with a foreign key referencing a non-existent table.
        let child_table = create_dummy_table(
            "children",
            vec![Column {
                name: "parent_id".to_string(),
                datatype: DataType::Int,
                options: vec![Options::ForeignKey("nonexistent".to_string())],
            }],
            vec![],
        );
        db.create(child_table).unwrap();

        let result = db.validate_foreign_keys();
        assert!(result.is_err(), "Foreign key validation should fail when referenced table does not exist");
        if let Err(msg) = result {
            assert!(msg.contains("foreign key to missing table"), "Error message should mention missing foreign table");
        }
    }
}
