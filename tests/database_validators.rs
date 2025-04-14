#[cfg(test)]
mod tests {
    use super::*; // Import Database from validators.rs
    use database::database::validators::Database;
    use database::table::data::{Table, Column, DataType, Options, Value};
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
                row.push(Value::Null);
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
    fn test_create_table_success() {
        let mut db = Database::new();
        let table = create_dummy_table(
            "users",
            vec![
                Column { name: "id".to_string(), datatype: DataType::Int, options: vec![Options::NotNull, Options::Unique] },
                Column { name: "name".to_string(), datatype: DataType::Varchar, options: vec![] },
            ],
            vec![],
        );
        assert!(db.create_table(table).is_ok(), "Table creation should succeed");
        assert!(db.tables.contains_key("users"), "Database should contain the created table");
    }

    #[test]
    fn test_create_table_duplicate() {
        let mut db = Database::new();
        let table1 = create_dummy_table("users", vec![], vec![]);
        let table2 = create_dummy_table("users", vec![], vec![]);
        assert!(db.create_table(table1).is_ok(), "First table creation should succeed");
        let result = db.create_table(table2);
        assert!(result.is_err(), "Creating duplicate table should return an error");
        if let Err(msg) = result {
            assert!(msg.contains("already exists"), "Error message should mention table already exists");
        }
    }

    #[test]
    fn test_drop_table_success() {
        let mut db = Database::new();
        let table = create_dummy_table("orders", vec![], vec![]);
        db.create_table(table).unwrap();
        assert!(db.drop_table("orders").is_ok(), "Dropping existing table should succeed");
        assert!(!db.tables.contains_key("orders"), "Table should be removed from database");
    }

    #[test]
    fn test_drop_table_nonexistent() {
        let mut db = Database::new();
        let result = db.drop_table("nonexistent");
        assert!(result.is_err(), "Dropping a non-existent table should return an error");
        if let Err(msg) = result {
            assert!(msg.contains("does not exist"), "Error message should mention table does not exist");
        }
    }

    #[test]
    fn test_alter_add_column_success() {
        let mut db = Database::new();
        // Create a table with two columns and one row.
        let table = create_dummy_table(
            "products",
            vec![
                Column { name: "id".to_string(), datatype: DataType::Int, options: vec![Options::NotNull] },
                Column { name: "price".to_string(), datatype: DataType::Double, options: vec![] },
            ],
            vec![vec![Value::Int(1), Value::Double(10.0)]],
        );
        db.create_table(table).unwrap();

        let new_column = Column { name: "stock".to_string(), datatype: DataType::Int, options: vec![] };
        let result = db.alter_add_column("products", new_column.clone());
        assert!(result.is_ok(), "Altering table to add new column should succeed");

        // Verify that the column is added and that existing rows got a Null for the new column.
        let updated_table = db.tables.get("products").unwrap();
        assert_eq!(updated_table.columns.len(), 3, "There should be three columns after addition");
        assert_eq!(updated_table.columns.last().unwrap().name, "stock", "New column name should be 'stock'");
        for row in &updated_table.rows {
            assert_eq!(row.len(), 3, "Each row should have a new value (Null) for the added column");
            assert_eq!(row.last().unwrap(), &Value::Null, "New column value should be Null for existing rows");
        }
    }

    #[test]
    fn test_alter_add_column_table_nonexistent() {
        let mut db = Database::new();
        let new_column = Column { name: "stock".to_string(), datatype: DataType::Int, options: vec![] };
        let result = db.alter_add_column("nonexistent", new_column);
        assert!(result.is_err(), "Adding column to non-existent table should return an error");
        if let Err(msg) = result {
            assert!(msg.contains("does not exist"), "Error message should mention table does not exist");
        }
    }

    #[test]
    fn test_alter_add_column_duplicate() {
        let mut db = Database::new();
        let table = create_dummy_table(
            "inventory",
            vec![
                Column { name: "item_id".to_string(), datatype: DataType::Int, options: vec![] },
            ],
            vec![],
        );
        db.create_table(table).unwrap();
        let new_column = Column { name: "item_id".to_string(), datatype: DataType::Int, options: vec![] };
        let result = db.alter_add_column("inventory", new_column);
        assert!(result.is_err(), "Adding a duplicate column should return an error");
        if let Err(msg) = result {
            assert!(msg.contains("already exists"), "Error message should mention column already exists");
        }
    }

    #[test]
    fn test_rename_column_success() {
        let mut db = Database::new();
        let table = create_dummy_table(
            "employees",
            vec![
                Column { name: "fname".to_string(), datatype: DataType::Varchar, options: vec![] },
                Column { name: "lname".to_string(), datatype: DataType::Varchar, options: vec![] },
            ],
            vec![],
        );
        db.create_table(table).unwrap();
        let result = db.rename_column("employees", "fname", "first_name");
        assert!(result.is_ok(), "Renaming an existing column should succeed");
        let updated_table = db.tables.get("employees").unwrap();
        let col_names: Vec<_> = updated_table.columns.iter().map(|c| c.name.clone()).collect();
        assert!(col_names.contains(&"first_name".to_string()), "New column name should be present");
        assert!(!col_names.contains(&"fname".to_string()), "Old column name should be absent");
    }

    #[test]
    fn test_rename_column_nonexistent_table() {
        let mut db = Database::new();
        let result = db.rename_column("nonexistent", "old", "new");
        assert!(result.is_err(), "Renaming column in non-existent table should return an error");
        if let Err(msg) = result {
            assert!(msg.contains("does not exist"), "Error message should mention table does not exist");
        }
    }

    #[test]
    fn test_rename_column_nonexistent_column() {
        let mut db = Database::new();
        let table = create_dummy_table(
            "departments",
            vec![Column { name: "dept_name".to_string(), datatype: DataType::Varchar, options: vec![] }],
            vec![],
        );
        db.create_table(table).unwrap();
        let result = db.rename_column("departments", "nonexistent", "new_name");
        assert!(result.is_err(), "Renaming non-existent column should return an error");
        if let Err(msg) = result {
            assert!(msg.contains("does not exist"), "Error message should mention column does not exist");
        }
    }

    #[test]
    fn test_rename_column_duplicate_name() {
        let mut db = Database::new();
        let table = create_dummy_table(
            "projects",
            vec![
                Column { name: "project_id".to_string(), datatype: DataType::Int, options: vec![] },
                Column { name: "name".to_string(), datatype: DataType::Varchar, options: vec![] },
            ],
            vec![],
        );
        db.create_table(table).unwrap();
        // Attempt to rename 'project_id' to 'name', which already exists.
        let result = db.rename_column("projects", "project_id", "name");
        assert!(result.is_err(), "Renaming to an existing column name should return an error");
        if let Err(msg) = result {
            assert!(msg.contains("already exists"), "Error message should mention duplicate column name");
        }
    }

    #[test]
    fn test_drop_column_success() {
        let mut db = Database::new();
        let table = create_dummy_table(
            "sales",
            vec![
                Column { name: "sale_id".to_string(), datatype: DataType::Int, options: vec![] },
                Column { name: "amount".to_string(), datatype: DataType::Double, options: vec![] },
            ],
            vec![
                vec![Value::Int(1), Value::Double(100.0)],
                vec![Value::Int(2), Value::Double(200.0)],
            ],
        );
        db.create_table(table).unwrap();
        let result = db.drop_column("sales", "amount");
        assert!(result.is_ok(), "Dropping an existing column should succeed");
        let updated_table = db.tables.get("sales").unwrap();
        assert_eq!(updated_table.columns.len(), 1, "Table should have one column after dropping one");
        for row in &updated_table.rows {
            assert_eq!(row.len(), 1, "Each row should have one less value after dropping a column");
        }
    }

    #[test]
    fn test_drop_column_nonexistent_table() {
        let mut db = Database::new();
        let result = db.drop_column("nonexistent", "any");
        assert!(result.is_err(), "Dropping column from non-existent table should return an error");
        if let Err(msg) = result {
            assert!(msg.contains("does not exist"), "Error message should mention table does not exist");
        }
    }

    #[test]
    fn test_drop_column_nonexistent_column() {
        let mut db = Database::new();
        let table = create_dummy_table(
            "inventory",
            vec![Column { name: "item".to_string(), datatype: DataType::Varchar, options: vec![] }],
            vec![],
        );
        db.create_table(table).unwrap();
        let result = db.drop_column("inventory", "price");
        assert!(result.is_err(), "Dropping a non-existent column should return an error");
        if let Err(msg) = result {
            assert!(msg.contains("does not exist"), "Error message should mention column does not exist");
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
                options: vec![Options::FK("parents".to_string())],
            }],
            vec![],
        );
        db.create_table(parent_table).unwrap();
        db.create_table(child_table).unwrap();

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
                options: vec![Options::FK("nonexistent".to_string())],
            }],
            vec![],
        );
        db.create_table(child_table).unwrap();

        let result = db.validate_foreign_keys();
        assert!(result.is_err(), "Foreign key validation should fail when referenced table does not exist");
        if let Err(msg) = result {
            assert!(msg.contains("foreign key to missing table"), "Error message should mention missing foreign table");
        }
    }
}
