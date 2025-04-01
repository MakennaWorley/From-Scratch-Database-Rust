use database::database::validators::Database;
use database::table::data::{Table, Column, DataType, Options};

use std::collections::HashMap;

#[test]
fn test_validate_foreign_keys_valid() {
    let mut tables = HashMap::new();

    let referenced_table = Table {
        name: "users".to_string(),
        columns: vec![],
        rows: vec![],
        primary_key: None,
    };

    let referencing_table = Table {
        name: "orders".to_string(),
        columns: vec![Column {
            name: "user_id".to_string(),
            datatype: DataType::Int,
            options: vec![Options::FK("users".to_string())],
        }],
        rows: vec![],
        primary_key: None,
    };

    tables.insert("users".to_string(), referenced_table);
    tables.insert("orders".to_string(), referencing_table);

    let db = Database { tables };

    assert!(db.validate_foreign_keys().is_ok());
}

#[test]
fn test_validate_foreign_keys_missing_table() {
    let mut tables = HashMap::new();

    let referencing_table = Table {
        name: "orders".to_string(),
        columns: vec![Column {
            name: "user_id".to_string(),
            datatype: DataType::Int,
            options: vec![Options::FK("users".to_string())],
        }],
        rows: vec![],
        primary_key: None,
    };

    tables.insert("orders".to_string(), referencing_table);

    let db = Database { tables };

    let result = db.validate_foreign_keys();

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err(),
        "Table 'orders' has a foreign key to missing table 'users'."
    );
}
