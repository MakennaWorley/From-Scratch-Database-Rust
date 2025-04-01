use database::table::data::{Table, Column, DataType, Value};
use std::fs;

fn basic_columns() -> Vec<Column> {
    vec![
        Column {
            name: "id".to_string(),
            datatype: DataType::Int,
            options: vec![],
        },
        Column {
            name: "name".to_string(),
            datatype: DataType::Varchar,
            options: vec![],
        },
    ]
}

#[test]
fn test_new_and_insert_valid() {
    let columns = basic_columns();
    let mut table = Table::new("people", columns.clone(), Some(vec!["id".to_string()]));

    let row = vec![Value::Int(1), Value::Varchar("Alice".to_string())];
    let result = table.insert(row);

    assert!(result.is_ok());
    assert_eq!(table.rows.len(), 1);
}

#[test]
fn test_insert_type_mismatch() {
    let columns = basic_columns();
    let mut table = Table::new("people", columns, None);

    let row = vec![Value::Varchar("Not an Int".to_string()), Value::Varchar("Bob".to_string())];
    let result = table.insert(row);

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err(),
        "Type mismatch at column id: expected Int, got Varchar(\"Not an Int\")"
    );
}

#[test]
fn test_select_all_and_select_where() {
    let columns = basic_columns();
    let mut table = Table::new("people", columns.clone(), None);

    table.insert(vec![Value::Int(1), Value::Varchar("Alice".to_string())]).unwrap();
    table.insert(vec![Value::Int(2), Value::Varchar("Bob".to_string())]).unwrap();

    let all = table.select_all();
    assert_eq!(all.len(), 2);

    let filtered = table.select_where(|row| row[0] == Value::Int(1));
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0][1], Value::Varchar("Alice".to_string()));
}

#[test]
fn test_update_where() {
    let columns = basic_columns();
    let mut table = Table::new("people", columns.clone(), None);

    table.insert(vec![Value::Int(1), Value::Varchar("Alice".to_string())]).unwrap();

    let result = table.update_where(
        |row| row[0] == Value::Int(1),
        vec![None, Some(Value::Varchar("Alicia".to_string()))],
    );

    assert!(result.is_ok());

    let updated = table.select_all();
    assert_eq!(updated[0][1], Value::Varchar("Alicia".to_string()));
}

#[test]
fn test_delete_where() {
    let columns = basic_columns();
    let mut table = Table::new("people", columns.clone(), None);

    table.insert(vec![Value::Int(1), Value::Varchar("Alice".to_string())]).unwrap();
    table.insert(vec![Value::Int(2), Value::Varchar("Bob".to_string())]).unwrap();

    table.delete_where(|row| row[0] == Value::Int(1));

    let remaining = table.select_all();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0][1], Value::Varchar("Bob".to_string()));
}

#[test]
fn test_save_and_load_file() {
    let columns = basic_columns();
    let mut table = Table::new("people", columns.clone(), None);

    table.insert(vec![Value::Int(1), Value::Varchar("Alice".to_string())]).unwrap();
    table.insert(vec![Value::Int(2), Value::Varchar("Bob".to_string())]).unwrap();

    table.save_to_file("testdb").unwrap();

    let loaded = Table::load_from_file("testdb", "people", columns.clone(), None).unwrap();

    assert_eq!(loaded.rows.len(), 2);
    assert_eq!(loaded.rows[0][1], Value::Varchar("Alice".to_string()));

    // Clean up
    fs::remove_file("db/testdb.people.csv").unwrap();
}
