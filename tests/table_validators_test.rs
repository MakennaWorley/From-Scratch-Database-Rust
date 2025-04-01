use database::table::data::{Table, Column, DataType, Value, Options};

fn col(name: &str, dtype: DataType, options: Vec<Options>) -> Column {
    Column {
        name: name.to_string(),
        datatype: dtype,
        options,
    }
}

fn int_val(n: i32) -> Value {
    Value::Int(n)
}

#[test]
fn test_validate_schema_duplicate_columns() {
    let columns = vec![
        col("id", DataType::Int, vec![]),
        col("id", DataType::Int, vec![]),
    ];
    let table = Table::new("users", columns, None);
    let result = table.validate_schema();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Duplicate column name"));
}

#[test]
fn test_validate_schema_missing_primary_key_column() {
    let columns = vec![col("id", DataType::Int, vec![])];
    let table = Table::new("users", columns, Some(vec!["not_id".to_string()]));
    let result = table.validate_schema();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Primary key column"));
}

#[test]
fn test_validate_row_type_mismatch() {
    let columns = vec![col("id", DataType::Int, vec![])];
    let mut table = Table::new("test", columns, None);
    let row = vec![Value::Varchar("oops".to_string())];
    let result = table.validate_row(&row);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not match declared type"));
}

#[test]
fn test_validate_row_not_null_violation() {
    let columns = vec![col("name", DataType::Varchar, vec![Options::NotNull])];
    let mut table = Table::new("test", columns, None);
    let row = vec![Value::Null];
    let result = table.validate_row(&row);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("NOT NULL"));
}

#[test]
fn test_validate_row_enum_constraint_violation() {
    let allowed = vec!["Red".to_string(), "Blue".to_string()];
    let columns = vec![col("color", DataType::Enum, vec![])];
    let mut table = Table::new("test", columns, None);
    let row = vec![Value::Enum("Green".to_string(), allowed)];
    let result = table.validate_row(&row);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Invalid enum value"));
}

#[test]
fn test_validate_row_check_constraint_failure() {
    let columns = vec![col(
        "status",
        DataType::Varchar,
        vec![Options::Check("status = active".to_string())],
    )];
    let mut table = Table::new("test", columns, None);
    let row = vec![Value::Varchar("inactive".to_string())];
    let result = table.validate_row(&row);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("CHECK failed"));
}

#[test]
fn test_validate_row_unique_violation() {
    let columns = vec![col("id", DataType::Int, vec![Options::Unique])];
    let mut table = Table::new("test", columns.clone(), None);
    table.insert(vec![int_val(1)]).unwrap();
    let result = table.validate_row(&vec![int_val(1)]);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique constraint violated"));
}

#[test]
fn test_validate_row_primary_key_violation() {
    let columns = vec![
        col("id", DataType::Int, vec![]),
        col("name", DataType::Varchar, vec![]),
    ];
    let mut table = Table::new("test", columns, Some(vec!["id".to_string()]));
    table.insert(vec![int_val(1), Value::Varchar("Alice".to_string())]).unwrap();
    let result = table.validate_row(&vec![int_val(1), Value::Varchar("Bob".to_string())]);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err(),
        "Primary key constraint violated: duplicate entry"
    );
}

#[test]
fn test_apply_defaults_and_autoincrement() {
    let columns = vec![
        col(
            "id",
            DataType::Int,
            vec![Options::NotNull, Options::Autoincrement],
        ),
        col(
            "role",
            DataType::Varchar,
            vec![Options::Default(Value::Varchar("user".to_string()))],
        ),
    ];
    let mut table = Table::new("accounts", columns, None);
    let row = vec![Value::Null, Value::Null];
    let result = table.apply_defaults(&row).unwrap();
    assert_eq!(result[0], int_val(1));
    assert_eq!(result[1], Value::Varchar("user".to_string()));

    // Insert and apply again to confirm autoincrement increments
    table.rows.push(result);
    let second = table.apply_defaults(&vec![Value::Null, Value::Null]).unwrap();
    assert_eq!(second[0], int_val(2));
}
