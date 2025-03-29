mod table_data;
mod table_functions;
mod table_validators;
mod database_validators;

use chrono::{NaiveDate};
use crate::table_data::*;
use crate::database_validators::*;

fn main() {
    // Define columns
    let columns = vec![
        Column {
            name: "id".to_string(),
            datatype: DataType::Int,
            options: vec![Options::NotNull, Options::Autoincrement],
        },
        Column {
            name: "name".to_string(),
            datatype: DataType::Varchar,
            options: vec![Options::NotNull, Options::Unique],
        },
        Column {
            name: "joined".to_string(),
            datatype: DataType::Date,
            options: vec![Options::Default(Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()))],
        },
    ];

    let mut table = Table {
        name: "users".to_string(),
        columns,
        rows: vec![],
        primary_key: Some(vec!["id".to_string()]),
    };

    // Validate schema
    match table.validate_schema() {
        Ok(_) => println!("Schema valid ✅"),
        Err(e) => println!("Schema error ❌: {}", e),
    }

    // Insert row with missing autoincrement and default date
    let input_row = vec![Value::Null, Value::Varchar("Alice".to_string())];
    match table.apply_defaults(&input_row) {
        Ok(full_row) => {
            println!("Row after defaults: {:?}", full_row);
            match table.validate_row(&full_row) {
                Ok(_) => {
                    table.rows.push(full_row);
                    println!("Row valid and inserted ✅");
                }
                Err(e) => println!("Row validation error ❌: {}", e),
            }
        }
        Err(e) => println!("Default application error ❌: {}", e),
    }

    // Try to insert a duplicate name (should violate UNIQUE)
    let duplicate_row = vec![Value::Null, Value::Varchar("Alice".to_string())];
    let dup_full = table.apply_defaults(&duplicate_row).unwrap();
    match table.validate_row(&dup_full) {
        Ok(_) => println!("Duplicate row should not be valid ❌"),
        Err(e) => println!("Duplicate row error as expected ✅: {}", e),
    }

    // Foreign key test setup
    let mut db = Database::new();
    db.tables.insert("users".to_string(), table);

    // Add table with a foreign key to a missing table
    let fk_table = Table {
        name: "logins".to_string(),
        columns: vec![
            Column {
                name: "user_id".to_string(),
                datatype: DataType::Int,
                options: vec![Options::FK("users_missing".to_string())],
            }
        ],
        rows: vec![],
        primary_key: None,
    };
    db.tables.insert("logins".to_string(), fk_table);

    match db.validate_foreign_keys() {
        Ok(_) => println!("FK validation should have failed ❌"),
        Err(e) => println!("FK validation failed as expected ✅: {}", e),
    }
}
