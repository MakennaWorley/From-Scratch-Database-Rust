mod table_data;
mod table_functions;
mod table_validators;
mod database_validators;

use chrono::NaiveDate;
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
            options: vec![Options::Default(Value::Date(
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            ))],
        },
    ];

    // Create table and validate schema
    let mut table = Table::new("users", columns.clone(), Some(vec!["id".to_string()]));
    match table.validate_schema() {
        Ok(_) => println!("Schema valid ✅"),
        Err(e) => println!("Schema error ❌: {}", e),
    }

    // Insert a valid row
    let input_row = vec![Value::Null, Value::Varchar("Alice".to_string()), Value::Null];
    match table.insert(input_row) {
        Ok(_) => println!("Row valid and inserted ✅"),
        Err(e) => println!("Insert failed ❌: {}", e),
    }

    let input_row = vec![Value::Null, Value::Varchar("Bella".to_string()), Value::Null];
    match table.insert(input_row) {
        Ok(_) => println!("Row valid and inserted ✅"),
        Err(e) => println!("Insert failed ❌: {}", e),
    }

    // Insert duplicate (should violate UNIQUE constraint)
    let duplicate_row = vec![Value::Null, Value::Varchar("Alice".to_string()), Value::Null];
    match table.insert(duplicate_row) {
        Ok(_) => println!("❌ Inserted duplicate — UNIQUE constraint failed!"),
        Err(e) => println!("✅ UNIQUE constraint working as expected: {}", e),
    }

    // Save table to file
    match table.save_to_file("testdb") {
        Ok(_) => println!("Table saved to file ✅"),
        Err(e) => println!("Failed to save file ❌: {}", e),
    }

    // Load table back
    let loaded_columns = table.columns.clone();
    let loaded_table = Table::load_from_file("testdb", "users", loaded_columns, Some(vec!["id".to_string()]));
    match loaded_table {
        Ok(t) => {
            println!("Loaded table:");
            t.print_table();
        }
        Err(e) => println!("Failed to load table ❌: {}", e),
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

    // === Additional Edge Case Tests ===

    println!("\n=== Edge Case Tests ===");

    // NOT NULL Violation
    let columns_not_null = vec![
        Column {
            name: "email".to_string(),
            datatype: DataType::Varchar,
            options: vec![Options::NotNull],
        }
    ];
    let mut not_null_table = Table::new("emails", columns_not_null, None);
    let result = not_null_table.insert(vec![Value::Null]);
    match result {
        Ok(_) => println!("❌ NOT NULL violation not caught!"),
        Err(e) => println!("✅ NOT NULL test passed: {}", e),
    }

    // CHECK Constraint Violation
    let columns_check = vec![
        Column {
            name: "status".to_string(),
            datatype: DataType::Varchar,
            options: vec![Options::Check("status = active".to_string())],
        }
    ];
    let mut check_table = Table::new("checktest", columns_check, None);
    let result = check_table.insert(vec![Value::Varchar("inactive".to_string())]);
    match result {
        Ok(_) => println!("❌ CHECK constraint violation not caught!"),
        Err(e) => println!("✅ CHECK constraint test passed: {}", e),
    }

    // ENUM Constraint Violation
    let columns_enum = vec![
        Column {
            name: "role".to_string(),
            datatype: DataType::Enum,
            options: vec![],
        }
    ];
    let allowed_roles = vec!["admin".to_string(), "user".to_string()];
    let mut enum_table = Table::new("enumtest", columns_enum, None);
    let result = enum_table.insert(vec![Value::Enum("guest".to_string(), allowed_roles.clone())]);
    match result {
        Ok(_) => println!("❌ ENUM constraint violation not caught!"),
        Err(e) => println!("✅ ENUM constraint test passed: {}", e),
    }

    // SET Constraint Violation
    let columns_set = vec![
        Column {
            name: "tags".to_string(),
            datatype: DataType::Set,
            options: vec![],
        }
    ];
    let allowed_tags = vec!["safe".to_string(), "reviewed".to_string()];
    let mut set_table = Table::new("settest", columns_set, None);
    let result = set_table.insert(vec![Value::Set(vec!["dangerous".to_string()], allowed_tags.clone())]);
    match result {
        Ok(_) => println!("❌ SET constraint violation not caught!"),
        Err(e) => println!("✅ SET constraint test passed: {}", e),
    }

    // Autoincrement Check
    let columns_auto = vec![
        Column {
            name: "id".to_string(),
            datatype: DataType::Int,
            options: vec![Options::NotNull, Options::Autoincrement],
        }
    ];
    let mut auto_table = Table::new("autotest", columns_auto, Some(vec!["id".to_string()]));
    auto_table.insert(vec![Value::Null]).unwrap();
    auto_table.insert(vec![Value::Null]).unwrap();
    println!("✅ Autoincrement test:");
    auto_table.print_table();

    // Insert with defaults (joined date set via default)
    let columns_default = vec![
        Column {
            name: "joined".to_string(),
            datatype: DataType::Date,
            options: vec![Options::Default(Value::Date(NaiveDate::from_ymd_opt(2023, 12, 25).unwrap()))],
        }
    ];
    let mut default_table = Table::new("defaulttest", columns_default, None);
    default_table.insert(vec![Value::Null]).unwrap();
    println!("✅ Default value test:");
    default_table.print_table();

    let columns_pk = vec![
        Column {
            name: "id".to_string(),
            datatype: DataType::Int,
            options: vec![Options::NotNull],
        }
    ];
    let mut pk_table = Table::new("pk_test", columns_pk, Some(vec!["id".to_string()]));
    pk_table.insert(vec![Value::Int(1)]).unwrap();
    let result = pk_table.insert(vec![Value::Int(1)]);
    match result {
        Ok(_) => println!("❌ Duplicate primary key not caught!"),
        Err(e) => println!("✅ Primary key uniqueness enforced: {}", e),
    }

    let columns_multi_pk = vec![
        Column {
            name: "first".to_string(),
            datatype: DataType::Int,
            options: vec![],
        },
        Column {
            name: "second".to_string(),
            datatype: DataType::Int,
            options: vec![],
        },
    ];
    let mut multi_pk_table = Table::new("multipk", columns_multi_pk, Some(vec!["first".to_string(), "second".to_string()]));
    multi_pk_table.insert(vec![Value::Int(1), Value::Int(2)]).unwrap();
    let result = multi_pk_table.insert(vec![Value::Int(1), Value::Int(2)]);
    match result {
        Ok(_) => println!("❌ Multi-column primary key not enforced!"),
        Err(e) => println!("✅ Multi-column PK test passed: {}", e),
    }

    let columns_type = vec![
        Column {
            name: "age".to_string(),
            datatype: DataType::Int,
            options: vec![],
        }
    ];
    let mut type_table = Table::new("typetest", columns_type, None);
    let result = type_table.insert(vec![Value::Varchar("not an int".to_string())]);
    match result {
        Ok(_) => println!("❌ Type mismatch not caught!"),
        Err(e) => println!("✅ Type mismatch caught: {}", e),
    }

    let columns_mismatch = vec![
        Column { name: "a".to_string(), datatype: DataType::Int, options: vec![] },
        Column { name: "b".to_string(), datatype: DataType::Int, options: vec![] },
    ];
    let mut mismatch_table = Table::new("col_mismatch", columns_mismatch, None);
    let result = mismatch_table.insert(vec![Value::Int(1)]); // Missing second column
    match result {
        Ok(_) => println!("❌ Insert with missing values not caught!"),
        Err(e) => println!("✅ Column count mismatch caught: {}", e),
    }

    let mut db = Database::new();

    let user_cols = vec![Column {
        name: "id".to_string(),
        datatype: DataType::Int,
        options: vec![Options::NotNull, Options::Autoincrement],
    }];
    let mut user_table = Table::new("users", user_cols.clone(), Some(vec!["id".to_string()]));
    user_table.insert(vec![Value::Null]).unwrap();

    let login_cols = vec![Column {
        name: "user_id".to_string(),
        datatype: DataType::Int,
        options: vec![Options::FK("users".to_string())],
    }];
    let mut login_table = Table::new("logins", login_cols, None);
    login_table.insert(vec![Value::Int(1)]).unwrap(); // Should pass FK

    db.tables.insert("users".to_string(), user_table);
    db.tables.insert("logins".to_string(), login_table);

    match db.validate_foreign_keys() {
        Ok(_) => println!("✅ Valid FK test passed"),
        Err(e) => println!("❌ Valid FK check failed: {}", e),
    }

    let combo_cols = vec![Column {
        name: "score".to_string(),
        datatype: DataType::Int,
        options: vec![Options::NotNull, Options::Default(Value::Int(100))],
    }];
    let mut combo_table = Table::new("combo", combo_cols, None);
    let result = combo_table.insert(vec![Value::Null]);
    match result {
        Ok(_) => {
            println!("✅ Default + NOT NULL combo accepted:");
            combo_table.print_table();
        },
        Err(e) => println!("❌ Default + NOT NULL test failed: {}", e),
    }

    // Complex types test: save + load
    let complex_columns = vec![
        Column {
            name: "joined".to_string(),
            datatype: DataType::Date,
            options: vec![],
        },
        Column {
            name: "role".to_string(),
            datatype: DataType::Enum,
            options: vec![],
        },
        Column {
            name: "tags".to_string(),
            datatype: DataType::Set,
            options: vec![],
        },
    ];
    let mut complex_table = Table::new("complex", complex_columns.clone(), None);

    // Insert with complex values
    let insert_result = complex_table.insert(vec![
        Value::Date(NaiveDate::from_ymd_opt(2024, 5, 20).unwrap()),
        Value::Enum("admin".to_string(), vec!["admin".to_string(), "user".to_string()]),
        Value::Set(vec!["safe".to_string(), "reviewed".to_string()], vec!["safe".to_string(), "reviewed".to_string(), "flagged".to_string()])
    ]);

    match insert_result {
        Ok(_) => println!("✅ Inserted complex row"),
        Err(e) => println!("❌ Insert failed: {}", e),
    }

    // Save to file
    match complex_table.save_to_file("testdb") {
        Ok(_) => println!("✅ Complex table saved"),
        Err(e) => println!("❌ Save failed: {}", e),
    }

    // Load it back
    let loaded_table = Table::load_from_file("testdb", "complex", complex_columns.clone(), None);
    match loaded_table {
        Ok(t) => {
            println!("✅ Loaded complex table:");
            t.print_table();
        },
        Err(e) => println!("❌ Load failed: {}", e),
    }

    // Delete from an empty table
    let empty_columns = vec![
        Column {
            name: "id".to_string(),
            datatype: DataType::Int,
            options: vec![],
        }
    ];
    let mut empty_table = Table::new("empty_delete", empty_columns.clone(), None);
    empty_table.delete_where(|row| match &row[0] {
        Value::Int(x) => *x == 1,
        _ => false,
    });
    println!("✅ Safe delete on empty table passed");
    empty_table.print_table();

    // Delete a row that doesn’t exist
    let mut one_row_table = Table::new("delete_miss", empty_columns.clone(), None);
    one_row_table.insert(vec![Value::Int(1)]).unwrap();
    one_row_table.delete_where(|row| match &row[0] {
        Value::Int(x) => *x == 999,
        _ => false,
    });
    println!("✅ No rows deleted, as expected:");
    one_row_table.print_table();
}
