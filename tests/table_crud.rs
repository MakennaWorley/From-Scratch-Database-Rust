#[cfg(test)]
mod tests {
    use super::*;
    use database::table::data::{Column, DataType, Options, Value, Table};
    use database::table::filters::FilterExpr;
    use std::collections::HashMap;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    // Helper function to compare two vectors of Columns by comparing each field.
    fn compare_columns(cols1: &Vec<Column>, cols2: &Vec<Column>) {
        assert_eq!(cols1.len(), cols2.len(), "Column lengths differ");
        for (a, b) in cols1.iter().zip(cols2.iter()) {
            assert_eq!(a.name, b.name, "Column names differ: {} vs {}", a.name, b.name);
            assert_eq!(a.datatype, b.datatype, "Datatypes differ for column {}", a.name);
            assert_eq!(a.options, b.options, "Options differ for column {}", a.name);
        }
    }

    #[test]
    fn test_new_table() {
        let columns = vec![
            Column {
                name: "id".to_string(),
                datatype: DataType::Int,
                options: vec![Options::NotNull],
            },
            Column {
                name: "name".to_string(),
                datatype: DataType::Varchar,
                options: vec![],
            },
        ];
        let pk = Some(vec!["id".to_string()]);
        let table = Table::new("users", columns.clone(), pk.clone());
        assert_eq!(table.name, "users");
        compare_columns(&table.columns, &columns);
        assert_eq!(table.primary_key, pk);
    }

    #[test]
    fn test_insert_success() {
        let columns = vec![
            Column {
                name: "id".to_string(),
                datatype: DataType::Int,
                options: vec![Options::NotNull],
            },
            Column {
                name: "name".to_string(),
                datatype: DataType::Varchar,
                options: vec![],
            },
        ];
        let mut table = Table::new("users", columns, None);
        let row = vec![Value::Int(1), Value::Varchar("Alice".to_string())];
        let result = table.insert(row.clone());
        assert!(result.is_ok(), "Insertion should succeed");
        assert_eq!(table.rows.len(), 1, "Table should have one row after insertion");
        assert_eq!(table.rows[0], row, "Inserted row should match provided values");
    }

    #[test]
    fn test_update_where() {
        let columns = vec![
            Column {
                name: "id".to_string(),
                datatype: DataType::Int,
                options: vec![Options::NotNull],
            },
            Column {
                name: "score".to_string(),
                datatype: DataType::Int,
                options: vec![],
            },
        ];
        let mut table = Table::new("scores", columns, None);
        // Insert two rows.
        let row1 = vec![Value::Int(1), Value::Int(50)];
        let row2 = vec![Value::Int(2), Value::Int(60)];
        table.insert(row1).unwrap();
        table.insert(row2).unwrap();

        // Update rows where id == 1, setting score to 100.
        let filter = FilterExpr::Eq("id".to_string(), Value::Int(1));
        // Updates: None for the first column, Some(100) for the second.
        let updates = vec![None, Some(Value::Int(100))];
        let result = table.update_where(&filter, updates);
        assert!(result.is_ok(), "Update should succeed");
        // Check that the first row's score was updated.
        assert_eq!(table.rows[0][1], Value::Int(100));
        // The second row remains unchanged.
        assert_eq!(table.rows[1][1], Value::Int(60));
    }

    #[test]
    fn test_delete_where() {
        let columns = vec![
            Column {
                name: "id".to_string(),
                datatype: DataType::Int,
                options: vec![Options::NotNull],
            },
            Column {
                name: "name".to_string(),
                datatype: DataType::Varchar,
                options: vec![],
            },
        ];
        let mut table = Table::new("users", columns, None);
        // Insert three rows.
        let row1 = vec![Value::Int(1), Value::Varchar("Alice".to_string())];
        let row2 = vec![Value::Int(2), Value::Varchar("Bob".to_string())];
        let row3 = vec![Value::Int(3), Value::Varchar("Charlie".to_string())];
        table.insert(row1).unwrap();
        table.insert(row2).unwrap();
        table.insert(row3).unwrap();

        // Delete rows where name == "Bob".
        let filter = FilterExpr::Eq("name".to_string(), Value::Varchar("Bob".to_string()));
        table.delete_where(&filter);
        // Expect only "Alice" and "Charlie" to remain.
        assert_eq!(table.rows.len(), 2, "Two rows should remain after deletion");
        let remaining_names: Vec<String> = table.rows.iter().map(|row| {
            if let Value::Varchar(ref s) = row[1] {
                s.clone()
            } else {
                String::new()
            }
        }).collect();
        assert!(remaining_names.contains(&"Alice".to_string()));
        assert!(remaining_names.contains(&"Charlie".to_string()));
        assert!(!remaining_names.contains(&"Bob".to_string()));
    }

    #[test]
    fn test_with_alias() {
        let columns = vec![
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
        ];
        let table = Table::new("employees", columns.clone(), None);
        let aliased_table = table.with_alias("emp");
        // New table name should be "employees_alias".
        assert_eq!(aliased_table.name, "employees_alias");
        // Each column name should start with "emp.".
        for col in aliased_table.columns.iter() {
            assert!(col.name.starts_with("emp."), "Column name '{}' should start with 'emp.'", col.name);
        }
        // Rows should be identical.
        assert_eq!(table.rows, aliased_table.rows, "Rows should match in the aliased table");
    }

    fn make_test_table() -> Table {
        let columns = vec![
            Column { name: "char_col".to_string(), datatype: DataType::Char, options: vec![] },
            Column { name: "varchar_col".to_string(), datatype: DataType::Varchar, options: vec![] },
            Column { name: "text_col".to_string(), datatype: DataType::Text, options: vec![] },
            Column { name: "enum_col".to_string(), datatype: DataType::Enum, options: vec![] },
            Column { name: "set_col".to_string(), datatype: DataType::Set, options: vec![] },
            Column { name: "bool_col".to_string(), datatype: DataType::Boolean, options: vec![] },
            Column { name: "int_col".to_string(), datatype: DataType::Int, options: vec![] },
            Column { name: "bigint_col".to_string(), datatype: DataType::BigInt, options: vec![] },
            Column { name: "float_col".to_string(), datatype: DataType::Float, options: vec![] },
            Column { name: "double_col".to_string(), datatype: DataType::Double, options: vec![] },
            Column { name: "date_col".to_string(), datatype: DataType::Date, options: vec![] },
            Column { name: "time_col".to_string(), datatype: DataType::Time, options: vec![] },
            Column { name: "datetime_col".to_string(), datatype: DataType::DateTime, options: vec![] },
        ];

        Table::new("test_all_types", columns, None)
    }

    fn make_test_row() -> Vec<Value> {
        vec![
            Value::Char('c'),
            Value::Varchar("hello".to_string()),
            Value::Text("this is a long string".to_string()),
            Value::Enum("red".to_string(), vec!["red".to_string(), "green".to_string(), "blue".to_string()]),
            Value::Set(
                vec!["apple".to_string(), "banana".to_string()],
                vec!["apple".to_string(), "banana".to_string(), "cherry".to_string()],
            ),
            Value::Boolean(true),
            Value::Int(42),
            Value::BigInt(1_000_000_000_000),
            Value::Float(3.14),
            Value::Double(2.718281828459),
            Value::Date(NaiveDate::from_ymd_opt(2024, 4, 13).unwrap()),
            Value::Time(NaiveTime::from_hms_opt(14, 30, 0).unwrap()),
            Value::DateTime(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2024, 4, 13).unwrap(),
                NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
            )),
        ]
    }

    #[test]
    fn test_insert_update_delete() {
        let mut table = make_test_table();
        let row = make_test_row();

        // Insert
        assert!(table.insert(row.clone()).is_ok());
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0], row);

        // Update `int_col` where `varchar_col == "hello"`
        let filter = FilterExpr::Eq("varchar_col".to_string(), Value::Varchar("hello".to_string()));
        let mut updates = vec![None; row.len()];
        updates[6] = Some(Value::Int(100)); // Update int_col (index 6)

        assert!(table.update_where(&filter, updates.clone()).is_ok());
        assert_eq!(table.rows[0][6], Value::Int(100));

        // Delete the row
        table.delete_where(&FilterExpr::Eq("int_col".to_string(), Value::Int(100)));
        assert_eq!(table.rows.len(), 0);
    }

    #[test]
    fn test_with_alias_2() {
        let mut table = make_test_table();
        let row = make_test_row();
        table.insert(row).unwrap();

        let aliased = table.with_alias("alias");
        for col in &aliased.columns {
            assert!(col.name.starts_with("alias."));
        }
        assert_eq!(aliased.rows.len(), 1);
    }
}
