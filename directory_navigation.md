
## Table
```
table/
├── mod.rs

├── model/
│   ├── mod.rs
│   ├── table.rs        # struct Table + core fields
│   ├── column.rs       # struct Column, enum Options
│   ├── value.rs        # enum Value (+ display + parse helpers)
│   ├── datatype.rs     # enum DataType
│   ├── index.rs        # enum IndexType (+ get)
│   ├── query.rs        # enum Query, AggregationResult
│   └── filter.rs       # enum FilterExpr (+ to_predicate)

├── ops/
│   ├── mod.rs
│   ├── ddl.rs          # create/alter/drop/truncate/view (Table methods)
│   ├── dml.rs          # select/insert/update/delete/join/groupby/index (Table methods)
│   └── transactions.rs # begin/commit/rollback (Table methods)

├── validate/
│   ├── mod.rs
│   ├── schema.rs       # Table::validate_schema, Column::validate (schema rules)
│   ├── row.rs          # Table::validate_row, Table::apply_defaults, autoincrement
│   └── value.rs        # Value::is_type_compatible_with, value_matches_type (if you keep it)
│
└── storage/
    ├── mod.rs
    ├── csv.rs          # save/load csv + view helpers
    └── print.rs        # print_table, print_join_results

```