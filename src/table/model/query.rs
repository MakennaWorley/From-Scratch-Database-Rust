use crate::table::model::{Column, DataType, Table, Value};
use std::collections::HashMap;

#[derive(Debug)]
pub enum AggregationResult {
    Sum(f64),
    Avg(f64),
    Count(usize),
    Min(Value),
    Max(Value),
}

#[derive(Debug, Clone, Copy)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Lag { col_idx: usize, offset: usize },
    Lead { col_idx: usize, offset: usize },
}

#[derive(Debug, Clone)]
pub struct OverClause {
    pub partition_by: Vec<usize>,
    pub order_by: Vec<(usize, OrderDirection)>,
}

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub output_name: String,
    pub func: WindowFunction,
    pub over: OverClause,
}


pub enum Query {
    Select {
        table: Table,
        filter: Option<Box<dyn Fn(&Vec<Value>) -> bool>>,
    },
    TableRef {
        name: String,
    },
    With {
        recursive: bool,
        ctes: Vec<(String, Box<Query>)>,
        body: Box<Query>,
    },
    Window {
        input: Box<Query>,
        specs: Vec<WindowSpec>,
    },
    Union {
        left: Box<Query>,
        right: Box<Query>,
    },
    Intersect {
        left: Box<Query>,
        right: Box<Query>,
    },
    Except {
        left: Box<Query>,
        right: Box<Query>,
    },
}

impl Query {
    pub fn execute(&self) -> Result<Table, String> {
        let mut ctx: HashMap<String, Table> = HashMap::new();
        self.execute_with_ctx(&mut ctx)
    }

    fn execute_with_ctx(&self, ctx: &mut HashMap<String, Table>) -> Result<Table, String> {
        match self {
            Query::Select { table, filter } => {
                let rows = if let Some(pred) = filter {
                    table.rows.iter().filter(|row| pred(row)).cloned().collect()
                } else {
                    table.rows.clone()
                };

                Ok(Table {
                    name: table.name.clone(),
                    columns: table.columns.clone(),
                    rows,
                    primary_key: table.primary_key.clone(),
                    indexes: HashMap::new(),
                    transaction_backup: None,
                })
            }

            Query::TableRef { name } => ctx
                .get(name)
                .map(|t| t.clone_data_only())
                .ok_or_else(|| format!("Unknown table/CTE reference: {}", name)),

            Query::With {
                recursive,
                ctes,
                body,
            } => {
                if *recursive {
                    return Err("WITH RECURSIVE not implemented yet (non-recursive WITH works)".to_string());
                }

                // Evaluate CTEs in order; later CTEs can reference earlier ones
                for (name, subq) in ctes {
                    let result = subq.execute_with_ctx(ctx)?;
                    ctx.insert(name.clone(), result);
                }

                body.execute_with_ctx(ctx)
            }

            Query::Window { input, specs } => {
                let mut t = input.execute_with_ctx(ctx)?;
                apply_window_specs(&mut t, specs)?;
                Ok(t)
            }

            Query::Union { left, right } => {
                let left_table = left.execute_with_ctx(ctx)?;
                let right_table = right.execute_with_ctx(ctx)?;
                left_table.union(&right_table)
            }
            Query::Intersect { left, right } => {
                let left_table = left.execute_with_ctx(ctx)?;
                let right_table = right.execute_with_ctx(ctx)?;
                left_table.intersect(&right_table)
            }
            Query::Except { left, right } => {
                let left_table = left.execute_with_ctx(ctx)?;
                let right_table = right.execute_with_ctx(ctx)?;
                left_table.except(&right_table)
            }
        }
    }
}

fn apply_window_specs(table: &mut Table, specs: &[WindowSpec]) -> Result<(), String> {
    for spec in specs {
        let computed = compute_window_column(&table.rows, spec)?;

        if computed.len() != table.rows.len() {
            return Err("Window computation produced wrong number of rows".to_string());
        }

        for (row, val) in table.rows.iter_mut().zip(computed.into_iter()) {
            row.push(val);
        }

        // Append column definition for the computed window output
        let dtype = match &spec.func {
            WindowFunction::RowNumber
            | WindowFunction::Rank
            | WindowFunction::DenseRank => DataType::BigInt,

            WindowFunction::Lag { col_idx, .. } | WindowFunction::Lead { col_idx, .. } => {
                let src_col = table
                    .columns
                    .get(*col_idx)
                    .ok_or_else(|| format!("Window function references invalid column index: {}", col_idx))?;
                src_col.datatype.clone()
            }
        };

        table.columns.push(Column {
            name: spec.output_name.clone(),
            datatype: dtype,
            options: vec![],
        });
    }

    Ok(())
}

fn compute_window_column(rows: &[Vec<Value>], spec: &WindowSpec) -> Result<Vec<Value>, String> {
    // Build partitions: key -> list of row indices
    let mut partitions: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();

    for (i, row) in rows.iter().enumerate() {
        let key = spec
            .over
            .partition_by
            .iter()
            .map(|&idx| row.get(idx).cloned().unwrap_or(Value::NULL))
            .collect::<Vec<_>>();
        partitions.entry(key).or_default().push(i);
    }

    let mut output = vec![Value::NULL; rows.len()];

    for (_key, mut idxs) in partitions {
        // Sort indices within the partition by ORDER BY
        sort_partition(&mut idxs, rows, &spec.over.order_by);

        match &spec.func {
            WindowFunction::RowNumber => {
                for (pos, row_idx) in idxs.iter().enumerate() {
                    // 1-based
                    output[*row_idx] = Value::BigInt((pos as i64) + 1);
                }
            }

            WindowFunction::Rank => {
                // RANK: gaps on ties
                let mut rank: i64 = 1;
                let mut i = 0usize;
                while i < idxs.len() {
                    let start = i;
                    let current_idx = idxs[i];
                    let mut end = i + 1;

                    while end < idxs.len() && order_keys_equal(rows, current_idx, idxs[end], &spec.over.order_by) {
                        end += 1;
                    }

                    for j in start..end {
                        output[idxs[j]] = Value::BigInt(rank);
                    }

                    // jump by group size (gaps)
                    rank += (end - start) as i64;
                    i = end;
                }
            }

            WindowFunction::DenseRank => {
                // DENSE_RANK: no gaps on ties
                let mut rank: i64 = 1;
                let mut i = 0usize;
                while i < idxs.len() {
                    let start = i;
                    let current_idx = idxs[i];
                    let mut end = i + 1;

                    while end < idxs.len() && order_keys_equal(rows, current_idx, idxs[end], &spec.over.order_by) {
                        end += 1;
                    }

                    for j in start..end {
                        output[idxs[j]] = Value::BigInt(rank);
                    }

                    rank += 1;
                    i = end;
                }
            }

            WindowFunction::Lag { col_idx, offset } => {
                for (pos, row_idx) in idxs.iter().enumerate() {
                    if pos < *offset {
                        output[*row_idx] = Value::NULL;
                    } else {
                        let src_row_idx = idxs[pos - *offset];
                        output[*row_idx] = rows[src_row_idx]
                            .get(*col_idx)
                            .cloned()
                            .unwrap_or(Value::NULL);
                    }
                }
            }

            WindowFunction::Lead { col_idx, offset } => {
                for (pos, row_idx) in idxs.iter().enumerate() {
                    let src_pos = pos + *offset;
                    if src_pos >= idxs.len() {
                        output[*row_idx] = Value::NULL;
                    } else {
                        let src_row_idx = idxs[src_pos];
                        output[*row_idx] = rows[src_row_idx]
                            .get(*col_idx)
                            .cloned()
                            .unwrap_or(Value::NULL);
                    }
                }
            }
        }
    }

    Ok(output)
}

fn sort_partition(idxs: &mut [usize], rows: &[Vec<Value>], order_by: &[(usize, OrderDirection)]) {
    idxs.sort_by(|&a, &b| {
        for &(col_idx, dir) in order_by {
            let va = rows[a].get(col_idx).unwrap_or(&Value::NULL);
            let vb = rows[b].get(col_idx).unwrap_or(&Value::NULL);

            let mut cmp = va.cmp(vb);
            if let OrderDirection::Desc = dir {
                cmp = cmp.reverse();
            }
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn order_keys_equal(rows: &[Vec<Value>], ia: usize, ib: usize, order_by: &[(usize, OrderDirection)]) -> bool {
    for &(col_idx, _dir) in order_by {
        let va = rows[ia].get(col_idx).unwrap_or(&Value::NULL);
        let vb = rows[ib].get(col_idx).unwrap_or(&Value::NULL);
        if va != vb {
            return false;
        }
    }
    true
}