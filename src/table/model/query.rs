use crate::table::model::value::Value;
use crate::table::model::table::Table;
use std::collections::HashMap;

#[derive(Debug)]
pub enum AggregationResult {
    Sum(f64),
    Avg(f64),
    Count(usize),
    Min(Value),
    Max(Value),
}

pub enum Query {
    Select {
        table: Table,
        filter: Option<Box<dyn Fn(&Vec<Value>) -> bool>>,
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
            Query::Union { left, right } => {
                let left_table = left.execute()?;
                let right_table = right.execute()?;
                left_table.union(&right_table)
            }
            Query::Intersect { left, right } => {
                let left_table = left.execute()?;
                let right_table = right.execute()?;
                left_table.intersect(&right_table)
            }
            Query::Except { left, right } => {
                let left_table = left.execute()?;
                let right_table = right.execute()?;
                left_table.except(&right_table)
            }
        }
    }

    // need to have the row_number, rank/density, lag/lead, over
    // add temporary named subqueries, WITH ___ AS or WITH RECURSIVE
}