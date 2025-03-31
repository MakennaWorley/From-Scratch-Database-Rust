use crate::table::data::{Table, Value};
pub use crate::table::data::FilterExpr;

impl FilterExpr {
    pub fn value(&self) -> &Value {
        match self {
            FilterExpr::Eq(_, v)
            | FilterExpr::Ne(_, v)
            | FilterExpr::Gt(_, v)
            | FilterExpr::Lt(_, v)
            | FilterExpr::Ge(_, v)
            | FilterExpr::Le(_, v) => v,
        }
    }

    pub fn to_predicate(&self, table: &Table) -> Box<dyn Fn(&Vec<Value>) -> bool + '_> {
        let col_index = table.columns.iter().position(|c| c.name == *self.column()).unwrap();
        match self {
            FilterExpr::Eq(_, v) => {
                let val = v.clone();
                Box::new(move |row| row[col_index] == val)
            }
            FilterExpr::Ne(_, v) => {
                let val = v.clone();
                Box::new(move |row| row[col_index] != val)
            }
            FilterExpr::Gt(_, v) => {
                let val = v.clone();
                Box::new(move |row| row[col_index] > val)
            }
            FilterExpr::Lt(_, v) => {
                let val = v.clone();
                Box::new(move |row| row[col_index] < val)
            }
            FilterExpr::Ge(_, v) => {
                let val = v.clone();
                Box::new(move |row| row[col_index] >= val)
            }
            FilterExpr::Le(_, v) => {
                let val = v.clone();
                Box::new(move |row| row[col_index] <= val)
            }
        }
    }

    pub fn column(&self) -> &String {
        match self {
            FilterExpr::Eq(col, _)
            | FilterExpr::Ne(col, _)
            | FilterExpr::Gt(col, _)
            | FilterExpr::Lt(col, _)
            | FilterExpr::Ge(col, _)
            | FilterExpr::Le(col, _) => col,
        }
    }
}
