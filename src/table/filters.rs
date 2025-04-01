use crate::table::data::{Table, Value};
pub use crate::table::data::FilterExpr;

impl FilterExpr {
    pub fn value(&self) -> Option<&Value> {
        match self {
            FilterExpr::Eq(_, v)
            | FilterExpr::Ne(_, v)
            | FilterExpr::Gt(_, v)
            | FilterExpr::Lt(_, v)
            | FilterExpr::Ge(_, v)
            | FilterExpr::Le(_, v) => Some(v),
            FilterExpr::Like(_, _)
            | FilterExpr::In(_, _)
            | FilterExpr::Between(_, _, _)
            | FilterExpr::IsNull(_)
            | FilterExpr::IsNotNull(_) => None,
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
            FilterExpr::Like(_, pattern) => {
                let pat = pattern.clone();
                Box::new(move |row| {
                    let val_str = row[col_index].to_display_string();
                    // A very basic LIKE implementation: support wildcard '%' at beginning/end.
                    if pat.starts_with('%') && pat.ends_with('%') {
                        let inner = pat.trim_matches('%');
                        val_str.contains(inner)
                    } else if pat.starts_with('%') {
                        let inner = pat.trim_start_matches('%');
                        val_str.ends_with(inner)
                    } else if pat.ends_with('%') {
                        let inner = pat.trim_end_matches('%');
                        val_str.starts_with(inner)
                    } else {
                        val_str == pat
                    }
                })
            }
            FilterExpr::In(_, list) => {
                let list_clone = list.clone();
                Box::new(move |row| {
                    list_clone.iter().any(|item| row[col_index] == *item)
                })
            }
            FilterExpr::Between(_, low, high) => {
                let low = low.clone();
                let high = high.clone();
                Box::new(move |row| row[col_index] >= low && row[col_index] <= high)
            }
            FilterExpr::IsNull(_) => {
                Box::new(move |row| matches!(row[col_index], Value::Null))
            }
            FilterExpr::IsNotNull(_) => {
                Box::new(move |row| !matches!(row[col_index], Value::Null))
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
            | FilterExpr::Le(col, _)
            | FilterExpr::Like(col, _)
            | FilterExpr::In(col, _)
            | FilterExpr::Between(col, _, _)
            | FilterExpr::IsNull(col)
            | FilterExpr::IsNotNull(col) => col,
        }
    }
}
