#[macro_export]
macro_rules! filter {
    (col $col_name:literal == $val:expr) => {
        FilterExpr::Eq($col_name.to_string(), $val.clone())
    };
    (col $col_name:literal != $val:expr) => {
        FilterExpr::Ne($col_name.to_string(), $val.clone())
    };
    (col $col_name:literal > $val:expr) => {
        FilterExpr::Gt($col_name.to_string(), $val.clone())
    };
    (col $col_name:literal < $val:expr) => {
        FilterExpr::Lt($col_name.to_string(), $val.clone())
    };
    (col $col_name:literal >= $val:expr) => {
        FilterExpr::Ge($col_name.to_string(), $val.clone())
    };
    (col $col_name:literal <= $val:expr) => {
        FilterExpr::Le($col_name.to_string(), $val.clone())
    };
}
