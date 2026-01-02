use crate::table::model::{DataType, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Options {
    PrimaryKey,
    ForeignKey(String),
    Unique,
    NotNull,
    Check(String),
    Default(Value),
    AutoIncrement,
    OnDelete,
    OnUpdate,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub options: Vec<Options>,
}

impl Column {
    pub fn has_option(col: &Column, f: impl Fn(&Options) -> bool) -> bool {
        col.options.iter().any(f)
    }

    pub fn remove_options_matching(col: &mut Column, f: impl Fn(&Options) -> bool) {
        col.options.retain(|opt| !f(opt));
    }
}