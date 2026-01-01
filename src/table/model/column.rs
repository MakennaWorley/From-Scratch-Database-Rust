use crate::table::model::datatype::DataType;
use crate::table::model::value::Value;

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