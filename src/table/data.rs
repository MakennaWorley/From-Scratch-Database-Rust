use chrono::{NaiveDate, NaiveTime, NaiveDateTime};

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Char, //Single character
    Varchar, //Multiple characters
    Text, //Longer varchars
    Enum, //Single object
    Set, //0-64 objects
    Boolean, //True or False
    Int, //Integers
    BigInt, //Larger integers
    Float, //Numbers with decimals
    Double, //Larger numbers with decimals
    Date, //YYYY-MM-DD
    Time, //HH:MM:SS
    DateTime, //YYYY-MM-DD HH:MM:SS
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Char(char),
    Varchar(String),
    Text(String),
    Enum(String, Vec<String>),
    Set(Vec<String>, Vec<String>),
    Boolean(bool),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    Null
}

#[derive(Debug, Clone, PartialEq)]
pub enum Options {
    Unique,
    NotNull,
    FK(String),
    Check(String),
    Default(Value),
    Autoincrement
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub options: Vec<Options>,
}

pub type DBRows = Vec<Value>;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<DBRows>,
    pub primary_key: Option<Vec<String>>,
}
