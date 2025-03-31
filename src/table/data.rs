use chrono::{NaiveDate, NaiveTime, NaiveDateTime};
use std::collections::{HashMap, BTreeMap};
use std::hash::{Hash, Hasher};
use std::mem;

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

#[derive(Debug, Clone)]
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

impl Value {
    fn variant_index(&self) -> u8 {
        match self {
            Value::Char(_) => 0,
            Value::Varchar(_) => 1,
            Value::Text(_) => 2,
            Value::Enum(_, _) => 3,
            Value::Set(_, _) => 4,
            Value::Boolean(_) => 5,
            Value::Int(_) => 6,
            Value::BigInt(_) => 7,
            Value::Float(_) => 8,
            Value::Double(_) => 9,
            Value::Date(_) => 10,
            Value::Time(_) => 11,
            Value::DateTime(_) => 12,
            Value::Null => 13,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Float(a), Float(b)) => a.to_bits() == b.to_bits(),
            (Double(a), Double(b)) => a.to_bits() == b.to_bits(),
            _ => mem::discriminant(self) == mem::discriminant(other) && {
                match (self, other) {
                    (Char(a), Char(b)) => a == b,
                    (Varchar(a), Varchar(b)) => a == b,
                    (Text(a), Text(b)) => a == b,
                    (Enum(a1, e1), Enum(a2, e2)) => a1 == a2 && e1 == e2,
                    (Set(s1, e1), Set(s2, e2)) => s1 == s2 && e1 == e2,
                    (Boolean(a), Boolean(b)) => a == b,
                    (Int(a), Int(b)) => a == b,
                    (BigInt(a), BigInt(b)) => a == b,
                    (Date(a), Date(b)) => a == b,
                    (Time(a), Time(b)) => a == b,
                    (DateTime(a), DateTime(b)) => a == b,
                    (Null, Null) => true,
                    _ => false,
                }
            }
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use Value::*;
        match (self, other) {
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Double(a), Double(b)) => a.partial_cmp(b),
            _ => Some(self.cmp(other))
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use Value::*;
        match (self, other) {
            (Float(a), Float(b)) => a.to_bits().cmp(&b.to_bits()),
            (Double(a), Double(b)) => a.to_bits().cmp(&b.to_bits()),
            (Char(a), Char(b)) => a.cmp(b),
            (Varchar(a), Varchar(b)) => a.cmp(b),
            (Text(a), Text(b)) => a.cmp(b),
            (Enum(a1, e1), Enum(a2, e2)) => (a1, e1).cmp(&(a2, e2)),
            (Set(s1, e1), Set(s2, e2)) => (s1, e1).cmp(&(s2, e2)),
            (Boolean(a), Boolean(b)) => a.cmp(b),
            (Int(a), Int(b)) => a.cmp(b),
            (BigInt(a), BigInt(b)) => a.cmp(b),
            (Date(a), Date(b)) => a.cmp(b),
            (Time(a), Time(b)) => a.cmp(b),
            (DateTime(a), DateTime(b)) => a.cmp(b),
            (Null, Null) => std::cmp::Ordering::Equal,
            _ => self.variant_index().cmp(&other.variant_index()),
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use Value::*;
        mem::discriminant(self).hash(state);
        match self {
            Char(c) => c.hash(state),
            Varchar(s) => s.hash(state),
            Text(s) => s.hash(state),
            Enum(val, all) => {
                val.hash(state);
                all.hash(state);
            }
            Set(vals, all) => {
                vals.hash(state);
                all.hash(state);
            }
            Boolean(b) => b.hash(state),
            Int(i) => i.hash(state),
            BigInt(i) => i.hash(state),
            Float(f) => f.to_bits().hash(state),
            Double(f) => f.to_bits().hash(state),
            Date(d) => d.hash(state),
            Time(t) => t.hash(state),
            DateTime(dt) => dt.hash(state),
            Null => (),
        }
    }
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
    pub rows: Vec<Vec<Value>>,
    pub primary_key: Option<Vec<String>>,
    pub indexes: HashMap<String, IndexType>,
    pub transaction_backup: Option<Vec<Vec<Value>>>,
}

#[derive(Debug, Clone)]
pub enum FilterExpr {
    Eq(String, Value),
    Gt(String, Value),
    Lt(String, Value),
    Ge(String, Value),
    Le(String, Value),
    Ne(String, Value),
}

pub struct View<'a> {
    pub name: String,
    pub builder: Box<dyn Fn() -> Result<Table, String> + 'a>,
}

#[derive(Debug, Clone)]
pub enum IndexType {
    Hash(HashMap<Value, Vec<usize>>),
    BTree(BTreeMap<Value, Vec<usize>>),
}

#[derive(Debug)]
pub enum AggregationResult {
    Sum(f64),
    Avg(f64),
    Count(usize),
    Min(Value),
    Max(Value),
}
