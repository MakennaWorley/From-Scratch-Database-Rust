use crate::table::model::{DataType, };
use chrono::{NaiveDate, NaiveTime, NaiveDateTime};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem;

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
    JSON(String),
    Generated(f64),
    Hashed(String),
    NULL
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
            Value::JSON(_) => 13,
            Value::Generated(_) => 14,
            Value::Hashed(_) => 15,
            Value::NULL => 16,
        }
    }
    
    pub fn from_date_str(s: &str) -> Result<Self, chrono::ParseError> {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").map(Value::Date)
    }

    pub fn from_time_str(s: &str) -> Result<Self, chrono::ParseError> {
        NaiveTime::parse_from_str(s, "%H:%M:%S").map(Value::Time)
    }

    pub fn from_datetime_str(s: &str) -> Result<Self, chrono::ParseError> {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(Value::DateTime)
    }

    pub fn is_type_compatible_with(&self, dtype: &DataType) -> bool {
        match (self, dtype) {
            (Value::Char(_), DataType::Char) => true,
            (Value::Varchar(_), DataType::Varchar) => true,
            (Value::Text(_), DataType::Text) => true,
            (Value::Enum(_, _), DataType::Enum) => true,
            (Value::Set(_, _), DataType::Set) => true,
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Int(_), DataType::Int) => true,
            (Value::BigInt(_), DataType::BigInt) => true,
            (Value::Float(_), DataType::Float) => true,
            (Value::Double(_), DataType::Double) => true,
            (Value::Date(_), DataType::Date) => true,
            (Value::Time(_), DataType::Time) => true,
            (Value::DateTime(_), DataType::DateTime) => true,
            (Value::JSON(_), DataType::JSON) => true,
            (Value::Generated(_), DataType::Generated) => true,
            (Value::Hashed(_), DataType::Hashed) => true,
            (Value::NULL, _) => true, // null is allowed type-wise (check nullability separately)
            _ => false,
        }
    }

    pub fn to_display_string(&self) -> String {
        match self {
            Value::Char(c) => c.to_string(),
            Value::Varchar(s) | Value::Text(s) => s.clone(),
            Value::Enum(val, _) => val.clone(),
            Value::Set(vals, _) => format!("{{{}}}", vals.join(",")),
            Value::Boolean(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::BigInt(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Double(f) => f.to_string(),
            Value::Date(d) => d.to_string(),
            Value::Time(t) => t.to_string(),
            Value::DateTime(dt) => dt.to_string(),
            Value::JSON(jsonString) => jsonString.to_string(),
            Value::Generated(g) => g.to_string(),
            Value::Hashed(h) => "Hashed".to_string(),
            Value::NULL => "NULL".to_string(),
        }
    }

    pub fn from_str(s: &str, dtype: &DataType) -> Result<Self, String> {
        let unquoted = s.trim().trim_matches('"');

        match dtype {
            DataType::Char => {
                if unquoted.len() == 1 {
                    Ok(Value::Char(unquoted.chars().next().unwrap()))
                } else {
                    Err("Expected a single character".to_string())
                }
            }
            DataType::Varchar | DataType::Text => Ok(Value::Varchar(unquoted.to_string())),
            DataType::Boolean => match unquoted {
                "true" => Ok(Value::Boolean(true)),
                "false" => Ok(Value::Boolean(false)),
                _ => Err("Invalid boolean value".to_string()),
            },
            DataType::Int => unquoted.parse().map(Value::Int).map_err(|_| "Invalid int".to_string()),
            DataType::BigInt => unquoted.parse().map(Value::BigInt).map_err(|_| "Invalid bigint".to_string()),
            DataType::Float => unquoted.parse().map(Value::Float).map_err(|_| "Invalid float".to_string()),
            DataType::Double => unquoted.parse().map(Value::Double).map_err(|_| "Invalid double".to_string()),
            DataType::Date => Value::from_date_str(unquoted).map_err(|e| format!("Invalid date: {e}")),
            DataType::Time => Value::from_time_str(unquoted).map_err(|e| format!("Invalid time: {e}")),
            DataType::DateTime => Value::from_datetime_str(unquoted).map_err(|e| format!("Invalid datetime: {e}")),
            DataType::Enum => Ok(Value::Enum(unquoted.to_string(), vec![])),
            DataType::Set => {
                let inner = unquoted.trim_matches(|c| c == '{' || c == '}');
                let items = if inner.is_empty() {
                    vec![]
                } else {
                    inner.split(',').map(|s| s.trim().to_string()).collect()
                };
                Ok(Value::Set(items, vec![]))
            },
            DataType::JSON => Ok(Value::JSON(unquoted.to_string())),
            DataType::Generated => unquoted
                .parse::<f64>()
                .map(Value::Generated)
                .map_err(|_| "Invalid generated numeric literal".to_string()),
            DataType::Hashed => Ok(Value::Hashed(unquoted.to_string()))
        }
    }

    pub fn value_matches_type(val: &Value, dtype: &DataType) -> bool {
        match (val, dtype) {
            (Value::Char(_), DataType::Char) => true,
            (Value::Varchar(_), DataType::Varchar) => true,
            (Value::Text(_), DataType::Text) => true,
            (Value::Enum(_, _), DataType::Enum) => true,
            (Value::Set(_, _), DataType::Set) => true,
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Int(_), DataType::Int) => true,
            (Value::BigInt(_), DataType::BigInt) => true,
            (Value::Float(_), DataType::Float) => true,
            (Value::Double(_), DataType::Double) => true,
            (Value::Date(_), DataType::Date) => true,
            (Value::Time(_), DataType::Time) => true,
            (Value::DateTime(_), DataType::DateTime) => true,
            (Value::JSON(_), DataType::JSON) => true,
            (Value::Generated(_), DataType::Generated) => true,
            (Value::Hashed(_), DataType::Hashed) => true,
            (Value::NULL, _) => true, // Allow null everywhere for now
            _ => false,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Float(a), Float(b)) => a.to_bits() == b.to_bits(),
            (Double(a), Double(b)) => a.to_bits() == b.to_bits(),
            (Generated(a), Generated(b)) => a.to_bits() == b.to_bits(),
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
                    (JSON(a), JSON(b)) => a == b,
                    (Generated(a), Generated(b)) => a.to_bits() == b.to_bits(),
                    (Hashed(a), Hashed(b)) => a == b,
                    (NULL, NULL) => true,
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
            (Generated(a), Generated(b)) => a.partial_cmp(b),
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
            (JSON(a), JSON(b)) => a.cmp(b),
            (Generated(a), Generated(b)) => a.to_bits().cmp(&b.to_bits()),
            (Hashed(a), Hashed(b)) => a.cmp(b),
            (NULL, NULL) => std::cmp::Ordering::Equal,
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
            JSON(s) => s.hash(state),
            Generated(g) => g.to_bits().hash(state),
            Hashed(h) => h.hash(state),
            NULL => (),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::NULL => write!(f, "NULL"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(x) => write!(f, "{x}"),
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Text(s) => write!(f, "{s}"),
            Value::Date(d) => write!(f, "{d}"),
            Value::Time(t) => write!(f, "{t}"),
            Value::DateTime(dt) => write!(f, "{dt}"),
            Value::JSON(s) => write!(f, "{s}"),
            Value::Generated(g) => write!(f, "{g}"),
            Value::Hashed(_) => write!(f, "Hashed"),
            _ => todo!()
        }
    }
}
