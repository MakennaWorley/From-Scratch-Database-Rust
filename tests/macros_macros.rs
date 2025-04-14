#[cfg(test)]
mod tests {
    use database::filter;
    use database::table::data::{FilterExpr, Value};

    #[test]
    fn test_filter_eq() {
        let expr = filter!(col "age" == Value::Int(30));
        match expr {
            FilterExpr::Eq(ref col, ref val) => {
                assert_eq!(col, "age");
                match val {
                    Value::Int(i) => assert_eq!(*i, 30),
                    _ => panic!("Expected Value::Int, got a different variant"),
                }
            },
            _ => panic!("Expected FilterExpr::Eq variant"),
        }
    }

    #[test]
    fn test_filter_ne() {
        let expr = filter!(col "name" != Value::Varchar("John".to_string()));
        match expr {
            FilterExpr::Ne(ref col, ref val) => {
                assert_eq!(col, "name");
                match val {
                    Value::Varchar(ref s) => assert_eq!(s, "John"),
                    _ => panic!("Expected Value::Varchar, got a different variant"),
                }
            },
            _ => panic!("Expected FilterExpr::Ne variant"),
        }
    }

    #[test]
    fn test_filter_gt() {
        let expr = filter!(col "score" > Value::Int(50));
        match expr {
            FilterExpr::Gt(ref col, ref val) => {
                assert_eq!(col, "score");
                match val {
                    Value::Int(i) => assert_eq!(*i, 50),
                    _ => panic!("Expected Value::Int, got a different variant"),
                }
            },
            _ => panic!("Expected FilterExpr::Gt variant"),
        }
    }

    #[test]
    fn test_filter_lt() {
        let expr = filter!(col "score" < Value::Int(100));
        match expr {
            FilterExpr::Lt(ref col, ref val) => {
                assert_eq!(col, "score");
                match val {
                    Value::Int(i) => assert_eq!(*i, 100),
                    _ => panic!("Expected Value::Int, got a different variant"),
                }
            },
            _ => panic!("Expected FilterExpr::Lt variant"),
        }
    }

    #[test]
    fn test_filter_ge() {
        let expr = filter!(col "height" >= Value::Double(5.5));
        match expr {
            FilterExpr::Ge(ref col, ref val) => {
                assert_eq!(col, "height");
                match val {
                    Value::Double(d) => assert_eq!(*d, 5.5),
                    _ => panic!("Expected Value::Double, got a different variant"),
                }
            },
            _ => panic!("Expected FilterExpr::Ge variant"),
        }
    }

    #[test]
    fn test_filter_le() {
        let expr = filter!(col "weight" <= Value::Double(150.0));
        match expr {
            FilterExpr::Le(ref col, ref val) => {
                assert_eq!(col, "weight");
                match val {
                    Value::Double(d) => assert_eq!(*d, 150.0),
                    _ => panic!("Expected Value::Double, got a different variant"),
                }
            },
            _ => panic!("Expected FilterExpr::Le variant"),
        }
    }
}
