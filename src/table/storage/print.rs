use crate::table::model::{Table, Value};

impl Table {
    pub fn print_table(&self) {
        println!("\nTable: {}", self.name);
        for col in &self.columns {
            print!("| {:<15} ", col.name);
        }
        println!("|");

        for row in &self.rows {
            for val in row {
                print!("| {:<15} ", val.to_string());
            }
            println!("|");
        }
    }

    pub fn print_join_results(
        left_headers: &[String],
        right_headers: &[String],
        results: &[(Vec<&Value>, Vec<&Value>)],
    ) {
        let total_headers = left_headers
            .iter()
            .chain(right_headers.iter())
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        println!("{}", total_headers.join(" | "));

        for (left, right) in results {
            let row = left
                .iter()
                .chain(right.iter())
                .map(|v| v.to_string())
                .collect::<Vec<_>>();
            println!("{}", row.join(" | "));
        }
    }
}