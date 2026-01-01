use crate::table::model::table::Table;

pub struct View<'a> {
    pub name: String,
    pub builder: Box<dyn Fn() -> Result<Table, String> + 'a>,
}