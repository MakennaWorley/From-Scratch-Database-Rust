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
    JSON, //Storing JSON inside tables
    Generated, //Combinations from other columns in table (a+b) Stored (think beastmodes from DOMO)
    Hashed, //Data unsearchable, treated like its not there unless user can unhash it
}
