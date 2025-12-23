#![allow(dead_code)]
struct Field {
    name: String,
    dtype: DataType,
}

enum DataType {
    I64,
    U32,
    F64,
}

struct Schema {
    fields: Vec<Field>,
}
