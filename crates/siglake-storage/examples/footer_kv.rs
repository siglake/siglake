//! Dump a Parquet file's footer key_value_metadata keys (+ sizes), row count,
//! and row-group count. Scratch diagnostic: `cargo run -p siglake-storage
//! --example footer_kv -- <file.parquet>`.

use parquet::file::reader::{FileReader, SerializedFileReader};

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: footer_kv <file.parquet>");
    let file = std::fs::File::open(&path).expect("open");
    let reader = SerializedFileReader::new(file).expect("parquet open");
    let md = reader.metadata();
    let fmd = md.file_metadata();
    println!("rows: {}", fmd.num_rows());
    println!("row_groups: {}", md.num_row_groups());
    match fmd.key_value_metadata() {
        None => println!("footer KV: <none>"),
        Some(kv) => {
            for e in kv {
                println!(
                    "kv: {} => {} bytes",
                    e.key,
                    e.value.as_deref().map_or(0, str::len)
                );
            }
        }
    }
}
