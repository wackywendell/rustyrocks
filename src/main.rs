use rocksdb::DB;
use std::io;
use std::io::prelude::*;

fn main() {
    let path = "words.db";
    let db = DB::open_default(path).unwrap();

    // NB: db is automatically closed at end of lifetime
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let splits: Vec<_> = line.trim().splitn(2, " ").collect();
        if splits.len() < 2 {
            println!("Could not split '{}'", line);
            continue;
        }
        let word = splits[0];
        let value = splits[1];
        db.put(word, value).unwrap();
    }

    println!("Hello, world!");
}
