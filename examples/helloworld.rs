use std::collections::BTreeSet;
use std::io::prelude::*;

use rustyrocks::{AssociateMergeable, DBIter, MergeableDB, Serializable, StaticDeserialize};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct BSet<T: std::cmp::Ord>(BTreeSet<T>);

impl StaticDeserialize for BSet<String> {
    type Error = bincode::Error;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes)
    }
}

impl Serializable for &BSet<String> {
    type Bytes = Vec<u8>;

    fn serialize(self) -> Self::Bytes {
        bincode::serialize(&self).unwrap()
    }
}

impl AssociateMergeable for BSet<String> {
    fn merge(&mut self, other: &mut Self) {
        self.0.append(&mut other.0)
    }

    fn handle_deser_error(key: &[u8], buf: &[u8], err: Self::Error) -> Option<Self> {
        panic!(
            "Error deserializing. key: {:?}; error: {}; bytes: {:?}",
            key, err, buf
        )
    }

    fn into_bytes(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

fn main() -> Result<(), failure::Error> {
    let path = "words.db";

    // NB: db is automatically closed at end of lifetime
    let db: MergeableDB<&str, BSet<String>, &BSet<String>> = MergeableDB::new(path)?;

    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let splits: Vec<_> = line.trim().splitn(2, ' ').collect();
        if splits.len() != 2 {
            println!("Could not split '{}'", line);
            continue;
        }
        let word = splits[0];
        let value = splits[1];
        let mut new_set = BSet(BTreeSet::new());
        new_set.0.insert(value.to_string());
        // db.put(word, value).unwrap();
        if let Err(e) = db.merge(&word.to_string(), &new_set) {
            println!("Ignoring merge err: {}", e);
        }

        // let uvec = match db.get(word) {
        //     Ok(Some(value)) => value,
        //     Ok(None) => {
        //         println!("value not found");
        //         continue;
        //     }
        //     Err(e) => {
        //         println!("operational problem encountered: {}", e);
        //         continue;
        //     }
        // };

        // // let r = db.get(word).unwrap().unwrap();
        // let values = deserialize(uvec.as_ref());
        // println!("{}", word);
        // for v in values {
        //     println!("  {}", v);
        // }
    }

    let iter: DBIter<String, _> = db.db_iter(); // Always iterates forward
    for kv in iter {
        let (k, v) = kv?;
        print!("{}:", k);
        for r in v.0 {
            print!(" {}", r);
        }
        println!();
    }

    Ok(())
}
