use std::collections::BTreeSet;
use std::io::prelude::*;

use rustyrocks::{AssociateMergeable, MergeableDB, StaticDeserialize, StaticSerialize};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct BSet<T>(BTreeSet<T>);

// impl StaticDeserialize for String {
//     type Error = bincode::Error;
//     fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error> {
//         return bincode::deserialize(bytes);
//     }
// }

impl StaticDeserialize for BSet<String> {
    type Error = bincode::Error;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes)
    }
}

impl StaticSerialize for BSet<String> {
    fn serialize(&self) -> &[u8] {
        bincode::serialize(bytes)
    }
}

impl AssociateMergeable for BSet<String> {
    fn merge(&mut self, other: &mut Self) {
        self.0.append(other.0)
    }
}

fn main() -> Result<(), failure::Error> {
    let path = "words.db";

    // NB: db is automatically closed at end of lifetime
    let db: MergeableDB<String, BTreeSet<String>> = MergeableDB::new(path)?;

    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let splits: Vec<_> = line.trim().splitn(2, " ").collect();
        if splits.len() != 2 {
            println!("Could not split '{}'", line);
            continue;
        }
        let word = splits[0];
        let value = splits[1];
        let mut new_set = BTreeSet::new();
        new_set.insert(value.to_string());
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

    let iter = db.typed_db.into_iter(); // Always iterates forward
    for kv in iter {
        let (k, v) = kv?;
        print!("{}:", k);
        for r in v {
            print!(" {}", r);
        }
        print!("\n");
    }

    Ok(())
}
