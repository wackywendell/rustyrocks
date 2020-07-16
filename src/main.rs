use std::collections::BTreeSet;
use std::io::prelude::*;

use rustyrocks::{AssociateMergeable, MergeableDB, StaticDeserialize};

struct BTSet(BTreeSet<String>);

impl StaticDeserialize for BTSet {
    type Error = bincode::Error;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error> {
        Ok(BTSet(bincode::deserialize(bytes)?))
    }
}

// impl StaticDeserialize for String {
//     type Error = bincode::Error;
//     fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error> {
//         return bincode::deserialize(bytes);
//     }
// }

impl AssociateMergeable for BTSet {
    fn merge(&mut self, other: &mut Self) {
        self.0.append(&mut other.0)
    }
}

fn main() -> Result<(), failure::Error> {
    let path = "words.db";

    // NB: db is automatically closed at end of lifetime
    let db: MergeableDB<String, BTSet> = MergeableDB::new(path)?;

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
