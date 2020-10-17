use std::io;
use std::io::prelude::*;

use itertools::kmerge;
use itertools::Itertools;
use rmp::decode::read_str_from_slice;
use rmp::encode::write_str;
use rocksdb::{IteratorMode, MergeOperands, Options, DB};

fn concat_merge(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(operands.size_hint().0);

    let mut existing: Vec<&str> = vec![];
    if let Some(mut unparsed) = existing_val {
        while let Ok((chunk, tail)) = read_str_from_slice(unparsed) {
            existing.push(chunk);
            unparsed = tail;
        }
    }

    let mut merged_inputs: Vec<&str> = vec![];
    for mut unparsed in operands {
        while let Ok((chunk, tail)) = read_str_from_slice(unparsed) {
            merged_inputs.push(chunk);
            unparsed = tail;
        }
    }

    merged_inputs.sort_unstable();
    for s in kmerge(vec![existing, merged_inputs]).dedup() {
        match write_str(&mut result, s) {
            Ok(()) => {}
            Err(e) => println!("Ignoring err {}", e),
        }
    }

    Some(result)
}

fn serialize_single(s: &str) -> Vec<u8> {
    let mut r: Vec<u8> = Vec::with_capacity(s.len());
    if let Err(e) = write_str(&mut r, s) {
        println!("Ignoring merge err: {}", e);
    }
    r
}

fn deserialize(s: &[u8]) -> Vec<&str> {
    let mut result: Vec<&str> = vec![];
    let mut unparsed = s;
    while let Ok((chunk, tail)) = read_str_from_slice(unparsed) {
        result.push(chunk);
        unparsed = tail;
    }
    result
}

fn main() {
    let path = "words.db";

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_merge_operator("test operator", concat_merge, None);
    let db = DB::open(&opts, path).unwrap();

    // NB: db is automatically closed at end of lifetime
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let splits: Vec<_> = line.trim().splitn(2, ' ').collect();
        if splits.len() < 2 {
            println!("Could not split '{}'", line);
            continue;
        }
        let word = splits[0];
        let value = splits[1];
        // db.put(word, value).unwrap();
        if let Err(e) = db.merge(word, serialize_single(value)) {
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

    let iter = db.iterator(IteratorMode::Start); // Always iterates forward
    for (key, value) in iter {
        let k = std::str::from_utf8(key.as_ref()).unwrap();
        print!("{}:", k);
        let results = deserialize(value.as_ref());
        for r in results {
            print!(" {}", r);
        }
        println!();
    }
}
