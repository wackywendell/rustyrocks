use std::io;
use std::io::prelude::*;

use itertools::kmerge;
use itertools::Itertools;
use rmp::decode::read_str_from_slice;
use rmp::encode::write_str;

use rocksdb::{IteratorMode, MergeOperands, Options, DB};
use serde::{Deserialize, Serialize};
pub struct PinnedItem<'de, V: ?Sized + Deserialize<'de>> {
    phantom: std::marker::PhantomData<V>,
    pinned_slice: rocksdb::DBPinnableSlice<'de>,
}

impl<'de, V: ?Sized + Deserialize<'de>> PinnedItem<'de, V> {
    pub fn into(&'de self) -> Result<V, failure::Error> {
        Ok(bincode::deserialize(self.pinned_slice.as_ref())?)
    }
}

pub struct TypedDB<K: ?Sized, V: ?Sized> {
    phantom_key: std::marker::PhantomData<K>,
    phantom_value: std::marker::PhantomData<V>,
    db: DB,
}

impl<K: Serialize + ?Sized, V: Serialize + ?Sized> TypedDB<K, V> {
    pub fn new(db: DB) -> Self {
        TypedDB {
            phantom_key: std::marker::PhantomData,
            phantom_value: std::marker::PhantomData,
            db: db,
        }
    }

    pub fn put(&self, k: &K, v: &V) -> Result<(), failure::Error> {
        let kb = bincode::serialize(k)?;
        let vb = bincode::serialize(v)?;

        self.db.put(kb, vb)?;
        Ok(())
    }
}

impl<'a, K: Serialize + ?Sized, V: Deserialize<'a> + ?Sized> TypedDB<K, V> {
    pub fn get(&'a self, k: &'a K) -> Result<Option<PinnedItem<'a, V>>, failure::Error> {
        let kb = bincode::serialize(k)?;
        let vb_opt: Option<rocksdb::DBPinnableSlice<'a>> = self.db.get_pinned(kb)?;
        let vb: rocksdb::DBPinnableSlice<'a> = match vb_opt {
            None => return Ok(None),
            Some(vb) => vb,
        };

        let pinned_item = PinnedItem {
            phantom: std::marker::PhantomData,
            pinned_slice: vb,
        };

        return Ok(Some(pinned_item));
    }
}

pub trait AssociateMergeable: Sized {
    type Error: std::fmt::Display;
    fn merge(&mut self, other: &Self) -> Self;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error>;
}

fn unwrap_or_log<V, E: std::fmt::Display>(r: Result<V, E>) -> Option<V> {
    match r {
        Ok(v) => Some(v),
        Err(e) => {
            println!("Error! {}", e);
            None
        }
    }
}

fn merge<V: Serialize + AssociateMergeable>(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let mut merged: Option<V> =
        existing_val.and_then(|unparsed| unwrap_or_log(V::deserialize(unparsed)));

    for unparsed in operands {
        let deser: Option<V> = unwrap_or_log(V::deserialize(unparsed));

        merged = match (merged, deser) {
            (None, None) => None,
            (Some(m), None) => Some(m),
            (None, Some(d)) => Some(d),
            (Some(ref mut m), Some(ref d)) => Some(m.merge(d)),
        };
    }

    merged.and_then(|value| unwrap_or_log(bincode::serialize(&value)))
}

pub struct MergeableDB<K: ?Sized, V: ?Sized> {
    typed_db: TypedDB<K, V>,
}

impl<'a, K: Serialize + ?Sized, V: Deserialize<'a> + ?Sized> MergeableDB<K, V> {
    pub fn get(&'a self, k: &'a K) -> Result<Option<PinnedItem<'a, V>>, failure::Error> {
        self.typed_db.get(k)
    }
}

impl<K: Serialize + ?Sized, V: Serialize + ?Sized> MergeableDB<K, V> {
    pub fn put(&self, k: &K, v: &V) -> Result<(), failure::Error> {
        self.typed_db.put(k, v)
    }
}

impl<'a, K: Serialize + ?Sized, V: Serialize + Deserialize<'a> + AssociateMergeable + ?Sized>
    MergeableDB<K, V>
{
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, failure::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        opts.set_merge_operator("test operator", merge::<V>, None);
        let db = DB::open(&opts, path)?;

        Ok(MergeableDB {
            typed_db: TypedDB::new(db),
        })
    }
}


fn concat_merge(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(operands.size_hint().0);

    let mut existing: Vec<&str> = vec![];
    existing_val.map(|mut unparsed| {
        while let Ok((chunk, tail)) = read_str_from_slice(unparsed) {
            existing.push(chunk);
            unparsed = tail;
        }
    });

    let mut merged_inputs: Vec<&str> = vec![];
    for mut unparsed in operands {
        while let Ok((chunk, tail)) = read_str_from_slice(unparsed) {
            merged_inputs.push(chunk);
            unparsed = tail;
        }
    }

    merged_inputs.sort();
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
    return r;
}

fn deserialize(s: &[u8]) -> Vec<&str> {
    let mut result: Vec<&str> = vec![];
    let mut unparsed = s;
    while let Ok((chunk, tail)) = read_str_from_slice(unparsed) {
        result.push(chunk);
        unparsed = tail;
    }
    return result;
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
        let splits: Vec<_> = line.trim().splitn(2, " ").collect();
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
        print!("\n");
    }
}
