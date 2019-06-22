use std::io;
use std::io::prelude::*;

use serde::{Deserialize, Serialize, Serializer};

use itertools::kmerge;
use itertools::Itertools;
use rmp::decode::read_str_from_slice;
use rmp::encode::write_str;
use rocksdb::{IteratorMode, MergeOperands, Options, DB};

pub trait Value<'de>: Deserialize<'de> + Serialize {
    fn merge(&mut self, other: &Self);
}

pub trait Key<'de>: Deserialize<'de> + Serialize {}

trait SerGen: Sized {
    type Ok;
    type Error: serde::ser::Error;
    type SerializeSeq: serde::ser::SerializeSeq<Ok = Self::Ok, Error = Self::Error>;
    type SerializeTuple: serde::ser::SerializeTuple<Ok = Self::Ok, Error = Self::Error>;
    type SerializeTupleStruct: serde::ser::SerializeTupleStruct<Ok = Self::Ok, Error = Self::Error>;
    type SerializeTupleVariant: serde::ser::SerializeTupleVariant<
        Ok = Self::Ok,
        Error = Self::Error,
    >;
    type SerializeMap: serde::ser::SerializeMap<Ok = Self::Ok, Error = Self::Error>;
    type SerializeStruct: serde::ser::SerializeStruct<Ok = Self::Ok, Error = Self::Error>;
    type SerializeStructVariant: serde::ser::SerializeStructVariant<
        Ok = Self::Ok,
        Error = Self::Error,
    >;
    type Serializer: Serializer<
        Ok = Self::Ok,
        Error = Self::Error,
        SerializeSeq = Self::SerializeSeq,
        SerializeTuple = Self::SerializeTuple,
        SerializeTupleStruct = Self::SerializeTupleStruct,
        SerializeTupleVariant = Self::SerializeTupleVariant,
        SerializeMap = Self::SerializeMap,
        SerializeStruct = Self::SerializeStruct,
        SerializeStructVariant = Self::SerializeStructVariant,
    >;
    fn new() -> Self::Serializer;
}

trait SerdeBytes {
    type SerErr: serde::ser::Error;
    type DeErr: serde::de::Error;
    fn serialize<T: Serialize>(value: T) -> Result<Vec<u8>, Self::SerErr>;
    fn deserialize<'de, T: Deserialize<'de>>(slice: &'de [u8]) -> Result<T, Self::DeErr>;
}

pub trait ByteSer {
    type Error: serde::ser::Error + Sync + Send + 'static;
    fn serialize(&self) -> Result<Vec<u8>, Self::Error>;
}

pub trait ByteDe: Sized {
    type Error: serde::de::Error + Sync + Send + 'static;
    fn deserialize<'de>(slice: &'de [u8]) -> Result<Self, Self::Error>;
}

pub struct TypedDatabase {
    db: DB,
}

impl TypedDatabase {

    
    pub fn new(db: DB) -> Self {
        TypedDatabase { db: db }
    }

    pub fn put<S: ByteSer>(&self, k: S, v: S) -> Result<(), failure::Error> {
        let kb = k.serialize()?;
        let vb = v.serialize()?;
        self.db.put(kb, vb)?;
        Ok(())
    }

    pub fn get<S: ByteSer, D: ByteDe>(&self, k: S) -> Result<Option<D>, failure::Error> {
        let kb = k.serialize()?;
        let vb_opt = self.db.get(kb)?;
        let vb = match vb_opt {
            None => return Ok(None),
            Some(vb) => vb,
        };
        return Ok(Some(D::deserialize(vb.as_ref())?));
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
