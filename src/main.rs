use std::collections::BTreeSet;
use std::io::prelude::*;
use std::marker::{PhantomData, Send};

use rocksdb::{IteratorMode, MergeOperands, Options, DB};
use serde::{Deserialize, Serialize};

use zerocopy::{AsBytes,FromBytes};

pub struct PinnedItem<'de, V: FromBytes> {
    phantom: PhantomData<V>,
    pinned_slice: rocksdb::DBPinnableSlice<'de>,
}

impl<'de, V: ?Sized + Deserialize<'de>> PinnedItem<'de, V> {
    pub fn into(&'de self) -> Result<V, failure::Error> {
        Ok(bincode::deserialize(self.pinned_slice.as_ref())?)
    }
}

pub struct TypedDB<K: ?Sized, V: ?Sized> {
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
    db: DB,
}

impl<K: Serialize + ?Sized, V: Serialize + ?Sized> TypedDB<K, V> {
    pub fn new(db: DB) -> Self {
        TypedDB {
            phantom_key: PhantomData,
            phantom_value: PhantomData,
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
            phantom: PhantomData,
            pinned_slice: vb,
        };

        return Ok(Some(pinned_item));
    }
}

struct DBIter<'a, K, V> {
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
    inner: rocksdb::DBIterator<'a>,
}

impl<'a, K: StaticDeserialize, V: StaticDeserialize> Iterator for DBIter<'a, K, V>
where
    <K as StaticDeserialize>::Error: Send + Sync + 'static,
    <V as StaticDeserialize>::Error: Send + Sync + 'static,
{
    type Item = Result<(K, V), failure::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let (kb, vb) = self.inner.next()?;

        let kd = K::deserialize(kb.as_ref());
        let k = match kd {
            Ok(k) => k,
            Err(e) => return Some(Err(e.into())),
        };

        let vd = V::deserialize(vb.as_ref());
        let v = match vd {
            Ok(v) => v,
            Err(e) => return Some(Err(e.into())),
        };

        Some(Ok((k, v)))
    }
}

impl<'a, K: Deserialize<'a>, V: Deserialize<'a>> TypedDB<K, V> {
    // type Item=Result<(K, V), failure::Error>;
    // type IntoIter=DBIter<'a, K, V>;

    fn into_iter(&'a self) -> DBIter<'a, K, V> {
        return DBIter {
            phantom_key: PhantomData,
            phantom_value: PhantomData,
            inner: self.db.iterator(IteratorMode::Start),
        };
    }
}

pub trait StaticDeserialize: Sized {
    type Error: std::error::Error;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error>;
}

pub trait AssociateMergeable: Sized + StaticDeserialize {
    fn merge(&mut self, other: &mut Self);
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
            (Some(mut m), Some(mut d)) => {
                m.merge(&mut d);
                Some(m)
            }
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

    pub fn merge(&self, k: &K, v: &V) -> Result<(), failure::Error> {
        let kb = bincode::serialize(k)?;
        let vb = bincode::serialize(v)?;

        self.typed_db.db.merge(kb, vb)?;
        Ok(())
    }
}

impl StaticDeserialize for BTreeSet<String> {
    type Error = bincode::Error;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error> {
        return bincode::deserialize(bytes);
    }
}

impl StaticDeserialize for String {
    type Error = bincode::Error;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error> {
        return bincode::deserialize(bytes);
    }
}

impl AssociateMergeable for BTreeSet<String> {
    fn merge(&mut self, other: &mut Self) {
        self.append(other)
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
