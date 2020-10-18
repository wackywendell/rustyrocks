use std::collections::BTreeSet;
use std::io::prelude::*;
use std::marker::{PhantomData, Send};

use rocksdb::{IteratorMode, MergeOperands, Options, DB};
use serde::{Deserialize, Serialize};

pub trait StaticDeserialize: Sized {
    type Error: std::error::Error + Send + Sync + 'static;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error>;
}

pub trait StaticSerialize: Sized {
    // TODO: Should we allow for errors in serializing?
    fn serialize(&self) -> &[u8];
}

trait TypedDB<K: ?Sized, V> {
    fn get(&self, k: &K) -> Result<Option<V>, failure::Error>;
    // TODO: Should we return the value on error?
    fn put(&self, k: &K, v: V) -> Result<(), failure::Error>;
}

pub struct KeyValueDB<K: ?Sized, V: ?Sized> {
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
    db: DB,
}

impl<K: StaticSerialize + ?Sized, V: StaticSerialize + StaticDeserialize + ?Sized>
    KeyValueDB<K, V>
{
    pub fn new(db: DB) -> Self {
        KeyValueDB {
            phantom_key: PhantomData,
            phantom_value: PhantomData,
            db: db,
        }
    }
}

impl<K: StaticSerialize + ?Sized, V: StaticSerialize + StaticDeserialize + ?Sized> TypedDB<K, V>
    for KeyValueDB<K, V>
where
    <V as StaticDeserialize>::Error: Send + Sync + 'static,
{
    fn put(&self, k: &K, v: V) -> Result<(), failure::Error> {
        let kb = k.serialize();
        let vb = v.serialize();

        self.db.put(kb, vb)?;
        Ok(())
    }

    fn get(&self, k: &K) -> Result<Option<V>, failure::Error> {
        let kb = k.serialize();
        let vb_opt: Option<rocksdb::DBPinnableSlice> = self.db.get_pinned(kb)?;
        let vb: rocksdb::DBPinnableSlice = match vb_opt {
            None => return Ok(None),
            Some(vb) => vb,
        };

        let v = V::deserialize(vb.as_ref())?;

        Ok(Some(v))
    }
}

struct DBIter<'a, K, V> {
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
    inner: rocksdb::DBIterator<'a>,
}

impl<'a, K: StaticDeserialize, V: StaticDeserialize> Iterator for DBIter<'a, K, V> {
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

impl<'a, K: Deserialize<'a>, V: Deserialize<'a>> KeyValueDB<K, V> {
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

pub trait AssociateMergeable: Sized + StaticSerialize + StaticDeserialize {
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

fn merge<V: AssociateMergeable>(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let mut merged: Option<V> = existing_val
        .map(|unparsed| V::deserialize(unparsed).expect("Could not deserialize existing value"));

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

    merged.map(|value| value.serialize().to_owned())
}

pub struct MergeableDB<K: ?Sized, V: ?Sized> {
    typed_db: KeyValueDB<K, V>,
}

impl<K, V> MergeableDB<K, V>
where
    K: StaticSerialize + ?Sized,
    V: StaticSerialize + StaticDeserialize,
{
    pub fn get(&self, k: &K) -> Result<Option<V>, failure::Error> {
        self.typed_db.get(k)
    }
}

impl<K, V> MergeableDB<K, V>
where
    K: StaticSerialize + ?Sized,
    V: StaticSerialize + StaticDeserialize,
{
    pub fn put(&self, k: &K, v: V) -> Result<(), failure::Error> {
        self.typed_db.put(k, v)
    }
}

impl<'a, K, V> MergeableDB<K, V>
where
    K: StaticSerialize + ?Sized,
    V: AssociateMergeable,
{
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, failure::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        opts.set_merge_operator("test operator", merge::<V>, None);
        let db = DB::open(&opts, path)?;

        Ok(MergeableDB {
            typed_db: KeyValueDB::new(db),
        })
    }

    pub fn merge(&self, k: &K, v: &V) -> Result<(), failure::Error> {
        let kb = k.serialize();
        let vb = v.serialize();

        self.typed_db.db.merge(kb, vb)?;
        Ok(())
    }
}
