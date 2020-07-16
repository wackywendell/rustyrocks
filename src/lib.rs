use std::marker::{PhantomData, Send};

use rocksdb::{IteratorMode, MergeOperands, Options, DB};
use serde::{Deserialize, Serialize};

pub trait StaticDeserialize: Sized {
    type Error: std::error::Error + Sync + Send + 'static;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error>;
}

pub trait StaticSerialize: Sized {
    type Error: std::error::Error + Sync + Send + 'static;
    type Bytes: AsRef<[u8]>;
    fn serialize(&self) -> Result<Self::Bytes, Self::Error>;
}

pub struct PinnedItem<'de, V: ?Sized + StaticDeserialize> {
    value_type: PhantomData<V>,
    pinned_slice: rocksdb::DBPinnableSlice<'de>,
}

impl<'de, V: ?Sized + StaticDeserialize> PinnedItem<'de, V> {
    pub fn into(&'de self) -> Result<V, V::Error> {
        V::deserialize(self.pinned_slice.as_ref())
    }
}

pub struct TypedDB<K: ?Sized, V: ?Sized> {
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
    db: DB,
}

impl<K: StaticSerialize + ?Sized, V: StaticSerialize + ?Sized> TypedDB<K, V> {
    pub fn new(db: DB) -> Self {
        TypedDB {
            phantom_key: PhantomData,
            phantom_value: PhantomData,
            db,
        }
    }

    pub fn put(&self, k: &K, v: &V) -> Result<(), failure::Error> {
        let kb = k.serialize()?;
        let vb = v.serialize()?;

        self.db.put(kb, vb)?;
        Ok(())
    }
}

impl<'a, K: StaticSerialize + ?Sized, V: StaticDeserialize + ?Sized> TypedDB<K, V> {
    pub fn get(&'a self, k: &'a K) -> Result<Option<PinnedItem<'a, V>>, failure::Error> {
        let kb = k.serialize()?;
        let vb_opt: Option<rocksdb::DBPinnableSlice<'a>> = self.db.get_pinned(kb)?;
        let vb: rocksdb::DBPinnableSlice<'a> = match vb_opt {
            None => return Ok(None),
            Some(vb) => vb,
        };

        let pinned_item = PinnedItem {
            value_type: PhantomData,
            pinned_slice: vb,
        };

        Ok(Some(pinned_item))
    }
}

pub struct DBIter<'a, K, V> {
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

    pub fn iter(&'a self) -> DBIter<'a, K, V> {
        DBIter {
            phantom_key: PhantomData,
            phantom_value: PhantomData,
            inner: self.db.iterator(IteratorMode::Start),
        }
    }
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

fn merge<V: StaticSerialize + AssociateMergeable>(
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

    merged.and_then(|value| unwrap_or_log(value.serialize()))
}

pub struct MergeableDB<K: ?Sized, V: ?Sized> {
    typed_db: TypedDB<K, V>,
}

impl<'a, K: StaticSerialize + ?Sized, V: StaticDeserialize + ?Sized> MergeableDB<K, V> {
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
