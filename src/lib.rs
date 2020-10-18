use std::marker::{PhantomData, Send};

use rocksdb::{IteratorMode, MergeOperands, Options, DB};

pub trait StaticDeserialize: Sized {
    type Error: std::error::Error + Send + Sync + 'static;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error>;
}

impl StaticDeserialize for String {
    type Error = std::str::Utf8Error;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error> {
        std::str::from_utf8(bytes).map(|s| s.to_owned())
    }
}

pub trait StaticSerialize {
    // TODO: Should we allow for errors in serializing?
    fn serialize(&self) -> &[u8];
}

impl StaticSerialize for str {
    fn serialize(&self) -> &[u8] {
        self.as_ref()
    }
}

impl StaticSerialize for String {
    fn serialize(&self) -> &[u8] {
        self.as_ref()
    }
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
            db,
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

pub struct DBIter<'a, K: ?Sized, V> {
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

impl<'a, K: ?Sized, V> KeyValueDB<K, V> {
    // type Item=Result<(K, V), failure::Error>;
    // type IntoIter=DBIter<'a, K, V>;

    fn db_iter(&'a self) -> DBIter<'a, K, V> {
        DBIter {
            phantom_key: PhantomData,
            phantom_value: PhantomData,
            inner: self.db.iterator(IteratorMode::Start),
        }
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

impl<K: StaticSerialize + ?Sized, V: StaticSerialize + StaticDeserialize + ?Sized> TypedDB<K, V>
    for MergeableDB<K, V>
where
    <V as StaticDeserialize>::Error: Send + Sync + 'static,
{
    fn get(&self, k: &K) -> Result<Option<V>, failure::Error> {
        self.typed_db.get(k)
    }

    fn put(&self, k: &K, v: V) -> Result<(), failure::Error> {
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

    pub fn db_iter(&'a self) -> DBIter<'a, K, V> {
        self.typed_db.db_iter()
    }
}
