use std::marker::{PhantomData, Send};

use rocksdb::{IteratorMode, MergeOperands, Options, DB};

pub trait Deserializable: Sized {
    type Error: std::error::Error + Send + Sync + 'static;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error>;
}

impl Deserializable for String {
    type Error = std::str::Utf8Error;
    fn deserialize(bytes: &[u8]) -> Result<Self, Self::Error> {
        std::str::from_utf8(bytes).map(|s| s.to_owned())
    }
}
pub trait Serializable {
    // TODO: Should we allow for errors in serializing?
    type Bytes: AsRef<[u8]>;
    fn serialize(self) -> Self::Bytes;
}

impl<'a> Serializable for &'a str {
    type Bytes = &'a str;
    fn serialize(self) -> Self::Bytes {
        self
    }
}

trait TypedDB<KRef, V, VRef> {
    // TODO use a real error instead of failure
    fn get(&self, k: KRef) -> Result<Option<V>, failure::Error>;
    // TODO: Should we return the value on error?
    fn put(&self, k: KRef, v: VRef) -> Result<(), failure::Error>;
}

trait PutDB<KRef, VRef> {
    fn put(&self, k: KRef, v: VRef) -> Result<(), failure::Error>;
}

trait GetDB<KRef, V> {
    fn get(&self, k: KRef) -> Result<Option<V>, failure::Error>;
}

impl<DB, KRef, V, VRef> TypedDB<KRef, V, VRef> for DB
where
    DB: PutDB<KRef, VRef> + GetDB<KRef, V>,
    VRef: AsRef<[u8]>,
{
    fn put(&self, k: KRef, v: VRef) -> Result<(), failure::Error> {
        PutDB::put(self, k, v)
    }

    fn get(&self, k: KRef) -> Result<Option<V>, failure::Error> {
        GetDB::get(self, k)
    }
}

pub struct KeyValueDB<KRef, V, VRef> {
    phantom_key: PhantomData<KRef>,
    phantom_value: PhantomData<V>,
    phantom_ref: PhantomData<VRef>,
    db: DB,
}

impl<KRef, V, VRef> KeyValueDB<KRef, V, VRef> {
    pub fn new(db: DB) -> Self {
        KeyValueDB {
            phantom_key: PhantomData,
            phantom_value: PhantomData,
            phantom_ref: PhantomData,
            db,
        }
    }
}

impl<KRef, V, VRef> PutDB<KRef, VRef> for KeyValueDB<KRef, V, VRef>
where
    KRef: Serializable,
    VRef: Serializable,
{
    fn put(&self, k: KRef, v: VRef) -> Result<(), failure::Error> {
        let kb = k.serialize();
        let vb = v.serialize();

        self.db.put(kb, vb)?;
        Ok(())
    }
}

impl<KRef, V, VRef> GetDB<KRef, V> for KeyValueDB<KRef, V, VRef>
where
    KRef: Serializable,
    V: Deserializable,
    <V as Deserializable>::Error: Send + Sync + 'static,
{
    fn get(&self, k: KRef) -> Result<Option<V>, failure::Error> {
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

pub struct DBIter<'a, K: Deserializable, V: Deserializable> {
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
    inner: rocksdb::DBIterator<'a>,
}

impl<'a, K: Deserializable, V: Deserializable> Iterator for DBIter<'a, K, V> {
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

impl<'a, KRef, V, VRef> KeyValueDB<KRef, V, VRef>
where
    V: Deserializable,
{
    // type Item=Result<(K, V), failure::Error>;
    // type IntoIter=DBIter<'a, K, V>;

    fn db_iter<K: Deserializable>(&'a self) -> DBIter<'a, K, V> {
        DBIter {
            phantom_key: PhantomData,
            phantom_value: PhantomData,
            inner: self.db.iterator(IteratorMode::Start),
        }
    }
}

pub trait AssociateMergeable: Sized + Deserializable {
    fn merge(&mut self, other: &mut Self);
    fn handle_deser_error(key: &[u8], buf: &[u8], err: Self::Error) -> Option<Self>;
    fn into_bytes(self) -> Vec<u8>;
}

fn merge<V: AssociateMergeable>(
    key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    // TODO add an extra option to AssociateMergeable for handling failed merges, so that
    // one has the option of e.g. panicking, logging, or... ?
    let mut merged: Option<V> = existing_val.and_then(|unparsed| match V::deserialize(unparsed) {
        Ok(v) => Some(v),
        Err(err) => V::handle_deser_error(key, unparsed, err),
    });

    for unparsed in operands {
        let deser: Option<V> = match V::deserialize(unparsed) {
            Ok(v) => Some(v),
            Err(err) => V::handle_deser_error(key, unparsed, err),
        };

        merged = match (merged, deser) {
            (m, None) => m,
            (None, Some(d)) => Some(d),
            (Some(mut m), Some(mut d)) => {
                m.merge(&mut d);
                Some(m)
            }
        };
    }

    // TODO this .as_ref().to_owned() does a copy, which for strings is unnecessary
    merged.map(|value| value.into_bytes())
}

pub struct MergeableDB<K, V, VRef> {
    typed_db: KeyValueDB<K, V, VRef>,
}

// impl<K: Serializable + ?Sized, V: Serializable + Deserializable + ?Sized> TypedDB<K, V>
impl<K, V, VRef> PutDB<K, VRef> for MergeableDB<K, V, VRef>
where
    K: Serializable,
    VRef: Serializable,
{
    fn put(&self, k: K, v: VRef) -> Result<(), failure::Error> {
        self.typed_db.put(k, v)
    }
}

impl<K, V, VRef> GetDB<K, V> for MergeableDB<K, V, VRef>
where
    K: Serializable,
    V: Deserializable,
    <V as Deserializable>::Error: Send + Sync + 'static,
{
    fn get(&self, k: K) -> Result<Option<V>, failure::Error> {
        self.typed_db.get(k)
    }
}

impl<KRef, V, VRef> MergeableDB<KRef, V, VRef>
where
    KRef: Serializable,
    V: AssociateMergeable,
    VRef: Serializable,
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

    pub fn merge(&self, k: KRef, v: VRef) -> Result<(), failure::Error> {
        let kb = k.serialize();
        let vb = v.serialize();

        self.typed_db.db.merge(kb, vb)?;
        Ok(())
    }

    pub fn db_iter<K: Deserializable>(&self) -> DBIter<K, V> {
        self.typed_db.db_iter()
    }
}
