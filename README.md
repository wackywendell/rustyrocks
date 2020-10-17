# Rusty Rocks

A safe, rustic interface for rocksdb that keeps data typed. In development.

## Requirements

Requires llvm, as a transitive requirement for [`rust-rocksdb`](https://github.com/rust-rocksdb/rust-rocksdb). The version of rocksdb used is pinned and included in the `rust-rocksdb` crate.

### Installation: Ubuntu

```bash
sudo aptitude install clang
```

That's it. Maybe just llvm would do... but clang worked.