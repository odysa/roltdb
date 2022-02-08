# RoltDB
RoltDB is Rust implementation of Bolt.
It utilizes Mmap.

```rust
let db = DB::new(path);
let tx = db.tx(true);
let b = tx.create_bucket("test");
b.put(b"hello",b"hello world").unwrap();
let value = b.get(b"hello").unwrap();
assert_eq!(value, "hello world");
```

