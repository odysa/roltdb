# RoltDB
Rust implementation of boltdb which is based on mmap
```rust
let db = DB::new(path);
let tx = db.tx(true);
let b = tx.create_bucket("test");
```

