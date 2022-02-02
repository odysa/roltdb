use roltdb::{Bucket, Transaction, DB};

#[test]
fn open() {
    let db = DB::open("./tests/test.db").unwrap();
}
