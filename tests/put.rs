use roltdb::{Bucket, Transaction, DB};

#[test]
fn open() {
    let db = DB::open("./tests/test.db").unwrap();
    let tx = db.tx(true);

    let mut b = tx.create_bucket("test".to_string()).unwrap();
}
