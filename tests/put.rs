use roltdb::{Bucket, Transaction, DB};

#[test]
fn open() {
    let db = DB::open("./tests/test.db").unwrap();
    {
        let tx = db.tx(true);
        let mut b = tx.create_bucket("test".to_string()).unwrap();
        // b.put(b"a", b"hello world").unwrap();
        drop(b);
        // let res = b.get(b"a").unwrap();
        // assert_eq!(res, b"hello world");
    }
}
