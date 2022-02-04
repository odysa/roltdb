use roltdb::{Bucket, Transaction, DB};

#[test]
fn open() {
    let db = DB::open("./tests/test.db").unwrap();
    let tx = db.tx(true);
    {
        let mut b = tx.create_bucket("test".to_string()).unwrap();
        b.put(b"a", b"hello world").unwrap();

        let res = b.get(b"a").unwrap();
        println!("{:?}", String::from_utf8(res.to_vec()));
    }
    tx.commit().unwrap();
}
