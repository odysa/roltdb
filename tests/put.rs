use roltdb::DB;

#[test]
fn open() {
    let db = DB::open("./tests/test.db").unwrap();
    let tx = db.tx(true).unwrap();
    let tx2 = db.tx(true);
    if tx2.is_ok() {
        panic!("tx2 should not be created");
    }
    let mut b = tx.create_bucket_if_not_exist("test".to_string()).unwrap();
    b.put(b"hello", b"hello world").unwrap();
    let res = b.get(b"a").unwrap();
    assert_eq!(res, b"a");
    let res = b.get(b"hello").unwrap();
    assert_eq!(res, b"hello world");
}
