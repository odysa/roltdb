use roltdb::DB;

#[test]
fn open() {
    let db = DB::open("./tests/test.db").unwrap();
    let tx = db.tx(true);

    let b = tx.create_bucket_if_not_exist("test".to_string()).unwrap();
    let res = b.get(b"a").unwrap();
    assert_eq!(res, b"a");
}
