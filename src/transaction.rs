use std::{
    collections::HashMap,
    ops::Deref,
    rc::{Rc, Weak},
    sync::RwLock,
};

use crate::{
    bucket::Bucket,
    data::RawPtr,
    db::WeakDB,
    error::Result,
    meta::Meta,
    page::{Page, PageId},
};
pub type TXID = u64;
#[derive(Debug, Clone)]
pub struct Transaction(pub(crate) Rc<ITransaction>);

#[derive(Debug, Clone)]
pub struct WeakTransaction(pub(crate) Weak<ITransaction>);
impl WeakTransaction {
    pub(crate) fn upgrade(&self) -> Option<Transaction> {
        self.0.upgrade().map(Transaction)
    }
}
#[derive(Debug)]
pub struct ITransaction {
    pub writable: bool,
    db: RwLock<WeakDB>,
    managed: bool,
    root: RwLock<Bucket>,
    pages: RwLock<HashMap<PageId, RawPtr<Page>>>,
    meta: RwLock<Meta>,
    // commit_handlers: Vec<Box<dyn Fn()>>, // call functions after commit
}

impl Transaction {
    pub fn new(db: WeakDB, writable: bool) -> Self {
        Transaction(Rc::new(ITransaction {
            db: RwLock::new(db),
            managed: false,
            // commit_handlers: Vec::new(),
            pages: RwLock::new(HashMap::new()),
            writable,
            meta: RwLock::new(Meta::default()),
            root: RwLock::new(Bucket::new(WeakTransaction::new())),
        }))
    }
    pub fn page(&self, id: PageId) -> Result<RawPtr<Page>> {
        let pages = self.0.pages.read().unwrap();
        if let Some(page) = pages.get(&id) {
            Ok(page.clone())
        } else {
            // get page from mmap
            todo!()
        }
    }
    pub fn rollback(&self) -> Result<()> {
        Ok(())
    }
    pub fn writable(&self) -> bool {
        self.writable
    }
    pub(crate) fn id(&self) -> TXID {
        self.meta.try_read().unwrap().tx_id
    }
    pub(crate) fn page_id(&self) -> PageId {
        self.meta.try_read().unwrap().page_id
    }
}

impl Deref for Transaction {
    type Target = Rc<ITransaction>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl WeakTransaction {
    pub fn new() -> Self {
        Self(Weak::new())
    }
}
