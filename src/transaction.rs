use crate::{
    bucket::Bucket,
    data::RawPtr,
    db::{WeakDB, DB},
    error::Result,
    meta::Meta,
    page::{Page, PageId},
};
use anyhow::anyhow;
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    ops::Deref,
    rc::{Rc, Weak},
};
pub type TXID = u64;
#[derive(Debug, Clone)]
pub struct Transaction(pub(crate) Rc<ITransaction>);

#[derive(Debug, Clone)]
pub struct WeakTransaction(pub(crate) Weak<ITransaction>);

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
        Self(Rc::new(ITransaction::new(db, writable)))
    }
}

impl ITransaction {
    pub fn new(db: WeakDB, writable: bool) -> Self {
        let mut meta = match db.upgrade() {
            None => Meta::default(),
            Some(db) => db.meta().unwrap(),
        };
        if writable {
            meta.tx_id += 1;
        }
        ITransaction {
            db: RwLock::new(db),
            managed: false,
            // commit_handlers: Vec::new(),
            pages: RwLock::new(HashMap::new()),
            writable,
            meta: RwLock::new(meta),
            root: RwLock::new(Bucket::new(WeakTransaction::new())),
        }
    }

    pub fn page(&self, id: PageId) -> Result<RawPtr<Page>> {
        let pages = self.pages.read();
        if let Some(page) = pages.get(&id) {
            Ok(page.clone())
        } else {
            // get page from mmap
            Ok(RawPtr::new(self.db().page(id)))
        }
    }

    pub(crate) fn db(&self) -> DB {
        self.db.read().upgrade().unwrap()
    }

    pub fn rollback(&self) -> Result<()> {
        let db = self.db();
        if self.writable {
            let tx_id = self.id();
            let mut free_list = db.free_list.write();
            free_list.rollback(tx_id);
            let free_list_id = db.meta()?.free_list;
            let free_list_page = db.page(free_list_id);
            // reload free_list
            free_list.reload(free_list_page);
        }
        // close tx
        Ok(())
    }
    // // close a transaction
    // fn close(&self) -> Result<()> {
    //     let mut db = self.db();
    //     todo!()
    //     Ok(())
    // }
    // write change to disk and update meta page
    pub fn commit(&mut self) -> Result<()> {
        if !self.writable() {
            return Err(anyhow!("cannot commit read-only tx"));
        }
        {
            let root = &mut *self.root.try_write().unwrap();
            // rebalance
            root.rebalance();
            // spill
            root.spill()?;
        }
        Ok(())
    }
    // write pages to disk
    fn write_page(&mut self) -> Result<()> {
        let mut pages: Vec<(PageId, RawPtr<Page>)> =
            self.pages.write().drain().map(|(id, p)| (id, p)).collect();
        pages.sort_by(|x, y| x.0.cmp(&y.0));
        let mut db = self.db();
        let page_size = db.page_size();
        for (page_id, p) in pages {}
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

impl Drop for ITransaction {
    fn drop(&mut self) {
        // rollback read-only tx
        if !self.writable {}
    }
}

impl Deref for Transaction {
    type Target = Rc<ITransaction>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl WeakTransaction {
    pub(crate) fn new() -> Self {
        Self(Weak::new())
    }
    pub(crate) fn upgrade(&self) -> Option<Transaction> {
        self.0.upgrade().map(Transaction)
    }
}
