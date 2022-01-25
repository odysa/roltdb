use std::{
    collections::HashMap,
    rc::{Rc, Weak},
    sync::{Arc, RwLock},
};

use crate::{
    bucket::Bucket,
    db::WeakDB,
    error::Result,
    meta::Meta,
    page::{Page, PageId},
};

#[derive(Debug)]
pub struct Transaction(pub(crate) Arc<ITransaction>);

#[derive(Debug)]
pub struct WeakTransaction(pub(crate) Weak<ITransaction>);

#[derive(Debug)]
pub(crate) struct ITransaction {
    pub(crate) writable: bool,
    db: RwLock<WeakDB>,
    managed: bool,
    root: RwLock<Bucket>,
    pages: RwLock<HashMap<PageId, Rc<Page>>>,
    meta: RwLock<Meta>,
    // commit_handlers: Vec<Box<dyn Fn()>>, // call functions after commit
}

impl Transaction {
    pub fn new(db: WeakDB, writable: bool) -> Self {
        Transaction(Arc::new(ITransaction {
            db: RwLock::new(db),
            managed: false,
            // commit_handlers: Vec::new(),
            pages: RwLock::new(HashMap::new()),
            writable,
            meta: RwLock::new(Meta::default()),
            root: RwLock::new(Bucket::new(WeakTransaction::new())),
        }))
    }
    pub fn page(&self, id: PageId) -> Result<Rc<Page>> {
        let pages = self.0.pages.try_read().unwrap();
        if let Some(page) = pages.get(&id) {
            Ok(page.clone())
        } else {
            todo!()
        }
    }
    pub fn rollback(&self) -> Result<()> {
        Ok(())
    }
    pub fn writable(&self) -> bool {
        self.0.writable
    }
}

impl WeakTransaction {
    pub fn new() -> Self {
        Self(Weak::new())
    }
}
