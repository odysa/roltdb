use std::{collections::HashMap, rc::Rc, sync::RwLock};

use crate::{
    bucket::Bucket,
    error::Result,
    meta::Meta,
    page::{Page, PageId},
};

pub(crate) struct Transaction {
    pub(crate) writable: bool,
    managed: bool,
    root: RwLock<Bucket>,
    pages: RwLock<HashMap<PageId, Rc<Page>>>,
    meta: RwLock<Meta>,
    commit_handlers: Vec<Box<dyn Fn()>>, // call functions after commit
}

impl Transaction {
    pub fn page(&self, id: PageId) -> Result<Rc<Page>> {
        let pages = self.pages.try_read().unwrap();
        if let Some(page) = pages.get(&id) {
            Ok(page.clone())
        } else {
            todo!()
        }
    }
}
