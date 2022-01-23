use std::{collections::HashMap, sync::RwLock};

use crate::{
    bucket::Bucket,
    meta::Meta,
    page::{Page, PageId},
};

pub(crate) struct Transaction {
    writable: bool,
    managed: bool,
    root: RwLock<Bucket>,
    pages: RwLock<HashMap<PageId, Page>>,
    meta: RwLock<Meta>,
    commit_handlers: Vec<Box<dyn Fn()>>, // call functions after commit
}

impl Transaction {
}
