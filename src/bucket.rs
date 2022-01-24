use std::{
    cell::RefCell,
    collections::HashMap,
    ptr::NonNull,
    rc::{Rc, Weak},
};

use either::Either;

use crate::{
    error::{Error, Result},
    node::Node,
    page::{Page, PageId},
    transaction::Transaction,
};

// a collection of kev-value pairs
pub(crate) struct Bucket {
    bucket: IBucket,
    // nested bucket
    buckets: HashMap<String, Bucket>,
    tx: Weak<Transaction>,
    fill_percent: f64,
    root: Option<Rc<Node>>,
    nodes: HashMap<PageId, Node>,
    page: Option<Rc<Page>>,
}
impl Bucket {
    const DEFAULT_FILL_PERCENT: f64 = 0.5;
    pub fn new(tx: Weak<Transaction>) -> Bucket {
        Bucket {
            bucket: IBucket::new(),
            buckets: HashMap::new(),
            root: None,
            nodes: HashMap::new(),
            page: None,
            fill_percent: Self::DEFAULT_FILL_PERCENT,
            tx,
        }
    }
    pub fn create_bucket(&self, key: String) {
        let tx = self.tx.clone();
    }
    pub fn tx(&self) -> Result<Rc<Transaction>> {
        self.tx.upgrade().ok_or(Error::TxNotValid)
    }
    pub fn root_id(&self) -> PageId {
        self.bucket.root
    }

    pub fn page_node(&self, id: PageId) -> Result<PageNode> {
        if self.root_id() == 0 {
            if id != 0 {}
            if let Some(ref root) = self.root {
                Ok(PageNode::from(root.clone()))
            } else {
                if let Some(ref page) = self.page {
                    Ok(PageNode::from(page.clone()))
                } else {
                    Err(Error::PageEmpty)
                }
            }
        } else {
            if let Some(node) = self.nodes.get(&id) {
                Ok(PageNode::from(Rc::new(node)))
            } else {
                todo!()
            }
        }
    }
    pub fn clear(&mut self) {
        self.page = None;
        self.buckets.clear();
        self.root = None;
        self.nodes.clear();
    }
}
// on-file representation of bucket
pub(crate) struct IBucket {
    root: PageId,
    // increase monotonically
    sequence: u64,
}

impl IBucket {
    pub fn new() -> Self {
        Self {
            root: 0,
            sequence: 0,
        }
    }
}

struct PageNode(Either<Rc<Page>, Rc<Node>>);

impl From<Rc<Node>> for PageNode {
    fn from(node: Rc<Node>) -> Self {
        Self(Either::Right(node))
    }
}

impl From<Rc<Page>> for PageNode {
    fn from(page: Rc<Page>) -> Self {
        Self(Either::Left(page))
    }
}
