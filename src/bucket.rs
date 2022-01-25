use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use either::Either;

use crate::{
    error::{Error, Result},
    node::Node,
    page::{Page, PageId},
    transaction::WeakTransaction,
};

pub(crate) struct Bucket(pub(crate) Arc<RefCell<InnerBucket>>);
// a collection of kev-value pairs
pub(crate) struct InnerBucket {
    bucket: IBucket,
    // nested bucket
    buckets: HashMap<String, Bucket>,
    tx: WeakTransaction,
    fill_percent: f64,
    root: Option<Rc<Node>>,
    nodes: HashMap<PageId, Rc<Node>>,
    page: Option<Rc<Page>>,
}

impl Bucket {
    const DEFAULT_FILL_PERCENT: f64 = 0.5;
    pub fn new(tx: WeakTransaction) -> Self {
        Self(Arc::new(RefCell::new(InnerBucket {
            bucket: IBucket::new(),
            buckets: HashMap::new(),
            root: None,
            nodes: HashMap::new(),
            page: None,
            fill_percent: Self::DEFAULT_FILL_PERCENT,
            tx,
        })))
    }
    // pub fn create_bucket(&self, key: String) {
    //     let tx = self.0.borrow().tx.0;
    //     let tx = tx.clone();
    // }
    // pub fn tx(&self) -> Result<Transaction> {
    //     let tx = self.0.tx.0;
    //     // tx.upgrade().ok_or(Error::TxNotValid)
    //     Ok(())
    // }
    pub fn root_id(&self) -> PageId {
        self.0.borrow().bucket.root
    }

    pub fn page_node(&self, id: PageId) -> Result<PageNode> {
        let b = self.0.borrow();
        if self.root_id() == 0 {
            if id != 0 {}
            if let Some(ref root) = b.root {
                Ok(PageNode::from(root.clone()))
            } else {
                if let Some(ref page) = b.page {
                    Ok(PageNode::from(page.clone()))
                } else {
                    Err(Error::PageEmpty)
                }
            }
        } else {
            if let Some(node) = b.nodes.get(&id) {
                Ok(PageNode::from(node.clone()))
            } else {
                todo!()
            }
        }
    }
    pub fn clear(&mut self) {
        let mut b = self.0.borrow_mut();
        b.page = None;
        b.buckets.clear();
        b.root = None;
        b.nodes.clear();
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

pub struct PageNode(Either<Rc<Page>, Rc<Node>>);

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
