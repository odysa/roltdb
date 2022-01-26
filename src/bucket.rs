use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use either::Either;

use crate::{
    data::RawPtr,
    error::{Error, Result},
    node::Node,
    page::{Page, PageId},
    transaction::WeakTransaction,
};

#[derive(Debug)]
pub(crate) struct Bucket(pub(crate) Arc<RefCell<InnerBucket>>);
// a collection of kev-value pairs
#[derive(Debug)]
pub(crate) struct InnerBucket {
    bucket: IBucket,
    // nested bucket
    buckets: HashMap<String, Bucket>,
    tx: WeakTransaction,
    fill_percent: f64,
    root: Option<Node>,
    nodes: HashMap<PageId, Node>,
    page: Option<RawPtr<Page>>,
}

impl Bucket {
    pub fn clone(&self) -> Self {
        Self(self.0.clone())
    }
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
#[derive(Debug)]
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
#[derive(Clone, Debug)]
pub struct PageNode(Either<RawPtr<Page>, Node>);

impl From<Node> for PageNode {
    fn from(node: Node) -> Self {
        Self(Either::Right(node))
    }
}

impl From<RawPtr<Page>> for PageNode {
    fn from(page: RawPtr<Page>) -> Self {
        Self(Either::Left(page))
    }
}

impl PageNode {
    pub(crate) fn is_leaf(&self) -> bool {
        match self.0 {
            Either::Left(_) => self.page().is_leaf(),
            Either::Right(ref n) => n.is_leaf(),
        }
    }
    fn page(&self) -> &Page {
        match self.0 {
            Either::Left(ref ptr) => &*ptr,
            Either::Right(_) => todo!(),
        }
    }
    pub(crate) fn upgrade(&self) -> Either<&Page, &Node> {
        match self.0 {
            Either::Left(ref ptr) => Either::Left(&*ptr),
            Either::Right(ref n) => Either::Right(n),
        }
    }
    pub(crate) fn count(&self) -> usize {
        match self.0 {
            Either::Left(_) => self.page().count as usize,
            Either::Right(ref n) => n.inner().inodes.len(),
        }
    }
}
