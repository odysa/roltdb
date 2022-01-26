use std::{
    cell::{Ref, RefCell, RefMut},
    collections::HashMap,
    ops::{Deref, DerefMut},
    rc::Rc,
};

use either::Either;

use crate::{
    cursor::Cursor,
    data::RawPtr,
    error::{Error, Result},
    node::{Node, WeakNode},
    page::{Page, PageId},
    transaction::{Transaction, WeakTransaction},
};

#[derive(Debug, Clone)]
pub(crate) struct Bucket(pub(crate) Rc<RefCell<InnerBucket>>);

impl Deref for Bucket {
    type Target = Rc<RefCell<InnerBucket>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Bucket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// a collection of kev-value pairs
#[derive(Debug, Clone)]
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
    pub fn tx(&self) -> Result<Transaction> {
        self.inner().tx.upgrade().ok_or(Error::from("tx not valid"))
    }
    pub fn new(tx: WeakTransaction) -> Self {
        Self(Rc::new(RefCell::new(InnerBucket {
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

    // get finds the value by key
    pub fn get(&self, target: &[u8]) -> Option<&[u8]> {
        let pair = self.cursor().seek(target).unwrap();
        let (key, value) = (pair.key(), pair.value());
        if key != Some(target) {
            None
        } else {
            // notice: lifetime of reference to value
            value
        }
    }
    fn cursor(&self) -> Cursor {
        Cursor::new(self.clone())
    }

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
    fn inner(&self) -> Ref<InnerBucket> {
        (*self.0).borrow()
    }
    fn inner_mut(&mut self) -> RefMut<InnerBucket> {
        (*self.0).borrow_mut()
    }
    pub fn clear(&mut self) {
        let mut b = self.inner_mut();
        b.page = None;
        b.buckets.clear();
        b.root = None;
        b.nodes.clear();
    }
    // create a node from page
    pub(crate) fn node(&mut self, page_id: PageId, parent: WeakNode) -> Node {
        // panic if it is not writable
        assert!(self.tx().unwrap().writable());

        let mut node = Node::default();
        let mut node_mut = node.inner_mut();
        node_mut.parent = parent;
        // node crated
        if let Some(n) = self.inner().nodes.get(&page_id) {
            return n.clone();
        }
        match parent.upgrade() {
            Some(p) => {
                p.inner_mut().children.push(node.clone());
            }
            None => {
                // set new root if parent is empty
                self.inner_mut().root.replace(node.clone());
            }
        };
        // read from page
        if let Some(ptr) = self.inner().page {
            let page = &*ptr;
            // convert page to node
            node.read(page);
        } else {
            // get page from tx
            let page = self.tx().unwrap().page(page_id).unwrap();
            node.read(&*page);
        }
        self.inner_mut().nodes.insert(page_id, node.clone());
        node
    }
}
// on-file representation of bucket
#[derive(Debug, Clone, Copy)]
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

impl Deref for PageNode {
    type Target = Either<RawPtr<Page>, Node>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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
