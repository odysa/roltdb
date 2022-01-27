use std::{collections::HashMap, ops::Deref};

use either::Either;

use crate::{
    cursor::Cursor,
    data::RawPtr,
    error::{Error, Result},
    node::{Node, WeakNode},
    page::{Page, PageId},
    transaction::{Transaction, WeakTransaction},
};

// #[derive(Debug, Clone)]
// pub(crate) struct Bucket(pub(crate) Rc<RefCell<InnerBucket>>);

// #[derive(Debug, Clone, Default)]
// pub(crate) struct WeakBucket(pub(crate) Weak<RefCell<InnerBucket>>);

// impl Deref for Bucket {
//     type Target = Rc<RefCell<InnerBucket>>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

// impl DerefMut for Bucket {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.0
//     }
// }

// a collection of kev-value pairs
#[derive(Debug, Clone)]
pub(crate) struct Bucket {
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
    const DEFAULT_FILL_PERCENT: f64 = 0.5;
    pub fn tx(&self) -> Result<Transaction> {
        self.tx.upgrade().ok_or(Error::from("tx not valid"))
    }
    pub fn new(tx: WeakTransaction) -> Self {
        Self {
            bucket: IBucket::new(),
            buckets: HashMap::new(),
            root: None,
            nodes: HashMap::new(),
            page: None,
            fill_percent: Self::DEFAULT_FILL_PERCENT,
            tx,
        }
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
                Ok(PageNode::from(node.clone()))
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
    // create a node from page
    pub(crate) fn node(&mut self, page_id: PageId, parent: WeakNode) -> Node {
        // panic if it is not writable
        assert!(self.tx().unwrap().writable());

        let mut node = Node::default();

        // node crated
        if let Some(n) = self.nodes.get(&page_id) {
            return n.clone();
        }
        match parent.upgrade() {
            Some(p) => {
                p.children.borrow_mut().push(node.clone());
            }
            None => {
                // set new root if parent is empty
                self.root.replace(node.clone());
            }
        };
        // read from page
        if let Some(ptr) = &self.page {
            let page = &*ptr;
            // convert page to node
            node.read(page);
        } else {
            // get page from tx
            let page = self.tx().unwrap().page(page_id).unwrap();
            node.read(&*page);
        }
        self.nodes.insert(page_id, node.clone());
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
pub(crate) struct PageNode(Either<RawPtr<Page>, Node>);

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
            Either::Right(ref n) => n.inodes.borrow().len(),
        }
    }
}
