use crate::{
    cursor::Cursor,
    data::RawPtr,
    error::{Result, RoltError},
    node::{Node, WeakNode},
    page::{LeafPageElement, Page, PageId},
    transaction::{Transaction, WeakTransaction},
    utils::struct_to_slice,
    Err,
};
use anyhow::anyhow;
use either::Either;
use std::{
    borrow::BorrowMut, collections::HashMap, intrinsics::copy_nonoverlapping, mem::size_of,
    ops::Deref,
};
use std::{cell::RefCell, collections::hash_map::Entry};
// a collection of kev-value pairs
#[derive(Debug, Clone)]
pub struct Bucket {
    pub(crate) bucket: IBucket,
    // nested bucket
    pub(crate) buckets: RefCell<HashMap<String, Bucket>>,
    pub(crate) tx: WeakTransaction,
    pub(crate) page: Option<RawPtr<Page>>,
    pub(crate) root: Option<Node>,
    pub(crate) fill_percent: f64,
    pub(crate) nodes: HashMap<PageId, Node>,
    dirty: bool,
}

#[allow(dead_code)]
impl Bucket {
    pub(crate) const DEFAULT_FILL_PERCENT: f64 = 0.5;
    pub(crate) const MIN_FILL_PERCENT: f64 = 0.1;
    pub(crate) const MAX_FILL_PERCENT: f64 = 1.0;
    pub(crate) const BUCKET_HEADER_SIZE: usize = size_of::<Self>();
    pub(crate) const FLAG: u32 = 1;

    pub fn tx(&self) -> Result<Transaction> {
        self.tx.upgrade().ok_or(RoltError::TxNotValid.into())
    }

    pub fn new(tx: WeakTransaction) -> Self {
        Self {
            bucket: IBucket::new(),
            buckets: RefCell::new(HashMap::new()),
            root: None,
            nodes: HashMap::new(),
            page: None,
            fill_percent: Self::DEFAULT_FILL_PERCENT,
            tx,
            dirty: false,
        }
    }
    // create a bucket and put it in the root node
    pub(crate) fn create_bucket(&mut self, name: String) -> Result<&mut Bucket> {
        if !self.tx()?.writable() {
            panic!("tx not writable")
        }
        let key = name.as_bytes();
        let mut cursor = self.cursor();
        let pair = cursor.seek_to(key)?;
        if Some(key) == pair.key() {
            return Err!(RoltError::BucketExist);
        }
        {
            let mut b = Bucket::new(self.tx.clone());
            b.root = Some(Node::new(RawPtr::new(&b), crate::node::NodeType::Leaf));
            b.fill_percent = Self::DEFAULT_FILL_PERCENT;
            let bytes = b.as_bytes();
            cursor.node()?.put(key, key, &bytes, 0, Self::FLAG);
            self.page = None;
        }
        self.get_bucket(name)
            .map(|b| unsafe { &mut *b })
            .ok_or(anyhow!("cannot get bucket"))
    }

    pub(crate) fn create_bucket_if_not_exist(&mut self, name: String) -> Result<&mut Bucket> {
        let self_mut = unsafe { &mut *(self as *mut Self) };
        match self_mut.create_bucket(name.clone()) {
            Ok(b) => Ok(b),
            Err(_) => self
                .get_bucket(name)
                .map(|b| unsafe { &mut *b })
                .ok_or(anyhow!("cannot get bucket")),
        }
    }
    // get a bucket from nested buckets
    fn get_bucket(&self, key: String) -> Option<*mut Bucket> {
        if let Some(b) = self.buckets.borrow_mut().get_mut(&key) {
            return Some(b);
        };

        let mut cursor = self.cursor();
        let pair = match cursor.seek_to(key.as_bytes()) {
            Err(_) => {
                return None;
            }
            Ok(p) => p,
        };
        if Some(key.as_bytes()) != pair.key() {
            return None;
        }
        // get a sub-bucket from value
        let child = self.open_bucket(pair.value().unwrap());
        let mut buckets = self.buckets.borrow_mut();
        let bucket = match buckets.entry(key) {
            Entry::Occupied(e) => {
                let b = e.into_mut();
                *b = child;
                b
            }
            Entry::Vacant(e) => e.insert(child),
        };
        Some(bucket)
    }
    // get sub-bucket
    fn open_bucket(&self, bytes: &[u8]) -> Bucket {
        let mut child = Bucket::new(self.tx.clone());
        child.bucket = unsafe { (&*(bytes.as_ptr() as *const IBucket)).clone() };
        // sub-bucket is inline
        if child.bucket.root == 0 {
            let slice = &bytes[IBucket::SIZE..];
            let p = Page::from_buf_direct(slice);
            child.page = Some(RawPtr::new(p));
        }
        child
    }
    // get finds the value by key
    pub fn get(&self, target: &[u8]) -> Option<&[u8]> {
        let mut c = self.cursor();
        let pair = c.seek(target).unwrap();
        let (key, value) = (pair.key(), pair.value());
        if pair.flags == Self::FLAG || key != Some(target) {
            None
        } else {
            // notice: lifetime of reference to value
            value
        }
    }

    // put key and value
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.tx()?.writable() {
            return Err!("tx not writable");
        }
        if key.is_empty() {
            return Err!("empty key");
        }
        let mut cursor = self.cursor();
        let pair = cursor.seek(key)?;
        if Some(key) == pair.key() {}
        let mut node = cursor.node()?;
        node.put(key, key, value, 0, 0);
        Ok(())
    }

    // create a new cursor
    fn cursor(&self) -> Cursor {
        Cursor::new(self)
    }

    // get root page id of bucket
    pub fn root_id(&self) -> PageId {
        self.bucket.root
    }

    // get page or a node
    pub(crate) fn page_node(&self, id: PageId) -> Result<PageNode> {
        // use inline page
        if self.root_id() == 0 {
            if id != 0 {
                return Err!("inline bucket must have zero page");
            }
            if let Some(ref root) = self.root {
                Ok(PageNode::from(root.clone()))
            } else {
                if let Some(ref page) = self.page {
                    Ok(PageNode::from(page.clone()))
                } else {
                    Err!(RoltError::PageEmpty)
                }
            }
        } else if let Some(node) = self.nodes.get(&id) {
            Ok(PageNode::from(node.clone()))
        } else {
            Ok(PageNode::from(self.tx()?.page(id)?))
        }
    }

    pub fn clear(&mut self) {
        self.page = None;
        self.buckets.borrow_mut().clear();
        self.root = None;
        self.nodes.clear();
    }

    // write nodes to dirty pages
    pub(crate) fn spill(&mut self) -> Result<()> {
        let mut buckets = self.buckets.borrow_mut();

        for (name, child) in buckets.iter_mut() {
            let u8_name = name.as_bytes();
            let value = {
                child.spill()?;
                unsafe {
                    let bytes = struct_to_slice(&child.bucket);
                    bytes.clone().to_vec()
                }
            };

            if child.root.is_none() {
                continue;
            }
            // update
            let mut c = self.cursor();
            let pair = c.seek(u8_name)?;
            if Some(u8_name) != pair.key {
                return Err(anyhow::anyhow!("bucket header not match"));
            }
            let mut node = c.node()?;
            node.put(u8_name, u8_name, value.as_slice(), 0, pair.flags);
        }

        // spill root node
        if self.root.is_some() {
            let mut root = self.root.clone().ok_or(anyhow!("root is empty"))?;
            root.spill()?;
            self.root = Some(root);
            let page_id = self.root.as_ref().unwrap().page_id();
            self.bucket.root = page_id;
        }
        Ok(())
    }

    pub(crate) fn rebalance(&mut self) -> Result<()> {
        for (_, b) in self.buckets.borrow_mut().iter_mut() {
            // recursively rebalance
            if b.dirty {
                self.dirty = true;
                b.rebalance()?;
            }
        }
        if self.dirty {
            for node in self.nodes.borrow_mut().values_mut() {
                node.rebalance()?;
            }
        }
        Ok(())
    }
    // create a node from page
    pub(crate) fn node(&mut self, page_id: PageId, parent: WeakNode) -> Node {
        // panic if it is not writable
        assert!(self.tx().unwrap().writable());

        let mut node = Node::new(RawPtr::new(&self), crate::node::NodeType::Leaf);

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
                self.root = Some(node.clone());
            }
        };
        // read from page
        if let Some(ptr) = &self.page {
            let page = &*ptr;
            // convert page to node
            node.read(page).unwrap();
        } else {
            // get page from tx
            let page = self.tx().unwrap().page(page_id).unwrap();
            node.read(&*page).unwrap();
        }
        self.nodes.insert(page_id, node.clone());
        node
    }
    // convert bucket to bytes
    fn as_bytes(&self) -> Vec<u8> {
        let n = self.root.as_ref().unwrap();
        let mut bytes: Vec<u8> = vec![0; n.size() + IBucket::SIZE];
        let bucket_ptr = bytes.as_mut_ptr() as *mut IBucket;
        unsafe {
            copy_nonoverlapping(&self.bucket, bucket_ptr, 1);
            let page_buf = &mut bytes[IBucket::SIZE..];
            let page = &mut *(page_buf.as_mut_ptr() as *mut Page);
            // write root node to the fake page
            n.write(page).unwrap();
        }

        bytes
    }

    // check whether this bucket can be stored inline
    fn fit_inline(&self) -> bool {
        if self.root.is_none() || !self.root.as_ref().unwrap().is_leaf() {
            return false;
        }
        let mut size = Page::page_header_size();
        let root = self.root.clone().unwrap();
        for inode in root.inodes.borrow().iter() {
            // find child bucket
            if inode.is_bucket() {
                return false;
            }
            size += LeafPageElement::SIZE + inode.key().len() + inode.value().unwrap().len();
            if size > (self.tx().unwrap().db().unwrap().page_size() / 4) as usize {
                return false;
            }
        }
        true
    }
}
// on-file representation of bucket
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct IBucket {
    pub(crate) root: PageId,
    // increase monotonically
    pub(crate) sequence: u64,
}

impl IBucket {
    pub(crate) const SIZE: usize = size_of::<Self>();
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
            Either::Right(_) => unreachable!(),
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
