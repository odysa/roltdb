use std::{cmp::Ordering, marker::PhantomData, ops::Deref, borrow::BorrowMut};

use crate::{
    bucket::{Bucket, PageNode},
    error::Result,
    node::Node,
    page::{Page, PageId},
};

pub(crate) struct Cursor<'a> {
    bucket: Bucket,
    stack: Vec<ElementRef>,
    // constrains the lifetime of pair
    _f: PhantomData<KVPair<'a>>,
}
impl<'a> Cursor<'a> {
    pub fn new(b: Bucket) -> Self {
        Self {
            bucket: b,
            stack: Vec::new(),
            _f: PhantomData,
        }
    }
    pub fn bucket(&self) -> &Bucket {
        &self.bucket
    }
    pub fn first(&mut self) -> Result<KVPair> {
        self.stack.clear();
        let root_elem = self.bucket.page_node(self.bucket.root_id())?;
        self.stack.push(ElementRef {
            page_node: root_elem,
            index: 0,
        });
        // look for the first leaf node
        self.first_leaf()?;
        //
        let pair = self.kv_pair()?;

        Ok(pair)
    }
    fn first_leaf(&mut self) -> Result<()> {
        loop {
            let elem = self.stack.last().ok_or("empty stack")?;
            // stop when find a leaf
            if elem.is_leaf() {
                break;
            }
            // if it is branch then go deeper
            let page_id = match elem.upgrade() {
                either::Either::Left(p) => p.branch_elements()?[elem.index].id,
                either::Either::Right(n) => n.inner().inodes[elem.index]
                    .page_id()
                    .ok_or("does not have page id")?,
            };
            let page_node = self.bucket.page_node(page_id)?;
            self.stack.push(ElementRef {
                index: 0,
                page_node,
            })
        }
        Ok(())
    }
    // move to the next leaf element
    fn next_leaf(&self) -> Result<KVPair> {
        todo!()
    }
    pub fn last(&self) -> Result<KVPair> {
        todo!()
    }
    pub fn next(&self) -> Result<KVPair> {
        todo!()
    }
    pub fn prev(&self) -> Result<KVPair> {
        todo!()
    }

    pub(crate) fn seek(&mut self, target: &[u8]) -> Result<KVPair<'a>> {
        let pair = self.seek_to(target)?;

        Ok(pair)
    }

    // move cursor to a key
    fn seek_to(&mut self, target: &[u8]) -> Result<KVPair<'a>> {
        self.stack.clear();
        let root_id = self.bucket.root_id();
        self.search(target, root_id)?;
        self.kv_pair()
    }

    // recursively look for the key
    fn search(&mut self, target: &[u8], id: PageId) -> Result<()> {
        // get node or page by id
        let page_node = self.bucket.page_node(id)?;
        let elem = ElementRef {
            index: 0,
            page_node,
        };
        self.stack.push(elem.clone());
        // it is a leaf node
        if elem.is_leaf() {
            return self.nsearch(target);
        }
        match elem.upgrade() {
            either::Either::Left(p) => self.search_page(target, p)?,
            either::Either::Right(n) => self.search_node(target, n)?,
        };

        Ok(())
    }
    // find target key in a page
    fn search_page(&mut self, target: &[u8], p: &Page) -> Result<()> {
        let branches = p.branch_elements()?;
        let (found, mut index) = match branches.binary_search_by(|b| b.key().cmp(target)) {
            Ok(mut v) => {
                // find the highest index
                let start = v;
                for i in start..(branches.len() - 1) {
                    match branches[i].key().cmp(target) {
                        Ordering::Equal => v = i,
                        _ => break,
                    }
                }
                (true, v)
            }
            Err(i) => (false, i),
        };
        // if not found, index be the last one
        if !found && index > 0 {
            index -= 1;
        }
        self.stack.last_mut().ok_or("empty stack")?.index = index;

        // recursively search the next node
        self.search(target, branches[index].id)?;

        Ok(())
    }
    // find target key in a node
    fn search_node(&mut self, target: &[u8], n: &Node) -> Result<()> {
        let inodes = &n.inner().inodes;
        let (found, mut index) =
            match inodes.binary_search_by(|inode| inode.key().as_slice().cmp(target)) {
                Ok(mut v) => {
                    // find the highest index
                    let start = v;
                    for i in start..(inodes.len() - 1) {
                        match inodes[i].key().as_slice().cmp(target) {
                            Ordering::Equal => v = i,
                            _ => break,
                        }
                    }
                    (true, v)
                }
                Err(i) => (false, i),
            };
        if !found && index > 0 {
            index -= 1;
        }
        self.stack.last_mut().ok_or("empty stack")?.index = index;
        let page_id = inodes[index]
            .page_id()
            .ok_or("leaf inode does not have page id")?;
        self.search(target, page_id)?;
        Ok(())
    }

    // search leaf node for the key
    fn nsearch(&mut self, target: &[u8]) -> Result<()> {
        let elem = self.stack.last_mut().ok_or("stack empty")?;
        match elem.upgrade() {
            either::Either::Left(p) => {
                let leaves = p.leaf_elements()?;
                let index = match leaves.binary_search_by(|l| l.key().cmp(target)) {
                    Ok(i) => i,
                    Err(i) => i,
                };
                elem.index = index;
                Ok(())
            }
            either::Either::Right(n) => {
                let index = match n
                    .inner()
                    .inodes
                    .binary_search_by(|inode| inode.key().as_slice().cmp(target))
                {
                    Ok(i) => i,
                    Err(i) => i,
                };
                elem.index = index;
                Ok(())
            }
        }
    }
    fn kv_pair(&self) -> Result<KVPair<'a>> {
        let elem = self.stack.last().ok_or("stack is empty")?;
        Ok(KVPair::from(elem))
    }
    pub(crate) fn node(&self) -> Result<Node> {
        let elem = &self.stack.last().ok_or("stack is empty")?;
        // leaf node is on the top of stack
        if elem.is_leaf() & elem.is_left() {
            Ok(elem.as_ref().right().unwrap().clone())
        }else{
            let root = self.stack[0].clone();
            match root.upgrade() {
                // read page
                either::Either::Left(p) => {
                    
                },
                either::Either::Right(n) => n.clone(),
            }
        }
        //
    }
    // pub fn first(&self) -> (Option<Entry>, Option<Entry>) {

    // }
}
#[derive(Debug, Clone)]
struct ElementRef {
    index: usize,
    page_node: PageNode,
}

impl Deref for ElementRef {
    type Target = PageNode;
    fn deref(&self) -> &Self::Target {
        &self.page_node
    }
}

#[derive(Debug)]
pub(crate) struct KVPair<'a> {
    pub(crate) key: Option<&'a [u8]>,
    pub(crate) value: Option<&'a [u8]>,
}

impl<'a> KVPair<'a> {
    pub(crate) fn null() -> Self {
        Self {
            key: None,
            value: None,
        }
    }
    pub(crate) fn key(&self) -> Option<&'a [u8]> {
        self.key
    }
    pub(crate) fn value(&self) -> Option<&'a [u8]> {
        self.value
    }
}

impl<'a> From<&ElementRef> for KVPair<'a> {
    fn from(elem: &ElementRef) -> Self {
        if elem.count() == 0 {
            return Self::null();
        }
        unsafe {
            match elem.upgrade() {
                either::Either::Left(ref p) => {
                    let leaf = &p.leaf_elements().unwrap()[elem.index];
                    Self {
                        key: Some(&*(leaf.key() as *const [u8])),
                        value: Some(&*(leaf.key() as *const [u8])),
                    }
                }
                either::Either::Right(ref n) => {
                    let inode = &n.inner().inodes[elem.index];
                    let value = inode.value().ok_or("does not have value").unwrap();
                    Self {
                        key: Some(&*(inode.key().as_slice() as *const [u8])),
                        value: Some(&*(value.as_slice() as *const [u8])),
                    }
                }
            }
        }
    }
}
