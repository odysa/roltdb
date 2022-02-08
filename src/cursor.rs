use std::{cell::RefCell, cmp::Ordering, marker::PhantomData, ops::Deref};

use crate::{
    bucket::{Bucket, PageNode},
    error::{Result, RoltError},
    node::{Node, WeakNode},
    page::{Page, PageId},
};
use anyhow::anyhow;
pub(crate) struct Cursor<'a> {
    bucket: &'a Bucket,
    stack: RefCell<Vec<ElementRef>>,
    // constrains the lifetime of pair
    _f: PhantomData<KVPair<'a>>,
}

#[allow(dead_code)]
impl<'a> Cursor<'a> {
    pub fn new(b: &'a Bucket) -> Self {
        Self {
            bucket: b,
            stack: RefCell::new(Vec::new()),
            _f: PhantomData,
        }
    }

    pub(crate) fn bucket(&self) -> &Bucket {
        self.bucket
    }

    pub(crate) fn bucket_mut(&mut self) -> &mut Bucket {
        unsafe { &mut *(self.bucket as *const Bucket as *mut Bucket) }
    }

    pub fn first(&mut self) -> Result<KVPair> {
        self.stack.borrow_mut().clear();
        let root_elem = self.bucket().page_node(self.bucket().root_id())?;
        self.stack.borrow_mut().push(ElementRef {
            page_node: root_elem,
            index: 0,
        });
        // look for the first leaf node
        self.first_leaf()?;
        //
        let pair = self.kv_pair()?;

        Ok(pair)
    }
    fn first_leaf(&self) -> Result<()> {
        loop {
            let stack = self.stack.borrow();
            let elem = stack.last().ok_or(anyhow!(RoltError::StackEmpty))?;
            // stop when find a leaf
            if elem.is_leaf() {
                break;
            }
            // if it is branch then go deeper
            let page_id = match elem.upgrade() {
                either::Either::Left(p) => p.branch_elements()?[elem.index].id,
                either::Either::Right(n) => n.inodes.borrow()[elem.index]
                    .page_id()
                    .ok_or(anyhow::anyhow!("does not have page id"))?,
            };
            let page_node = self.bucket().page_node(page_id)?;
            self.stack.borrow_mut().push(ElementRef {
                index: 0,
                page_node,
            })
        }
        Ok(())
    }
    // move to the next leaf element
    fn next_leaf(&self) -> Result<KVPair> {
        loop {}
    }
    pub fn last(&self) -> Result<KVPair> {
        todo!()
    }

    pub fn next(&self) -> Result<KVPair<'a>> {
        loop {
            let mut stack = self.stack.borrow_mut();
            let mut i = stack.len() as isize - 1;
            while i >= 0 {
                let e = &mut stack[i as usize];
                if e.index + 1 < e.count() {
                    e.index += 1;
                    break;
                }
                i -= 1;
            }
            // reach root page
            if i == -1 {
                return Ok(KVPair::null());
            }
            self.first_leaf()?;

            if self
                .stack
                .borrow()
                .last()
                .ok_or(anyhow!("empty stack"))?
                .count()
                != 0
            {
                return self.kv_pair();
            }
        }
    }
    pub fn prev(&self) -> Result<KVPair> {
        todo!()
    }

    pub(crate) fn seek(&mut self, target: &[u8]) -> Result<KVPair<'a>> {
        let mut pair = self.seek_to(target)?;
        let elem = self
            .stack
            .borrow()
            .last()
            .ok_or(anyhow!(RoltError::StackEmpty))?
            .clone();

        // last element of a page, move to the next one
        if elem.index >= elem.count() {
            pair = self.next()?;
        }
        Ok(pair)
    }

    // move cursor to a key
    pub(crate) fn seek_to(&mut self, target: &[u8]) -> Result<KVPair<'a>> {
        self.stack.borrow_mut().clear();
        let root_id = self.bucket().root_id();
        self.search(target, root_id)?;
        // if target is found
        let stack = self.stack.borrow();
        let elem = stack.last().ok_or(anyhow!(RoltError::StackEmpty))?;
        if elem.index >= elem.count() {
            Ok(KVPair::null())
        } else {
            self.kv_pair()
        }
    }

    // recursively look for the key
    fn search(&mut self, target: &[u8], id: PageId) -> Result<()> {
        // get node or page by id
        let page_node = self.bucket().page_node(id)?;
        let elem = ElementRef {
            index: 0,
            page_node,
        };
        self.stack.borrow_mut().push(elem.clone());
        // it is a leaf node
        if elem.is_leaf() {
            return self.search_leaf(target);
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
        self.stack
            .borrow_mut()
            .last_mut()
            .ok_or(anyhow!(RoltError::StackEmpty))?
            .index = index;

        // recursively search the next node
        self.search(target, branches[index].id)?;

        Ok(())
    }
    // find target key in a node
    fn search_node(&mut self, target: &[u8], n: &Node) -> Result<()> {
        let inodes = n.inodes.borrow();
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
        self.stack
            .borrow_mut()
            .last_mut()
            .ok_or(anyhow!(RoltError::StackEmpty))?
            .index = index;
        let page_id = inodes[index]
            .page_id()
            .ok_or(anyhow!("leaf inode does not have page id"))?;
        self.search(target, page_id)?;
        Ok(())
    }

    // search leaf node for the key
    fn search_leaf(&mut self, target: &[u8]) -> Result<()> {
        let mut stack = self.stack.borrow_mut();
        let elem = stack.last_mut().ok_or(anyhow!(RoltError::StackEmpty))?;
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
                    .inodes
                    .borrow()
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
        let stack = self.stack.borrow();
        let elem = stack.last().ok_or(anyhow!(RoltError::StackEmpty))?;
        Ok(KVPair::from(elem))
    }

    pub(crate) fn node(&mut self) -> Result<Node> {
        {
            let stack = self.stack.borrow();
            let elem = stack.last().ok_or(anyhow!(RoltError::StackEmpty))?;
            // leaf node is on the top of stack
            if elem.is_leaf() & elem.is_right() {
                return Ok(elem.as_ref().right().unwrap().clone());
            }
        }
        // begin from root node
        let elem = self.stack.borrow()[0].clone();
        let mut node = match elem.upgrade() {
            // read page
            either::Either::Left(p) => self.bucket_mut().node(p.id, WeakNode::default()),
            either::Either::Right(n) => n.clone(),
        };
        let len = self.stack.borrow().len();
        for e in &self.stack.borrow()[..len - 1] {
            let child = node.child_at(e.index)?;
            node = child;
        }
        Ok(node)
    }
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
    pub(crate) flags: u32,
}

#[allow(dead_code)]
impl<'a> KVPair<'a> {
    pub(crate) fn null() -> Self {
        Self {
            key: None,
            value: None,
            flags: 0,
        }
    }
    pub(crate) fn key(&self) -> Option<&'a [u8]> {
        self.key
    }
    pub(crate) fn value(&self) -> Option<&'a [u8]> {
        self.value
    }
    pub(crate) fn is_bucket(&self) -> bool {
        false
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
                        value: Some(&*(leaf.value() as *const [u8])),
                        flags: 0,
                    }
                }
                either::Either::Right(ref n) => {
                    let inode = &n.inodes.borrow()[elem.index];
                    let value = inode.value().ok_or("does not have value").unwrap();
                    Self {
                        key: Some(&*(inode.key().as_slice() as *const [u8])),
                        value: Some(&*(value.as_slice() as *const [u8])),
                        flags: 0,
                    }
                }
            }
        }
    }
}
