use std::{borrow::BorrowMut, cell::RefCell, sync::Arc, ops::Deref};

use crate::{
    bucket::{Bucket, PageNode},
    error::Result,
    node::Node,
    page::{Page, PageId},
};

struct Cursor {
    bucket: Bucket,
    stack: Vec<ElementRef>,
}
impl Cursor {
    pub fn new(b: Bucket) -> Self {
        Self {
            bucket: b,
            stack: Vec::new(),
        }
    }
    pub fn bucket(&self) -> &Bucket {
        &self.bucket
    }
    fn move_to(&mut self, target: &[u8]) {
        self.stack.clear();
    }

    // recursively look for the key
    fn search(&mut self, target: &[u8], id: PageId) -> Result<()> {
        let page_node = self.bucket.page_node(id)?;
        let elem = ElementRef {
            index: 0,
            page_node,
        };
        self.stack.push(elem.clone());
        Ok(())
    }
    // pub fn first(&self) -> (Option<Entry>, Option<Entry>) {

    // }
}
#[derive(Debug,Clone)]
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