use std::{cell::RefCell, rc::Rc, sync::Arc};

use crate::{bucket::Bucket, node::Node, page::Page};

struct Cursor {
    bucket: Arc<Bucket>,
    stack: Vec<ElementRef>,
}
impl Cursor {
    pub fn new(b: Arc<Bucket>) -> Self {
        Self {
            bucket: b,
            stack: Vec::new(),
        }
    }
    pub fn bucket(&self) -> Arc<Bucket> {
        self.bucket.clone()
    }
    // pub fn first(&self) -> (Option<Entry>, Option<Entry>) {

    // }
}
struct ElementRef {
    index: usize,
    node: RefCell<Node>,
    page: RefCell<Page>,
}
