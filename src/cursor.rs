use std::{cell::RefCell, rc::Rc};

use crate::{bucket::Bucket, data::Entry, node::Node, page::Page};

struct Cursor {
    bucket: Rc<Bucket>,
    stack: Vec<ElementRef>,
}
impl Cursor {
    pub fn new(b: Rc<Bucket>) -> Self {
        Self {
            bucket: b,
            stack: Vec::new(),
        }
    }
    pub fn bucket(&self) -> Rc<Bucket> {
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
