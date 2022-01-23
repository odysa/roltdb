use std::collections::HashMap;

use crate::{
    node::Node,
    page::{Page, PageId},
};

// a collection of kev-value pairs
pub(crate) struct Bucket {
    bucket: IBucket,
    buckets: HashMap<String, Bucket>,
    fill_percent: f64,
    root: Option<Node>,
    nodes: HashMap<PageId, Node>,
    page: Option<Page>,
}

// on-file representation of bucket
struct IBucket {
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
