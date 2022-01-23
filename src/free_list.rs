use crate::page::PageId;
use std::collections::{BTreeMap, BTreeSet};
struct FreeList {
    pending: BTreeMap<u64, Vec<PageId>>,
    free_pages: BTreeSet<PageId>, // in-memory look up
}

impl FreeList {
    pub fn new() -> FreeList {
        FreeList {
            pending: BTreeMap::new(),
            free_pages: BTreeSet::new(),
        }
    }
    pub fn init(&mut self, free_pages: &[PageId]) {
        for id in free_pages {
            self.free_pages.insert(*id);
        }
    }
}
