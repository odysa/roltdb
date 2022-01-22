use crate::page::PageId;
use std::collections::BTreeMap;
struct FreeList {
    ids: Vec<PageId>,
    pending: BTreeMap<u64, Vec<PageId>>,
    free_pages: BTreeMap<PageId, bool>, // in-memory look up
}

impl FreeList {
    pub fn new() -> FreeList {
        FreeList {
            pending: BTreeMap::new(),
            ids: Vec::new(),
            free_pages: BTreeMap::new(),
        }
    }
}
