use crate::error::Result;
use crate::page::{Page, PageId};
use std::collections::{BTreeMap, BTreeSet};
struct FreeList {
    pending: BTreeMap<PageId, Vec<PageId>>,
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
    // allocate a sequence of free pages
    pub fn allocate(&mut self, len: usize) -> Option<PageId> {
        if self.free_pages.is_empty() || self.free_pages.len() < len {
            return None;
        }
        let mut start: PageId = 0;
        let mut prev: PageId = 0;
        for id in self.free_pages.iter().cloned() {
            // find gap
            if prev == 0 || id - prev != 1 {
                start = id;
            }
            if id - start - 1 >= len as u64 {
                for id in start..start + len as u64 {
                    self.free_pages.remove(&id);
                }
                return Some(start);
            }
            prev = id;
        }
        None
    }
    pub fn free(&mut self, tx_id: u64, p: &Page) -> Result<()> {
        let free_ids = self.pending.entry(tx_id).or_insert_with(Vec::new);

        Ok(())
    }
    pub fn is_free(&self, id: PageId) -> bool {
        self.free_pages.contains(&id)
    }
    // read from freeList page
    pub fn read(&mut self, p: &Page) -> Result<()> {
        let mut count = p.count as usize;
        let mut begin = 0;
        // count overflow
        if count == u16::MAX as usize {
            let list = p.free_list()?;
            count = list[0] as usize;
            // skip the first elem
            begin = 1;
        }
        if count == 0 {
            self.free_pages.clear();
        } else {
            let list = p.free_list()?;
            for id in list[begin..].iter() {
                self.free_pages.insert(*id);
            }
        }
        Ok(())
    }
    pub fn write(&self, p: &mut Page) -> Result<()> {
        let count = self.count();
        p.page_type = Page::FREE_LIST_PAGE;
        if count == 0 {
            p.count = 0;
        } else if count < u16::MAX as usize {
            // count is in range of u16
            p.count = count as u16;
            let list = p.free_list_mut()?;
            list.copy_from_slice(&self.page_ids());
        } else {
            p.count = u16::MAX;
            let list = p.free_list_mut()?;
            list[0] = count as u64;
            list.copy_from_slice(&self.page_ids());
        }
        Ok(())
    }

    fn count(&self) -> usize {
        self.free_pages.len() + self.pending_count()
    }
    fn pending_count(&self) -> usize {
        self.pending.iter().fold(0, |acc, cur| acc + cur.1.len())
    }
    fn page_ids(&self) -> Vec<PageId> {
        let mut ids = Vec::with_capacity(self.count());
        let free_pages: Vec<PageId> = self.free_pages.iter().map(|x| *x).collect();
        ids.extend_from_slice(&free_pages);
        for list in self.pending.values() {
            ids.extend_from_slice(list);
        }
        ids.sort_unstable();
        ids
    }
}
