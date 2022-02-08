use crate::error::{Result, RoltError};
use crate::page::{Page, PageId};
use crate::Err;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::mem::size_of;

#[derive(Debug)]
#[repr(C)]
pub(crate) struct FreeList {
    pending: BTreeMap<PageId, Vec<PageId>>,
    free_pages: BTreeSet<PageId>, // in-memory look up
    cache: HashSet<PageId>,
}

#[allow(dead_code)]
impl FreeList {
    pub fn new() -> FreeList {
        FreeList {
            pending: BTreeMap::new(),
            free_pages: BTreeSet::new(),
            cache: HashSet::new(),
        }
    }
    pub fn init(&mut self, free_pages: &[PageId]) {
        for id in free_pages {
            self.free_pages.insert(*id);
            self.cache.insert(*id);
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
            if id - start + 1 >= len as u64 {
                for id in start..start + len as u64 {
                    self.free_pages.remove(&id);
                    self.cache.remove(&id);
                }
                return Some(start);
            }
            prev = id;
        }
        None
    }

    // release a page for a transaction
    pub fn free(&mut self, tx_id: u64, p: &Page) -> Result<()> {
        let free_ids = self.pending.entry(tx_id).or_insert_with(Vec::new);
        for id in (p.id)..=(p.id + p.overflow as PageId) {
            if self.free_pages.contains(&id) {
                return Err!(RoltError::InodeOverFlow);
            }
            free_ids.push(id);
            self.cache.insert(id);
        }
        Ok(())
    }

    pub fn is_free(&self, id: PageId) -> bool {
        self.cache.contains(&id)
    }
    // remove pages from a given tx id
    pub fn rollback(&mut self, tx_id: u64) {
        if let Some(pages) = self.pending.get(&tx_id) {
            for id in pages {
                self.cache.remove(id);
            }
        }
        self.pending.remove(&tx_id);
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

    pub fn count(&self) -> usize {
        self.free_pages.len() + self.pending_count()
    }

    fn pending_count(&self) -> usize {
        self.pending.iter().fold(0, |acc, cur| acc + cur.1.len())
    }

    fn page_ids(&self) -> Vec<PageId> {
        let mut ids = Vec::with_capacity(self.count());
        let free_pages: Vec<PageId> = self.free_pages.iter().copied().collect();
        ids.extend_from_slice(&free_pages);
        for list in self.pending.values() {
            ids.extend_from_slice(list);
        }
        ids.sort_unstable();
        ids
    }

    // rebuild cache
    fn reindex(&mut self) {
        self.cache = HashSet::new();
        for id in self.free_pages.iter() {
            self.cache.insert(*id);
        }
        for pages in self.pending.values() {
            for id in pages {
                self.cache.insert(*id);
            }
        }
    }
    pub(crate) fn reload(&mut self, p: &Page) {
        self.read(p).unwrap();
        let mut t_cache = HashSet::new();
        for (_, ids) in self.pending.iter() {
            for id in ids.iter() {
                t_cache.insert(*id);
            }
        }
        // add pages not in pending list to free pages
        let mut free_pages = BTreeSet::new();
        for id in self.free_pages.iter() {
            if !t_cache.contains(id) {
                free_pages.insert(*id);
            }
        }
        self.free_pages = free_pages;
        self.reindex();
    }
    pub(crate) fn size(&self) -> usize {
        let n = if self.count() > 0xFFF {
            self.count() + 1
        } else {
            self.count()
        };
        Page::page_header_size() + (size_of::<PageId>() * n)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_write() {
        let mut list = FreeList::new();
        let mut b1 = vec![0u8; 4096];
        let mut b2 = vec![0u8; 4096];

        let mut p1 = Page::from_buf_mut(&mut b1, 0, 0);
        p1.id = 2;
        let p2 = Page::from_buf_mut(&mut b2, 0, 0);
        list.free(0, &p1).unwrap();
        list.write(p2).unwrap();
        let _ = p2.free_list().unwrap();
    }
}
