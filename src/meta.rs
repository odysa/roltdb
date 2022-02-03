use fnv::FnvHasher;
use std::{hash::Hasher, intrinsics::copy_nonoverlapping, mem::size_of, slice::from_raw_parts};

use crate::{
    bucket::IBucket,
    error::Result,
    page::{Page, PageId},
    transaction::TXID,
    utils::struct_to_slice,
};

#[derive(Debug, Clone)]
#[repr(C)]
pub(crate) struct Meta {
    pub(crate) page_id: PageId,
    pub(crate) magic_number: u32,
    pub(crate) version: u32,
    pub(crate) page_size: u32,
    pub(crate) free_list: PageId, // page id of free list
    pub(crate) tx_id: TXID,
    pub(crate) root: IBucket,
    pub(crate) num_pages: PageId,
    check_sum: u64,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            free_list: 0,
            page_id: 0,
            tx_id: 0,
            check_sum: 0,
            root: IBucket::new(),
            magic_number: Meta::MAGIC,
            version: Meta::VERSION,
            page_size: page_size::get() as u32,
            num_pages: 0,
        }
    }
}
impl Meta {
    const MAGIC: u32 = 0xF0F43F;
    const VERSION: u32 = 1;
    const META_SIZE: usize = size_of::<Self>();
    const SUM_SIZE: usize = size_of::<u64>();
    pub fn init(&mut self, page_id: PageId) {
        self.page_id = page_id;
        self.magic_number = Self::MAGIC;
        self.version = Self::VERSION;
        self.root = IBucket {
            root: 3,
            sequence: 0,
        };
        self.check_sum = self.sum64();
    }
    // write meta to the given page
    pub fn write(&mut self, p: &mut Page) -> Result<()> {
        // either 0 or 1
        p.id = self.tx_id % 2;
        self.check_sum = self.sum64();
        unsafe {
            let bytes = struct_to_slice(self);
            // copy meta to the page data
            copy_nonoverlapping(bytes.as_ptr(), p.ptr_mut(), bytes.len());
            p.count = 0;
            p.page_type = Page::META_PAGE;
        }

        Ok(())
    }
    fn sum64(&self) -> u64 {
        let mut hash = FnvHasher::default();
        let buf: &[u8] = unsafe {
            from_raw_parts(
                self as *const Self as *const u8,
                Self::META_SIZE - Self::SUM_SIZE,
            )
        };
        hash.write(buf);
        hash.finish()
    }
    pub(crate) fn validate(&self) -> bool {
        self.magic_number == Self::MAGIC && self.check_sum == self.sum64()
    }
}
