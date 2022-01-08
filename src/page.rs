use std::slice::from_raw_parts;

use crate::error::{Error, Result};

pub type PageType = u8;
type PageId = u64;
#[repr(C)]
pub struct Page {
    count: u16,
    overflow: u32,
    id: PageId,
    ptr: usize,
    page_type: PageType,
}

pub struct Meta {
    id: i32,
}

impl Page {
    pub const BRANCH_PAGE: PageType = 0x01;
    pub const LEAF_PAGE: PageType = 0x02;
    pub const META_PAGE: PageType = 0x03;
    pub const FREE_LIST_PAGE: PageType = 0x04;
    pub fn new() -> Page {
        Page {
            count: 0,
            overflow: 0,
            id: 0,
            ptr: 0,
            page_type: 0x1,
        }
    }
    // dereference meta data
    pub fn meta(&self) -> Result<&Meta> {
        if self.page_type != Page::BRANCH_PAGE {
            Err(Error::InvalidPageType)
        } else {
            unsafe {
                let meta_ptr = self.ptr as *const Meta;
                let meta = &*meta_ptr;
                Ok(meta)
            }
        }
    }
    pub fn free_list(&self) -> Result<&[PageId]> {
        if self.page_type != Page::FREE_LIST_PAGE {
            Err(Error::InvalidPageType)
        } else {
            unsafe {
                let addr = self.ptr as *const PageId;
                Ok(from_raw_parts(addr, self.count as usize))
            }
        }
    }
    pub fn branch_elements(&self) -> Result<&[BranchPageElement]> {
        if self.page_type != Page::BRANCH_PAGE {
            Err(Error::InvalidPageType)
        } else {
            unsafe {
                let addr = self.ptr as *const u64 as *const BranchPageElement;
                Ok(from_raw_parts(addr, self.count as usize))
            }
        }
    }
    pub fn leaf_elements(&self) -> Result<&[LeafPageElement]> {
        if self.page_type != Page::BRANCH_PAGE {
            Err(Error::InvalidPageType)
        } else {
            unsafe {
                let addr = self.ptr as *const u64 as *const LeafPageElement;
                Ok(from_raw_parts(addr, self.count as usize))
            }
        }
    }
}

#[repr(C)]
pub struct BranchPageElement {
    pos: u32,
    k_size: u32,
    id: PageId,
}

#[repr(C)]
pub struct LeafPageElement {
    pos: u32,
    k_size: u32,
    v_size: u32,
}
