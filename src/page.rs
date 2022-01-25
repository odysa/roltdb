use std::{mem::size_of, slice::from_raw_parts};

use crate::{
    error::{Error, Result},
    meta::Meta,
};

pub type PageType = u8;
pub type PageId = u64;
#[repr(C)]
pub struct Page {
    pub(crate) count: u16,
    pub(crate) overflow: u32,
    pub(crate) id: PageId,
    ptr: u8,
    pub(crate) page_type: PageType,
}

impl Page {
    pub const BRANCH_PAGE: PageType = 0x01; // index
    pub const LEAF_PAGE: PageType = 0x02; // data
    pub const META_PAGE: PageType = 0x03; // meta data
    pub const FREE_LIST_PAGE: PageType = 0x04; // free pages
    pub fn new() -> Page {
        Page {
            count: 0,
            overflow: 0,
            id: 0,
            ptr: 0,
            page_type: 0x1,
        }
    }
    pub fn ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }
    pub fn ptr_mut(&mut self) -> *mut u8 {
        &mut self.ptr as *mut u8
    }
    // dereference meta data
    pub fn meta(&self) -> Result<&Meta> {
        match self.page_type {
            Page::META_PAGE => Err(Error::InvalidPageType),
            _ => unsafe {
                let meta_ptr = self.ptr as *const Meta;
                let meta = &*meta_ptr;
                Ok(meta)
            },
        }
    }
    pub fn free_list(&self) -> Result<&[PageId]> {
        match self.page_type {
            Page::FREE_LIST_PAGE => Err(Error::InvalidPageType),
            _ => unsafe {
                let addr = self.ptr as *const PageId;
                Ok(from_raw_parts(addr, self.count as usize))
            },
        }
    }

    pub fn free_list_mut(&self) -> Result<&mut [PageId]> {
        unsafe {
            let list = self.free_list()?;
            let list = list as *const [PageId] as *mut [PageId];
            Ok(&mut *list)
        }
    }

    pub fn branch_elements(&self) -> Result<&[BranchPageElement]> {
        match self.page_type {
            Page::BRANCH_PAGE => Err(Error::InvalidPageType),
            _ => unsafe {
                let addr = self.ptr as *const u64 as *const BranchPageElement;
                Ok(from_raw_parts(addr, self.count as usize))
            },
        }
    }
    pub fn branch_elements_mut(&self) -> Result<&mut [BranchPageElement]> {
        unsafe {
            let elem = self.branch_elements()?;
            let elem = elem as *const [BranchPageElement] as *mut [BranchPageElement];
            Ok(&mut *elem)
        }
    }
    pub fn leaf_elements(&self) -> Result<&[LeafPageElement]> {
        match self.page_type {
            Page::LEAF_PAGE => Err(Error::InvalidPageType),
            _ => unsafe {
                let addr = self.ptr as *const u64 as *const LeafPageElement;
                Ok(from_raw_parts(addr, self.count as usize))
            },
        }
    }
    pub fn leaf_elements_mut(&self) -> Result<&mut [LeafPageElement]> {
        unsafe {
            let elem = self.leaf_elements()?;
            let elem = elem as *const [LeafPageElement] as *mut [LeafPageElement];
            Ok(&mut *elem)
        }
    }
}

#[repr(C)]
pub struct BranchPageElement {
    // offset to key
    pub(crate) pos: u32,
    pub(crate) k_size: u32,
    pub(crate) id: PageId,
}

impl BranchPageElement {
    pub(crate) const SIZE: usize = size_of::<Self>();
    pub fn key(&self) -> &[u8] {
        unsafe {
            let pos = self.pos as usize;
            let addr = self as *const BranchPageElement as *const u8;
            let buffer = from_raw_parts(addr, (self.pos + self.k_size) as usize);
            &buffer[pos..]
        }
    }
}

#[repr(C)]
pub struct LeafPageElement {
    // offset to key and value
    pub(crate) pos: u32,
    pub(crate) k_size: u32,
    pub(crate) v_size: u32,
}

impl LeafPageElement {
    pub(crate) const SIZE: usize = size_of::<Self>();
    pub fn key(&self) -> &[u8] {
        unsafe {
            let pos = self.pos as usize;
            let addr = self as *const LeafPageElement as *const u8;
            let buffer = from_raw_parts(addr, (self.pos + self.k_size) as usize);
            &buffer[pos..]
        }
    }
    pub fn value(&self) -> &[u8] {
        unsafe {
            let pos = (self.pos + self.k_size) as usize;
            let addr = self as *const LeafPageElement as *const u8;
            let buffer = from_raw_parts(addr, (self.pos + self.k_size) as usize);
            &buffer[pos..]
        }
    }
}
