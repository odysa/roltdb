use std::{
    marker::PhantomData,
    mem::size_of,
    ops::{Add, Deref, DerefMut},
    slice::{from_raw_parts, from_raw_parts_mut},
};

use memoffset::offset_of;

use crate::{
    error::{Result, RoltError},
    meta::Meta,
    Err,
};

pub type PageType = u8;
pub type PageId = u64;

#[derive(Debug, Clone)]
#[repr(C)]
pub(crate) struct Page {
    pub(crate) id: PageId,
    pub(crate) page_type: PageType,
    pub(crate) count: u16,
    pub(crate) overflow: u32, // 0 means page allocated in one page block, 1 means 2 blocks
    pub(crate) ptr: PhantomData<u8>,
}

impl Page {
    pub const BRANCH_PAGE: PageType = 0x01; // index
    pub const LEAF_PAGE: PageType = 0x02; // data
    pub const META_PAGE: PageType = 0x03; // meta data
    pub const FREE_LIST_PAGE: PageType = 0x04; // free pages

    pub fn ptr(&self) -> *const u8 {
        &self.ptr as *const PhantomData<u8> as *const u8
    }
    pub fn ptr_mut(&mut self) -> *mut u8 {
        &mut self.ptr as *mut PhantomData<u8> as *mut u8
    }
    pub(crate) fn PAGE_HEADER_SIZE() -> usize {
        offset_of!(Self, ptr)
    }

    pub(crate) fn is_leaf(&self) -> bool {
        self.page_type == Self::LEAF_PAGE
    }
    // dereference meta data
    pub(crate) fn meta(&self) -> Result<&Meta> {
        match self.page_type {
            Page::META_PAGE => unsafe {
                let meta = &*(self.ptr() as *const Meta);
                Ok(meta)
            },
            _ => Err!(RoltError::InvalidPageType),
        }
    }
    pub(crate) fn meta_mut(&mut self) -> Result<&mut Meta> {
        match self.page_type {
            Page::META_PAGE => unsafe {
                let meta = &mut *(self.ptr_mut() as *mut Meta);
                Ok(meta)
            },
            _ => Err!(RoltError::InvalidPageType),
        }
    }
    pub fn free_list(&self) -> Result<&[PageId]> {
        match self.page_type {
            Page::FREE_LIST_PAGE => unsafe {
                let addr = self.ptr() as *const PageId;
                Ok(from_raw_parts(addr, self.count as usize))
            },
            _ => Err!(RoltError::InvalidPageType),
        }
    }

    pub fn free_list_mut(&mut self) -> Result<&mut [PageId]> {
        unsafe {
            let start = self.ptr_mut() as *mut PageId;
            let list = from_raw_parts_mut(start, self.count as usize);
            Ok(list)
        }
    }

    pub fn branch_elements(&self) -> Result<&[BranchPageElement]> {
        match self.page_type {
            Page::BRANCH_PAGE => unsafe {
                let addr = self.ptr() as *const BranchPageElement;
                Ok(from_raw_parts(addr, self.count as usize))
            },
            _ => Err!(RoltError::InvalidPageType),
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
            Page::LEAF_PAGE => unsafe {
                let addr = self.ptr() as *const LeafPageElement;
                Ok(from_raw_parts(addr, self.count as usize))
            },
            _ => Err!(RoltError::InvalidPageType),
        }
    }
    pub fn leaf_elements_mut(&self) -> Result<&mut [LeafPageElement]> {
        unsafe {
            let elem = self.leaf_elements()?;
            let elem = elem as *const [LeafPageElement] as *mut [LeafPageElement];
            Ok(&mut *elem)
        }
    }
    // get a page from buffer
    pub(crate) fn from_buf(buf: &[u8], id: PageId, page_size: u64) -> &Page {
        unsafe { &*(buf[(id * page_size) as usize..].as_ptr() as *const u8 as *const Page) }
    }
    pub(crate) fn from_buf_mut(buf: &mut [u8], id: PageId, page_size: u64) -> &mut Page {
        unsafe { &mut *(buf[(id * page_size) as usize..].as_mut_ptr() as *mut Page) }
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
            let addr = (self as *const LeafPageElement as *const u8).add(pos);
            from_raw_parts(addr, (self.pos + self.k_size) as usize)
        }
    }
    pub fn value(&self) -> &[u8] {
        unsafe {
            let pos = (self.pos + self.k_size) as usize;
            let addr = (self as *const LeafPageElement as *const u8).add(pos);
            from_raw_parts(addr, self.v_size as usize)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VPage {
    data: Vec<u8>,
}

impl VPage {
    pub(crate) fn new(size: usize) -> Self {
        Self {
            data: vec![0u8; size],
        }
    }
    pub(crate) fn data_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }
}

impl Deref for VPage {
    type Target = Page;
    fn deref(&self) -> &Self::Target {
        unsafe { &*(self.data.as_ptr() as *const Page) }
    }
}

impl DerefMut for VPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *(self.data.as_mut_ptr() as *mut Page) }
    }
}
