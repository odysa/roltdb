use crate::page::{Page, PageId};
pub struct Meta {
    page_id: PageId,
    magic_number: u32,
    version: u32,
    page_size: u32,
    free_list: PageId, // page id of free list
    tx_id: PageId,
}

impl Meta {}
