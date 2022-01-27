use anyhow::anyhow;
use fs2::FileExt;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    ops::Deref,
    path::Path,
    rc::{Rc, Weak},
    sync::Mutex,
};

use memmap::{Mmap, MmapOptions};

use crate::{error::Result, free_list::FreeList, meta::Meta, page::Page};

#[derive(Debug)]
pub struct DB(pub Rc<IDB>);
#[derive(Debug)]
pub struct WeakDB(pub Weak<IDB>);

#[derive(Debug)]
pub struct IDB {
    mmap: Mmap,
    file: Mutex<File>,
    page_size: u64,
    free_list: FreeList,
}

impl IDB {
    pub fn open(file: File) -> Result<Self> {
        let page_size = page_size::get() as u64;

        let mmap = unsafe {
            MmapOptions::new()
                .offset(0)
                .len(page_size as usize)
                .map(&file)?
        };

        let mut db = IDB {
            mmap,
            page_size,
            file: Mutex::new(file),
            free_list: FreeList::new(),
        };
        let meta = db.meta()?;
        let free_list = Page::from_buf(&db.mmap, meta.free_list, page_size)
            .free_list()
            .unwrap();
        if !free_list.is_empty() {
            db.free_list.init(free_list);
        }
        Ok(db)
    }
    fn meta(&self) -> Result<Meta> {
        let buf = &self.mmap;
        let meta0 = Page::from_buf(buf, 0, self.page_size).meta()?;
        let meta1 = Page::from_buf(buf, 1, self.page_size).meta()?;
        let meta = match (meta0.validate(), meta1.validate()) {
            (true, true) => {
                if meta0.tx_id > meta1.tx_id {
                    meta0
                } else {
                    meta1
                }
            }
            (true, false) => meta0,
            (false, true) => meta1,
            (false, false) => panic!("both meta not valid"),
        };
        Ok(meta.clone())
    }
    fn init_file(p: &Path, page_size: u64, page_num: u64) -> Result<File> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(p)?;
        file.allocate(page_size * page_num)?;
        // allocate 4 pages
        let mut buf: Vec<u8> = vec![0; (page_size * 4) as usize];
        // init meta pages
        for i in 0..4 {
            let page =
                unsafe { &mut *(&mut buf[(i * page_size) as usize] as *mut u8 as *mut Page) };
            if i < 2 {
                page.page_type = Page::META_PAGE;
                let m = page.meta_mut()?;
                m.init(i);
            } else if i == 2 {
                // init free list
                page.id = 2;
                page.page_type = Page::FREE_LIST_PAGE;
                page.count = 0;
            } else {
                page.id = 3;
                page.page_type = Page::LEAF_PAGE;
                page.count;
            }
        }
        file.write_all(&buf[..])?;
        file.flush()?;
        file.sync_all()?;
        Ok(file)
    }
}

impl Deref for DB {
    type Target = IDB;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
