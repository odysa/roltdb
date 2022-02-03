use anyhow::anyhow;
use fs2::FileExt;
use memmap::{Mmap, MmapOptions};
use parking_lot::{Mutex, RwLock};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    ops::{Deref, DerefMut},
    path::Path,
    rc::{Rc, Weak},
};

use crate::{
    error::Result,
    free_list::FreeList,
    meta::Meta,
    page::{Page, PageId},
    transaction::Transaction,
};

#[derive(Debug)]
pub struct DB(pub Rc<IDB>);
#[derive(Debug)]
pub struct WeakDB(pub Weak<IDB>);

pub struct DBBuilder {
    page_size: u64,
    num_pages: u64,
}

impl DBBuilder {
    pub fn page_size(mut self, size: u64) -> Self {
        self.page_size = size;
        self
    }
    pub fn num_pages(mut self, num: u64) -> Self {
        if num < 4 {
            panic!("Must have 4 pages or mode");
        }
        self.num_pages = num;
        self
    }
    pub fn open<P: AsRef<Path>>(&self, p: P) -> Result<DB> {
        let p = p.as_ref();
        let f = if !p.exists() {
            IDB::init_file(p, self.page_size, self.num_pages)?
        } else {
            OpenOptions::new().read(true).write(true).open(p)?
        };
        let db = IDB::open(f)?;
        Ok(DB(Rc::new(db)))
    }
}

impl DB {
    pub fn open<P: AsRef<Path>>(p: P) -> Result<DB> {
        DBBuilder::default().open(p)
    }
    pub fn tx(&self, writable: bool) -> Transaction {
        Transaction::new(WeakDB::from(self), writable)
    }
    pub(crate) fn write_at<T: Read>(&mut self, addr: u64, mut buf: T) -> Result<()> {
        let mut file = self.file.lock(); // unlock automatically
        file.seek(SeekFrom::Start(addr))
            .map_err(|_| anyhow!("can't write db file at give position"))?;
        std::io::copy(&mut buf, &mut *file)?;
        Ok(())
    }
}

impl Default for DBBuilder {
    fn default() -> Self {
        Self {
            page_size: page_size::get() as u64,
            num_pages: 32,
        }
    }
}

#[derive(Debug)]
pub struct IDB {
    mmap: RwLock<Mmap>,
    file: Mutex<File>,
    page_size: u64,
    pub(crate) free_list: RwLock<FreeList>,
}

impl IDB {
    pub(crate) fn page_size(&self) -> u64 {
        self.page_size
    }
    pub fn open(file: File) -> Result<Self> {
        let page_size = page_size::get() as u64;

        let mmap = unsafe { MmapOptions::new().offset(0).map(&file)? };

        let db = IDB {
            mmap: RwLock::new(mmap),
            page_size,
            file: Mutex::new(file),
            free_list: RwLock::new(FreeList::new()),
        };
        {
            let meta = db.meta()?;
            let buf = db.mmap.read();
            let buf = buf.as_ref();
            let free_list = Page::from_buf(buf, meta.free_list, page_size).free_list()?;
            if !free_list.is_empty() {
                db.free_list.write().init(free_list);
            }
        }
        Ok(db)
    }
    pub(crate) fn meta(&self) -> Result<Meta> {
        let buf = self.mmap.read();
        let buf = buf.as_ref();
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
    // init an empty file
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
                page.id = i;
                let m = page.meta_mut()?;
                // must before init
                m.free_list = 2;
                m.num_pages = 4;
                m.init(i);
            } else if i == 2 {
                // init free list
                page.id = 2;
                page.page_type = Page::FREE_LIST_PAGE;
                page.count = 0;
            } else {
                page.id = 3;
                page.page_type = Page::LEAF_PAGE;
                page.count = 0;
            }
        }
        file.write_all(&buf[..])?;
        file.flush()?;
        file.sync_all()?;
        Ok(file)
    }

    // get a page from mmap
    pub(crate) fn page(&self, id: PageId) -> &Page {
        let page_size = self.page_size;
        self.page_from_buf(id, page_size)
    }
    pub(crate) fn page_from_buf(&self, id: PageId, page_size: u64) -> &Page {
        let buf = self.mmap.read();
        unsafe { &*(&buf[(id * page_size) as usize] as *const u8 as *const Page) }
    }
}

impl Deref for DB {
    type Target = IDB;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl WeakDB {
    pub(crate) fn upgrade(&self) -> Option<DB> {
        self.0.upgrade().map(DB)
    }
}

impl From<&DB> for WeakDB {
    fn from(db: &DB) -> Self {
        Self(Rc::downgrade(&db.0))
    }
}
