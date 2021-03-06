use anyhow::anyhow;
use fs2::FileExt;
use memmap::Mmap;
use parking_lot::{Mutex, RwLock};

use crate::{
    error::{Result, RoltError},
    free_list::FreeList,
    meta::Meta,
    page::{Page, PageId},
    transaction::Transaction,
    Err,
};
use std::{
    cmp::Ordering as CmpOrdering,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    ops::Deref,
    path::Path,
    rc::{Rc, Weak},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Debug)]
pub struct DB(pub Rc<Idb>);
#[derive(Debug)]
pub struct WeakDB(pub Weak<Idb>);

pub struct DBBuilder {
    page_size: u64,
    num_pages: u64,
}

#[allow(dead_code)]
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
            Idb::init_file(p, self.page_size, self.num_pages)?
        } else {
            OpenOptions::new().read(true).write(true).open(p)?
        };
        let db = Idb::open(f)?;
        Ok(DB(Rc::new(db)))
    }
}

impl DB {
    pub fn open<P: AsRef<Path>>(p: P) -> Result<DB> {
        DBBuilder::default().open(p)
    }
    pub fn tx(&self, writable: bool) -> Result<Transaction> {
        if self.has_write.load(Ordering::Relaxed) {
            return Err!(RoltError::WritableTxNotAllowed);
        }
        if writable {
            self.has_write.store(true, Ordering::Relaxed);
        }
        Ok(Transaction::new(WeakDB::from(self), writable))
    }
    pub(crate) fn release_write_tx(&mut self) {
        self.has_write.store(false, Ordering::Relaxed);
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
pub struct Idb {
    // pub(crate) mmap: RwLock<Mmap>,
    pub(crate) mmap: Arc<Mmap>,
    file: Mutex<File>,
    page_size: u64,
    pub(crate) free_list: RwLock<FreeList>,
    has_write: AtomicBool,
}

#[allow(dead_code)]
impl Idb {
    pub(crate) fn page_size(&self) -> u64 {
        self.page_size
    }
    pub fn open(file: File) -> Result<Self> {
        file.lock_exclusive()?;
        let page_size = page_size::get() as u64;

        let mmap = unsafe { Mmap::map(&file)? };

        let db = Idb {
            mmap: Arc::new(mmap),
            page_size,
            file: Mutex::new(file),
            free_list: RwLock::new(FreeList::new()),
            has_write: AtomicBool::new(false),
        };
        {
            let meta = db.meta()?;
            let free_page = Page::from_buf(&db.mmap, meta.free_list, page_size);
            let free_list = free_page.free_list()?;
            if !free_list.is_empty() {
                db.free_list.write().init(free_list);
            }
        }
        Ok(db)
    }
    pub(crate) fn meta(&self) -> Result<Meta> {
        let buf = self.mmap.as_ref();
        let meta0 = Page::from_buf(buf, 0, self.page_size).meta()?;
        let meta1 = Page::from_buf(buf, 1, self.page_size).meta()?;
        let meta = match (meta0.validate(), meta1.validate()) {
            (true, true) => {
                if meta0.tx_id >= meta1.tx_id {
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
        let mut buf = vec![0u8; (page_size * 4) as usize];
        // init meta pages
        for i in 0..4 {
            let page =
                unsafe { &mut *(&mut buf[(i * page_size) as usize] as *mut u8 as *mut Page) };
            match i.cmp(&2) {
                CmpOrdering::Less => {
                    page.page_type = Page::META_PAGE;
                    page.id = i;
                    let m = page.meta_mut()?;
                    // must before init
                    m.free_list = 2;
                    m.num_pages = 4;
                    m.init(i);
                }
                CmpOrdering::Equal => {
                    // init free list
                    page.id = 2;
                    page.page_type = Page::FREE_LIST_PAGE;
                    page.count = 0;
                }
                CmpOrdering::Greater => {
                    page.id = 3;
                    page.page_type = Page::LEAF_PAGE;
                    page.count = 0;
                }
            }
        }
        file.write_all(&buf[..])?;
        file.flush()?;
        file.sync_all()?;
        Ok(file)
    }

    // get a page from mmap
    pub(crate) fn page(&self, id: PageId) -> &Page {
        let p = Page::from_buf(self.mmap.as_ref(), id, self.page_size);
        p
    }

    pub(crate) fn resize_mmap(&mut self, size: u64) -> Result<()> {
        let f = self.file.lock();
        f.allocate(size)?;
        let new_mmap = unsafe { Mmap::map(&f).unwrap() };
        self.mmap = Arc::new(new_mmap);
        Ok(())
    }
    pub(crate) fn sync(&self) -> Result<()> {
        let mut f = self.file.lock();
        f.flush().map_err(|_| anyhow!("cannot sync data to file"))
    }
}

impl Deref for DB {
    type Target = Idb;
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

#[cfg(test)]
mod tests {
    use crate::data::RawPtr;

    use super::*;
    #[test]
    fn test_b() {
        unsafe {
            let v = vec![0u8; 1000];
            let p = &*(v.as_ptr() as *const u8 as *const Page);
            let mut p = RawPtr::new(p);
            let mut p = &mut *p;
            p.id = 1;
            p.page_type = 4;
        }
    }
}
