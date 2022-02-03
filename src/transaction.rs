use crate::{
    bucket::Bucket,
    data::RawPtr,
    db::{WeakDB, DB},
    error::Result,
    meta::Meta,
    page::{Page, PageId},
};
use anyhow::anyhow;
use parking_lot::{MappedRwLockWriteGuard, RwLock, RwLockWriteGuard};
use std::{
    collections::HashMap,
    io::Cursor,
    ops::Deref,
    rc::{Rc, Weak},
    slice::from_raw_parts,
};
pub type TXID = u64;
#[derive(Debug, Clone)]
pub struct Transaction(pub(crate) Rc<ITransaction>);

#[derive(Debug, Clone)]
pub struct WeakTransaction(pub(crate) Weak<ITransaction>);

#[derive(Debug)]
pub struct ITransaction {
    pub(crate) writable: bool,
    db: RwLock<WeakDB>,
    managed: bool,
    root: RwLock<Bucket>,
    pages: RwLock<HashMap<PageId, RawPtr<Page>>>,
    meta: RwLock<Meta>,
    // commit_handlers: Vec<Box<dyn Fn()>>, // call functions after commit
}

impl Transaction {
    pub fn new(db: WeakDB, writable: bool) -> Self {
        let tx = Self(Rc::new(ITransaction::new(db, writable)));
        {
            let mut b = tx.root.write();
            b.tx = WeakTransaction(Rc::downgrade(&tx));
            b.bucket = tx.meta.read().root.clone();
        }
        tx
    }
}

impl ITransaction {
    pub fn new(db: WeakDB, writable: bool) -> Self {
        let mut meta = match db.upgrade() {
            None => Meta::default(),
            Some(db) => db.meta().unwrap(),
        };
        if writable {
            meta.tx_id += 1;
        }
        let tx = ITransaction {
            db: RwLock::new(db),
            managed: false,
            // commit_handlers: Vec::new(),
            pages: RwLock::new(HashMap::new()),
            writable,
            meta: RwLock::new(meta),
            root: RwLock::new(Bucket::new(WeakTransaction::new())),
        };
        tx
    }

    pub fn page(&self, id: PageId) -> Result<RawPtr<Page>> {
        let pages = self.pages.read();
        if let Some(page) = pages.get(&id) {
            Ok(page.clone())
        } else {
            // get page from mmap
            Ok(RawPtr::new(self.db().page(id)))
        }
    }

    pub(crate) fn db(&self) -> DB {
        self.db.read().upgrade().unwrap()
    }

    pub fn create_bucket(&self, name: String) -> Result<MappedRwLockWriteGuard<Bucket>> {
        if !self.writable() {
            return Err(anyhow!("read-only tx cannot create bucket"));
        }
        let mut b = self.root.write();
        if !b.tx().unwrap().writable {
            println!("wired!");
        }
        RwLockWriteGuard::try_map(b, |b| b.create_bucket(name).ok())
            .map_err(|_| anyhow!("failed to create bucket"))
    }

    pub fn rollback(&self) -> Result<()> {
        let db = self.db();
        if self.writable {
            let tx_id = self.id();
            let mut free_list = db.free_list.write();
            free_list.rollback(tx_id);
            let free_list_id = db.meta()?.free_list;
            let free_list_page = db.page(free_list_id);
            // reload free_list
            free_list.reload(free_list_page);
        }
        // close tx
        Ok(())
    }
    // // close a transaction
    // fn close(&self) -> Result<()> {
    //     let mut db = self.db();
    //     todo!()
    //     Ok(())
    // }
    // write change to disk and update meta page
    pub fn commit(&mut self) -> Result<()> {
        if !self.writable() {
            return Err(anyhow!("cannot commit read-only tx"));
        }
        {
            let root = &mut *self.root.try_write().unwrap();
            // rebalance
            root.rebalance();
            // spill
            root.spill()?;
        }

        {
            let mut meta = self.meta.write();
            meta.root.root = self.root.read().bucket.root;
            let db = self.db();
            let mut free_list = db.free_list.write();
            let p = db.page(meta.free_list);
            // free free_list
            free_list.free(meta.tx_id, p)?;
        }
        {
            let db = self.db();
            let free_list = db.free_list.write();
            let free_list_size = free_list.size();
            let (page_id, _) = self.allocate(free_list_size as u64);
            let mut ptr = match self.page(page_id) {
                Ok(p) => p,
                Err(_) => panic!("cannot allocate page"),
            };
            let page = &mut *ptr;
            free_list.write(page)?;
            {
                self.meta.write().free_list = page.id;
            }
            // write dirty pages to disk
            if let Err(e) = self.write_pages() {
                self.rollback()?;
                return Err(e);
            }
            // write dirty pages to disk
            if let Err(e) = self.write_meta() {
                self.rollback()?;
                return Err(e);
            }
            // close tx
        }
        Ok(())
    }

    fn page_size(&self) -> u64 {
        self.db().page_size()
    }

    fn allocate(&mut self, bytes: u64) -> (PageId, u64) {
        let page_size = self.page_size();
        let num = if bytes % page_size == 0 {
            bytes / page_size
        } else {
            bytes / page_size + 1
        };
        let db = self.db();
        let page_id = match db.free_list.write().allocate(num as usize) {
            None => {
                let page_id = self.meta.read().num_pages;
                self.meta.write().num_pages += num;
                page_id
            }
            Some(id) => id,
        };
        (page_id, num)
    }
    // write pages to disk
    fn write_pages(&mut self) -> Result<()> {
        let mut pages: Vec<(PageId, RawPtr<Page>)> =
            self.pages.write().drain().map(|(id, p)| (id, p)).collect();
        pages.sort_by(|x, y| x.0.cmp(&y.0));

        let mut db = self.db();
        let page_size = db.page_size();
        // write pages to file

        for (page_id, p) in pages {
            let size = ((p.overflow + 1) as u64) * page_size;
            let offset = page_id * page_size;
            let buf = unsafe { from_raw_parts(p.ptr(), size as usize) };
            db.write_at(offset, Cursor::new(buf))?;
        }

        Ok(())
    }
    // write meta to disk
    fn write_meta(&mut self) -> Result<()> {
        let mut meta = self.meta.write();
        let mut db = self.db();
        let page_size = db.page_size();
        let buf: Vec<u8> = vec![0; page_size as usize];
        let p = Page::from_buf_mut(&buf, meta.page_id, page_size);
        meta.write(p)?;
        let offset = meta.page_id * page_size;
        db.write_at(offset, Cursor::new(buf))?;
        Ok(())
    }
    // write free_list to disk
    fn write_free_list(&mut self) -> Result<()> {
        Ok(())
    }
    pub fn writable(&self) -> bool {
        self.writable
    }
    pub(crate) fn id(&self) -> TXID {
        self.meta.read().tx_id
    }
    pub(crate) fn page_id(&self) -> PageId {
        self.meta.read().page_id
    }
}

impl Drop for ITransaction {
    fn drop(&mut self) {
        // rollback read-only tx
        if !self.writable {}
    }
}

impl Deref for Transaction {
    type Target = Rc<ITransaction>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl WeakTransaction {
    pub(crate) fn new() -> Self {
        Self(Weak::new())
    }
    pub(crate) fn upgrade(&self) -> Option<Transaction> {
        self.0.upgrade().map(Transaction)
    }
}
