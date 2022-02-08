use either::Either;

use crate::{data::Entry, page::PageId, Bucket};

#[derive(Debug, Clone)]
pub(crate) struct Inode(pub(crate) Either<BranchINode, LeafINode>);

impl Inode {
    pub(crate) fn key(&self) -> &Vec<u8> {
        match &self.0 {
            Either::Left(b) => &b.key,
            Either::Right(l) => &l.key,
        }
    }
    pub(crate) fn value(&self) -> Option<&Vec<u8>> {
        match &self.0 {
            Either::Left(_) => None,
            Either::Right(l) => Some(&l.value),
        }
    }
    pub(crate) fn page_id(&self) -> Option<PageId> {
        match &self.0 {
            Either::Left(b) => Some(b.page_id),
            Either::Right(_) => None,
        }
    }
    pub(crate) fn flags(&self) -> u32 {
        match &self.0 {
            Either::Left(b) => b.flags,
            Either::Right(l) => l.flags,
        }
    }
    pub(crate) fn is_bucket(&self) -> bool {
        self.flags() == Bucket::FLAG
    }
}
impl From<BranchINode> for Inode {
    fn from(node: BranchINode) -> Self {
        Self(Either::Left(node))
    }
}

impl From<LeafINode> for Inode {
    fn from(node: LeafINode) -> Self {
        Self(Either::Right(node))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BranchINode {
    pub(crate) flags: u32,
    pub(crate) key: Entry,
    pub(crate) page_id: PageId,
}

#[derive(Debug, Clone)]
pub(crate) struct LeafINode {
    pub(crate) flags: u32,
    pub(crate) key: Entry,
    pub(crate) value: Entry,
}
