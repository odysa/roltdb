use std::{cell::RefCell, intrinsics::copy_nonoverlapping, rc::Weak, sync::Arc};

use either::Either;

use crate::{
    bucket::Bucket,
    data::Entry,
    error::{Error, Result},
    page::{BranchPageElement, LeafPageElement, Page, PageId},
};

type NodeId = u64;
#[derive(Default, Clone, Debug)]
pub struct Node(pub(crate) Arc<RefCell<InnerNode>>);

#[derive(Default, Debug)]
pub(crate) struct InnerNode {
    bucket: Option<Bucket>,
    page_id: PageId,
    unbalanced: bool,
    spilled: bool,
    inodes: Vec<Inode>,
    children: Vec<NodeId>,
    parent: RefCell<Weak<Node>>,
    node_type: NodeType,
    key: Option<Entry>,
}

impl Node {
    pub fn default() -> Node {
        Node {
            ..Default::default()
        }
    }
    pub fn num_children(&self) -> usize {
        self.0.borrow().children.len()
    }
    fn split(&mut self) {}
    // split a node into two nodes
    fn break_up(&mut self) -> Result<Option<Node>> {
        let mut new_node = Node::default();
        Ok(Some(new_node))
    }

    // read page to node
    pub fn read(&mut self, p: &Page) -> Result<()> {
        let mut node = self.0.borrow_mut();
        let count = p.count as usize;
        node.inodes = match node.node_type {
            NodeType::Branch => p
                .branch_elements()?
                .iter()
                .map(|b| {
                    Inode::from(BranchINode {
                        key: b.key().to_vec(),
                        page_id: b.id,
                    })
                })
                .collect(),
            NodeType::Leaf => p
                .leaf_elements()?
                .iter()
                .map(|f| {
                    Inode::from(LeafINode {
                        key: f.key().to_vec(),
                        value: f.value().to_vec(),
                    })
                })
                .collect(),
        };
        node.key = if !node.inodes.is_empty() {
            let key = node.inodes[0].key().clone();
            Some(key)
        } else {
            None
        };
        Ok(())
    }
    // write node to page
    pub fn write(&self, p: &mut Page) -> Result<()> {
        let mut node = self.0.borrow_mut();
        p.page_type = match node.node_type {
            NodeType::Branch => Page::BRANCH_PAGE,
            NodeType::Leaf => Page::LEAF_PAGE,
        };
        let inodes = &mut node.inodes;
        if inodes.len() > u16::MAX as usize {
            return Err(Error::InodeOverFlow);
        }
        p.count = inodes.len() as u16;
        if p.count == 0 {
            return Ok(());
        }
        let mut addr = unsafe {
            // offset to write key and value
            // memory: element element .... key value
            let offset = self.page_elem_size() * inodes.len();
            p.ptr_mut().add(offset)
        };
        match node.node_type {
            NodeType::Branch => {
                let branches = p.branch_elements_mut()?;
                for (i, inode) in node.inodes.iter().enumerate() {
                    let elem = &mut branches[i];
                    let ptr = elem as *const BranchPageElement as *const u8;
                    elem.k_size = inode.key().len() as u32;
                    elem.id = inode.page_id().ok_or(Error::InvalidInode)?;
                    // offset from key to the element
                    elem.pos = unsafe { addr.sub(ptr as usize) } as u32;
                    unsafe {
                        copy_nonoverlapping(inode.key().as_ptr(), addr, inode.key().len());
                        addr = addr.add(inode.key().len());
                    }
                }
            }
            NodeType::Leaf => {
                let leaves = p.leaf_elements_mut()?;
                for (i, inode) in node.inodes.iter().enumerate() {
                    let elem = &mut leaves[i];
                    let ptr = elem as *const LeafPageElement as *const u8;
                    elem.pos = unsafe { addr.sub(ptr as usize) } as u32;
                    elem.k_size = inode.key().len() as u32;
                    let value = inode.value().ok_or(Error::InvalidInode)?;
                    elem.v_size = value.len() as u32;
                    // write key and value
                    unsafe {
                        copy_nonoverlapping(inode.key().as_ptr(), addr, inode.key().len());
                        addr = addr.add(inode.key().len());
                        copy_nonoverlapping(value.as_ptr(), addr, value.len());
                        addr = addr.add(value.len());
                    }
                }
            }
        }
        Ok(())
    }
    fn page_elem_size(&self) -> usize {
        match self.0.borrow().node_type {
            NodeType::Branch => BranchPageElement::SIZE,
            NodeType::Leaf => LeafPageElement::SIZE,
        }
    }
}


#[derive(Debug)]
enum NodeType {
    Branch,
    Leaf,
}
impl Default for NodeType {
    fn default() -> Self {
        NodeType::Leaf
    }
}
#[derive(Debug)]
struct Inode(Either<BranchINode, LeafINode>);
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
// enum Inode {
//     Branch(BranchINode),
//     Leaf(LeafINode),
// }

#[derive(Debug)]
struct BranchINode {
    key: Entry,
    page_id: PageId,
}

#[derive(Debug)]
struct LeafINode {
    key: Entry,
    value: Entry,
}
