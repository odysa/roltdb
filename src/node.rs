use anyhow::anyhow;
use either::Either;
use std::{
    borrow::BorrowMut,
    cell::RefCell,
    intrinsics::copy_nonoverlapping,
    ops::{Deref, DerefMut},
    rc::{Rc, Weak},
};

use crate::{
    bucket::Bucket,
    data::{Entry, RawPtr},
    error::{Result, RoltError},
    page::{BranchPageElement, LeafPageElement, Page, PageId},
    Err,
};

type NodeId = u64;
#[derive(Default, Clone, Debug)]
pub(crate) struct Node(pub(crate) Rc<InnerNode>);

#[derive(Default, Debug, Clone)]
pub(crate) struct WeakNode(Weak<InnerNode>);
impl WeakNode {
    pub(crate) fn new() -> Self {
        Self(Weak::new())
    }
    pub(crate) fn upgrade(&self) -> Option<Node> {
        self.0.upgrade().map(Node)
    }
    pub(crate) fn from(n: &Node) -> Self {
        Self(Rc::downgrade(&n.0))
    }
}
impl Deref for WeakNode {
    type Target = Weak<InnerNode>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Default, Debug, Clone)]
pub(crate) struct InnerNode {
    bucket: RawPtr<Bucket>,
    page_id: PageId,
    unbalanced: bool,
    spilled: bool,
    pub(crate) inodes: RefCell<Vec<Inode>>,
    pub(crate) children: RefCell<Vec<Node>>,
    pub(crate) parent: RefCell<WeakNode>,
    node_type: NodeType,
    key: RefCell<Option<Entry>>,
}

impl Node {
    pub fn default() -> Node {
        Node {
            ..Default::default()
        }
    }

    fn bucket(&self) -> &Bucket {
        &*self.bucket
    }

    fn bucket_mut(&self) -> &mut Bucket {
        let bucket = self.bucket.unwrap();
        unsafe { &mut *(bucket as *mut Bucket) }
    }
    // fn bucket_mut(&self) -> &mut Bucket {
    //     (*self).bucket_mut()
    // }
    pub fn num_children(&self) -> usize {
        self.children.borrow().len()
    }
    pub(crate) fn is_leaf(&self) -> bool {
        match self.node_type {
            NodeType::Branch => false,
            NodeType::Leaf => true,
        }
    }
    fn split(&mut self) {}
    // split a node into two nodes
    fn break_up(&mut self) -> Result<Option<Node>> {
        let new_node = Node::default();
        Ok(Some(new_node))
    }
    pub(crate) fn put(&mut self, old: &[u8], key: &[u8], value: &[u8], page_id: PageId) {
        let node = self;
        let mut inodes = node.inodes.borrow_mut();
        let (found, index) = match inodes.binary_search_by(|inode| inode.key().as_slice().cmp(old))
        {
            Ok(i) => (true, i),
            Err(i) => (false, i),
        };
        // old key does not found, insert new inode
        if !found {
            inodes.insert(
                index,
                Inode::from(LeafINode {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }),
            );
        } else {
            let inode = &mut inodes[index];
            match &mut inode.0 {
                Either::Right(l) => {
                    l.key = key.to_vec();
                    l.value = key.to_vec();
                }
                _ => unreachable!(),
            }
        };
    }
    // read page to node
    pub fn read(&mut self, p: &Page) -> Result<()> {
        let node = self;
        let count = p.count as usize;
        node.inodes.replace(match node.node_type {
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
        });
        node.key.replace(if !node.inodes.borrow().is_empty() {
            let key = node.inodes.borrow()[0].key().clone();
            Some(key)
        } else {
            None
        });
        Ok(())
    }
    // write node to page
    pub fn write(&self, p: &mut Page) -> Result<()> {
        let node = self;
        p.page_type = match node.node_type {
            NodeType::Branch => Page::BRANCH_PAGE,
            NodeType::Leaf => Page::LEAF_PAGE,
        };
        let inodes = node.inodes.borrow_mut();
        if inodes.len() > u16::MAX as usize {
            return Err!(RoltError::InodeOverFlow);
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
                for (i, inode) in node.inodes.borrow().iter().enumerate() {
                    let elem = &mut branches[i];
                    let ptr = elem as *const BranchPageElement as *const u8;
                    elem.k_size = inode.key().len() as u32;
                    elem.id = inode.page_id().ok_or(RoltError::InvalidInode)?;
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
                for (i, inode) in node.inodes.borrow().iter().enumerate() {
                    let elem = &mut leaves[i];
                    let ptr = elem as *const LeafPageElement as *const u8;
                    elem.pos = unsafe { addr.sub(ptr as usize) } as u32;
                    elem.k_size = inode.key().len() as u32;
                    let value = inode.value().ok_or(RoltError::InvalidInode)?;
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
        match self.node_type {
            NodeType::Branch => BranchPageElement::SIZE,
            NodeType::Leaf => LeafPageElement::SIZE,
        }
    }

    pub(crate) fn child_at(&mut self, index: usize) -> Result<Node> {
        let inode = self
            .inodes
            .borrow()
            .get(index)
            .ok_or(anyhow!("inode index not valid"))?;
        let id = inode.page_id().unwrap();
        Ok(self.bucket_mut().node(id, WeakNode::from(self)))
    }

    pub(crate) fn rebalance(&mut self) {
        if !self.unbalanced {
            return;
        }
        self.unbalanced = false;
        // this node is root
        if self.parent().is_none() {
            let mut inodes = self.inodes.borrow_mut();
            // root node is branch and only has one inode
            if !self.is_leaf() && inodes.len() == 1 {
                // move up child
                let child = self
                    .bucket_mut()
                    .node(inodes[0].page_id().unwrap(), WeakNode::from(self));

                self.node_type = child.node_type;
                *inodes = child.inodes.borrow_mut().drain(..).collect();
                *self.children.borrow_mut() = child.children.borrow_mut().drain(..).collect();
                {
                    // assign new parent to children of new parent
                    for inode in inodes.iter() {
                        if let Some(child) = self.bucket().nodes.get_mut(&inode.page_id().unwrap())
                        {
                            *child.parent.borrow_mut() = WeakNode::from(self);
                        }
                    }
                }
                *child.parent.borrow_mut() = WeakNode::new();
                self.bucket().nodes.borrow_mut().remove(&child.page_id);
                // free child page
                child.free();
            }

            return;
        }

        // if node has no keys
        if self.num_children() == 0 {
            let key = self.0.key.borrow().clone().unwrap();
            let parent = &mut self.parent().unwrap();
            // remove this node from its parent

            self.bucket_mut().nodes.borrow_mut().remove(&self.page_id);
            // remove this node from its parent
            parent.remove(&key);
            parent.remove_child(self);
            self.free();
            parent.rebalance();
            return;
        }

        {
            let (next_sibling, sibling) = match self.parent().unwrap().child_index(self) {
                Some(i) => {
                    if i == 0 {
                        (true, self.next_sibling().unwrap())
                    } else {
                        (false, self.prev_sibling().unwrap())
                    }
                }
                None => (false, self.prev_sibling().unwrap()),
            };
            // move sibling to this node
            if next_sibling {
                let bucket = self.bucket_mut();
                // move children of sibling to this node
                for page_id in sibling.inodes.borrow().iter().map(|i| i.page_id().unwrap()) {
                    if let Some(child) = bucket.nodes.borrow_mut().get_mut(&page_id) {
                        // remove this child from its parent
                        child.parent().unwrap().remove_child(child);
                        *child.parent.borrow_mut() = WeakNode::from(self);
                        child
                            .parent()
                            .unwrap()
                            .children
                            .borrow_mut()
                            .push(child.clone());
                    }
                }
                // move inodes to this node
                self.inodes
                    .borrow_mut()
                    .append(&mut *sibling.inodes.borrow_mut());
                // remove sibling from parent
                let mut parent = &mut self.parent().unwrap();
                parent.remove(&sibling.key.borrow().as_ref().unwrap());
                parent.remove_child(&sibling);
                // remove sibling from bucket
                self.bucket_mut().nodes.remove(&sibling.page_id);
                sibling.free();
            } else {
                // combine this node into sibling
                for page_id in self.inodes.borrow().iter().map(|i| i.page_id().unwrap()) {
                    if let Some(child) = self.bucket_mut().nodes.get_mut(&page_id) {
                        let parent = &mut child.parent().unwrap();
                        parent.remove_child(&child);
                        // parent is sibling
                        *child.parent.borrow_mut() = WeakNode::from(&sibling);
                        // assign child to new parent
                        parent.children.borrow_mut().push(child.clone());
                    }
                }
                sibling
                    .inodes
                    .borrow_mut()
                    .append(&mut self.inodes.borrow_mut());
                let parent = &mut self.parent().unwrap();
                parent.remove(self.key.borrow().unwrap().as_ref());
                parent.remove_child(&self);

                self.bucket_mut().nodes.remove(&self.page_id);
                self.free();
            }
            self.parent().unwrap().rebalance();
        }
    }
    // return next sibling of this node
    fn next_sibling(&self) -> Option<Node> {
        match self.parent() {
            // its root node
            None => None,
            Some(mut parent) => {
                let index = parent.child_index(self);
                match index {
                    Some(i) => parent.child_at(i + 1).ok(),
                    None => None,
                }
            }
        }
    }
    // find previous sibling
    fn prev_sibling(&self) -> Option<Node> {
        match self.parent() {
            None => None,
            Some(parent) => {
                let index = parent.child_index(self);
                match index {
                    None => None,
                    Some(i) => parent.child_at(i - 1).ok(),
                }
            }
        }
    }
    // get the index of given child node
    fn child_index(&self, child: &Node) -> Option<usize> {
        for (index, node) in self.0.inodes.borrow().iter().enumerate() {
            if Some(node.key()) == child.0.key.borrow().as_ref() {
                return Some(index);
            }
        }
        None
    }
    // remove a child from its children list
    fn remove_child(&mut self, target: &Node) {
        let index = self
            .children
            .borrow()
            .iter()
            .position(|n| Rc::ptr_eq(&n.0, &target.0));
        if let Some(i) = index {
            self.children.borrow_mut().remove(i);
        }
    }

    // remove a key from node
    fn remove(&mut self, key: &[u8]) {
        let mut inodes = self.inodes.borrow_mut();
        match inodes.binary_search_by(|i| i.key().as_slice().cmp(key)) {
            Ok(i) => {
                inodes.remove(i);
                self.unbalanced = true;
            }
            Err(_) => {}
        };
    }

    fn parent(&self) -> Option<Node> {
        self.parent.borrow().upgrade()
    }
    fn free(&mut self) {
        if self.page_id != 0 {
            self.page_id = 0;
            // add a free page to free list
            todo!()
        }
    }
}

impl Deref for Node {
    type Target = Rc<InnerNode>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
enum NodeType {
    Branch,
    Leaf,
}
impl Default for NodeType {
    fn default() -> Self {
        NodeType::Leaf
    }
}
#[derive(Debug, Clone)]
pub(crate) struct Inode(Either<BranchINode, LeafINode>);

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

impl InnerNode {
    fn bucket_mut(&mut self) -> &mut Bucket {
        self.bucket.deref_mut()
    }
}

#[derive(Debug, Clone)]
struct BranchINode {
    key: Entry,
    page_id: PageId,
}

#[derive(Debug, Clone)]
struct LeafINode {
    key: Entry,
    value: Entry,
}
