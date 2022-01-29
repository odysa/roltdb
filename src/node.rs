use anyhow::anyhow;
use either::Either;
use std::{
    borrow::BorrowMut,
    cell::RefCell,
    intrinsics::copy_nonoverlapping,
    ops::{Deref, DerefMut},
    rc::{Rc, Weak},
    vec,
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
    pub(crate) bucket: RawPtr<Bucket>,
    pub(crate) page_id: RefCell<PageId>,
    unbalanced: bool,
    spilled: bool,
    pub(crate) inodes: RefCell<Vec<Inode>>,
    pub(crate) children: RefCell<Vec<Node>>,
    pub(crate) parent: RefCell<WeakNode>,
    pub(crate) node_type: RefCell<NodeType>,
    pub(crate) key: RefCell<Option<Entry>>,
}

impl Node {
    const MIN_KEY: usize = 2;
    pub(crate) fn new(b: RawPtr<Bucket>, node_type: NodeType) -> Node {
        Node(Rc::new(InnerNode {
            bucket: b,
            node_type: RefCell::new(node_type),
            ..Default::default()
        }))
    }
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
        match *self.node_type.borrow() {
            NodeType::Branch => false,
            NodeType::Leaf => true,
        }
    }
    // break up a node into some smaller nodes
    fn split(&mut self) -> Result<Option<Node>> {
        let mut nodes = vec![self.clone()];
        let mut node = self.clone();
        loop {
            let new_node = node.break_up()?;
            nodes.push(node);
            match new_node {
                Some(n) => {
                    node = n.clone();
                }
                // nothing to break
                None => break,
            }
        }
        let parent = match self.parent() {
            Some(p) => {
                // remove borrow
                {
                    let mut children = p.children.borrow_mut();
                    let index = children.iter().position(|ch| Rc::ptr_eq(self, ch));
                    // remove this old node from parent
                    if let Some(i) = index {
                        children.remove(i);
                    }
                    // insert splitted new nodes to children of parent
                    for node in nodes {
                        *node.parent.borrow_mut() = WeakNode::from(&p);
                        children.push(node);
                    }
                }
                p
            }
            None => {
                let p = Node::default();
                *p.children.borrow_mut() = nodes;
                for child in p.children.borrow_mut().iter_mut() {
                    *child.parent.borrow_mut() = WeakNode::from(&p);
                }
                p
            }
        };
        Ok(Some(parent))
    }

    // split a node into two nodes
    fn break_up(&mut self) -> Result<Option<Node>> {
        let inodes = self.inodes.borrow_mut();
        // do not need to break up this node
        if inodes.len() <= Self::MIN_KEY || self.fit_page_size() {
            return Ok(None);
        }
        let mut fill_percent = self.bucket().fill_percent;
        // bound fill_percent
        if fill_percent > Bucket::MAX_FILL_PERCENT {
            fill_percent = Bucket::MAX_FILL_PERCENT;
        } else if fill_percent < Bucket::MIN_FILL_PERCENT {
            fill_percent = Bucket::MIN_FILL_PERCENT;
        }

        let page_size = self.page_size() as usize;
        let threshold = ((page_size as f64) * fill_percent) as usize;
        let (index, _) = self.split_index(threshold);

        let new_node = Node::new(self.bucket.clone(), NodeType::Leaf);
        // move some inodes to new node
        let inodes: Vec<Inode> = self.inodes.borrow_mut().drain(index..).collect();
        *new_node.inodes.borrow_mut() = inodes;

        Ok(Some(new_node))
    }
    // find a index to split a node to fill threshold
    fn split_index(&self, threshold: usize) -> (usize, usize) {
        let mut index = 0;
        let mut size = Page::PAGE_HEADER_SIZE();
        let elem_size = self.page_elem_size();
        let inodes = self.inodes.borrow();
        let len = inodes.len() - Self::MIN_KEY;
        for (i, inode) in inodes.iter().enumerate().take(len) {
            index = i;
            let e_size = elem_size + inode.key().len() + inode.value().unwrap().len();
            // have minimum number of keys
            if index >= Self::MIN_KEY && size + e_size > threshold {
                break;
            }
            size += e_size;
        }
        return (index, size);
    }
    // whether this node fit one page
    fn fit_page_size(&self) -> bool {
        let head_size = Page::PAGE_HEADER_SIZE();
        let mut size = head_size;
        let elem_size = self.page_elem_size();
        let page_size = self.page_size() as usize;
        for inode in self.inodes.borrow().iter() {
            size += elem_size + inode.key().len() as usize + inode.value().unwrap().len();
            if size >= page_size {
                return false;
            }
        }
        true
    }

    pub(crate) fn page_id(&self) -> u64 {
        *self.page_id.borrow()
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
        node.inodes.replace(match *node.node_type.borrow() {
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
        p.page_type = match *node.node_type.borrow() {
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
        match *node.node_type.borrow() {
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
        match *self.node_type.borrow() {
            NodeType::Branch => BranchPageElement::SIZE,
            NodeType::Leaf => LeafPageElement::SIZE,
        }
    }
    fn page_size(&self) -> u64 {
        self.bucket().tx().unwrap().db().page_size()
    }
    // write nodes to dirty pages
    pub(crate) fn spill(&mut self) -> Result<()> {
        let page_size = self.page_size();
        {
            // spill children
            let mut children = self.children.borrow_mut();
            children.sort_by(|a, b| a.inodes.borrow()[0].key().cmp(b.inodes.borrow()[0].key()));
            for child in children.iter_mut() {
                child.spill()?;
            }
            self.children.borrow_mut().clear();
        }
        {}
        Ok(())
    }

    pub(crate) fn child_at(&mut self, index: usize) -> Result<Node> {
        let inodes = self.inodes.borrow();
        let inode = inodes.get(index).ok_or(anyhow!("inode index not valid"))?;
        let id = inode.page_id().unwrap();
        Ok(self.bucket_mut().node(id, WeakNode::from(self)))
    }

    pub(crate) fn rebalance(&mut self) -> Result<()> {
        if !self.unbalanced {
            return Ok(());
        }
        // self.unbalanced = false;
        // this node is root
        if self.parent().is_none() {
            let mut inodes = self.inodes.borrow_mut();
            // root node is branch and only has one inode
            if !self.is_leaf() && inodes.len() == 1 {
                // move up child
                let mut child = self
                    .bucket_mut()
                    .node(inodes[0].page_id().unwrap(), WeakNode::from(self));

                *self.node_type.borrow_mut() = *child.node_type.borrow();
                *inodes = child.inodes.borrow_mut().drain(..).collect();
                *self.children.borrow_mut() = child.children.borrow_mut().drain(..).collect();
                {
                    // assign new parent to children of new parent
                    for inode in inodes.iter() {
                        if let Some(child) =
                            self.bucket_mut().nodes.get_mut(&inode.page_id().unwrap())
                        {
                            *child.parent.borrow_mut() = WeakNode::from(self);
                        }
                    }
                }
                *child.parent.borrow_mut() = WeakNode::new();
                self.bucket_mut()
                    .nodes
                    .borrow_mut()
                    .remove(&child.page_id.borrow());
                // free child page
                child.free()?;
            }

            return Ok(());
        }

        // if node has no keys
        if self.num_children() == 0 {
            let key = self.0.key.borrow().clone().unwrap();
            let parent = &mut self.parent().unwrap();
            // remove this node from its parent

            self.bucket_mut()
                .nodes
                .borrow_mut()
                .remove(&self.page_id.borrow());
            // remove this node from its parent
            parent.remove(&key);
            parent.remove_child(self);
            self.free();
            parent.rebalance();
            return Ok(());
        }

        {
            let (next_sibling, mut sibling) = match self.parent().unwrap().child_index(self) {
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
                let parent = &mut self.parent().unwrap();
                parent.remove(&sibling.key.borrow().as_ref().unwrap());
                parent.remove_child(&sibling);
                // remove sibling from bucket
                self.bucket_mut().nodes.remove(&sibling.page_id.borrow());
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
                let parent = &mut self.parent().ok_or(anyhow!("parent not valid"))?;
                parent.remove(self.key.borrow().as_ref().unwrap());
                parent.remove_child(&self);

                self.bucket_mut().nodes.remove(&self.page_id.borrow());
                self.free();
            }
            self.parent()
                .ok_or(anyhow!("parent not valid"))?
                .rebalance();
        }
        Ok(())
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
            Some(mut parent) => {
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
                // self.unbalanced = true;
            }
            Err(_) => {}
        };
    }

    fn parent(&self) -> Option<Node> {
        self.parent.borrow().upgrade()
    }

    fn free(&mut self) -> Result<()> {
        if *self.page_id.borrow() != 0 {
            // add page to free list
            let b = self.bucket_mut();
            let tx = b.tx()?;
            let db = tx.db();
            let mut free_lit = db
                .free_list
                .write()
                .map_err(|_| anyhow!("unable to write free list"))?;
            let page = tx.page(*self.page_id.borrow())?;
            // free node's page
            free_lit.free(tx.id(), &page)?;
            *self.page_id.borrow_mut() = 0;
        }
        Ok(())
    }
}

impl Deref for Node {
    type Target = Rc<InnerNode>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum NodeType {
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
