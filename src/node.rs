use std::{cell::RefCell, rc::Weak};

use crate::{
    bucket::Bucket,
    data::Entry,
    error::Result,
    page::{Page, PageId},
};

type NodeId = u64;
#[derive(Default)]
pub(crate) struct Node {
    bucket: Option<*const Bucket>,
    page_id: PageId,
    unbalanced: bool,
    spilled: bool,
    inodes: Vec<Inode>,
    children: Vec<NodeId>,
    parent: RefCell<Weak<Node>>,
    node_type: NodeType,
}

impl Node {
    pub fn default() -> Node {
        Node {
            ..Default::default()
        }
    }
    pub fn num_children(&self) -> usize {
        self.children.len()
    }
    fn split(&mut self) {}
    // split a node into two nodes
    fn break_up(&mut self) -> Result<Option<Node>> {
        let mut new_node = Node::default();
        new_node.node_type = NodeType::Leaf;
        let nodes: Vec<Inode> = self.inodes.drain(0..).collect();
        new_node.inodes = nodes;

        Ok(Some(new_node))
    }
    pub fn from_page(&self, bucket: Option<*const Bucket>, p: &Page) -> Node {
        let inodes: Vec<Inode> = match p.page_type {
            Page::BRANCH_PAGE => {
                let mut inodes: Vec<Inode> = Vec::with_capacity(p.count as usize);
                match p.branch_elements() {
                    Ok(branches) => {
                        for branch in branches {
                            inodes.push(Inode::Branch(BranchINode {
                                key: Entry::from_slice(branch.key()),
                                page_id: branch.id,
                            }))
                        }
                        inodes
                    }
                    Err(_) => {
                        unreachable!()
                    }
                }
            }
            Page::LEAF_PAGE => {
                let mut inodes: Vec<Inode> = Vec::with_capacity(p.count as usize);
                match p.leaf_elements() {
                    Ok(leaves) => {
                        for leaf in leaves {
                            inodes.push(Inode::Leaf(LeafINode {
                                key: Entry::from_slice(leaf.key()),
                                value: Entry::from_slice(leaf.value()),
                            }))
                        }
                        inodes
                    }
                    Err(_) => unreachable!(),
                }
            }
            _ => unreachable!(),
        };
        Node {
            children: Vec::new(),
            page_id: p.id,
            inodes,
            bucket,
            ..Default::default()
        }
    }
}

enum NodeType {
    Branch,
    Leaf,
}
impl Default for NodeType {
    fn default() -> Self {
        NodeType::Leaf
    }
}
enum Inode {
    Branch(BranchINode),
    Leaf(LeafINode),
}

struct BranchINode {
    key: Entry,
    page_id: PageId,
}

struct LeafINode {
    key: Entry,
    value: Entry,
}
