use crate::{
    data::Entry,
    page::{Page, PageId},
};

type NodeId = u64;
struct Node {
    children: Vec<NodeId>,
    page_id: PageId,
    unbalanced: bool,
    spilled: bool,
    inodes: Vec<Inode>,
}

impl Node {
    pub fn from_page(&self, p: &Page) -> Node {
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
            unbalanced: false,
            spilled: false,
            inodes,
        }
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
