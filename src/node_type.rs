use crate::error::Error;
use crate::page_layout::PTR_SIZE;
use std::cmp::{Eq, Ord, Ordering, PartialOrd};
use std::convert::From;
use std::convert::TryFrom;
use crate::btree::MAX_BRANCHING_FACTOR;


#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Offset(pub usize);

/// Converts an array of length len(usize) to a usize as a BigEndian integer.
impl TryFrom<[u8; PTR_SIZE]> for Offset {
    type Error = Error;

    fn try_from(arr: [u8; PTR_SIZE]) -> Result<Self, Self::Error> {
        Ok(Offset(usize::from_be_bytes(arr)))
    }
}

#[derive(Clone, Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct Key(pub String);

#[derive(Clone, Eq, Debug)]
pub struct KeyValuePair {
    pub key: String,
    pub value: String,
}

impl Ord for KeyValuePair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for KeyValuePair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for KeyValuePair {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}

impl KeyValuePair {
    pub fn new(key: String, value: String) -> KeyValuePair {
        KeyValuePair { key, value }
    }
}


// NodeType Represents different node types in the BTree.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum NodeType {
    /// Internal nodes contain a vector of pointers to their children and a vector of keys.
    Internal(Vec<Offset>, Vec<Key>),

    /// Leaf nodes contain a vector of Keys and values.
    Leaf(Vec<KeyValuePair>),

    Unexpected,
}

pub static mut TOTAL_ALLOC_CALLS: u32 = 0;

// Converts a byte to a NodeType.
impl From<u8> for NodeType {
    fn from(orig: u8) -> NodeType {
        unsafe {
            TOTAL_ALLOC_CALLS += 1;
        }
        match orig {
            0x01 => NodeType::Internal(Vec::<Offset>::with_capacity(MAX_BRANCHING_FACTOR), Vec::<Key>::with_capacity(MAX_BRANCHING_FACTOR)),
            0x02 => NodeType::Leaf(Vec::<KeyValuePair>::with_capacity(MAX_BRANCHING_FACTOR)),
            _ => NodeType::Unexpected,
        }
    }
}

// Converts a NodeType to a byte.
impl From<&NodeType> for u8 {
    fn from(orig: &NodeType) -> u8 {
        match orig {
            NodeType::Internal(_, _) => 0x01,
            NodeType::Leaf(_) => 0x02,
            NodeType::Unexpected => 0x03,
        }
    }
}
