use crate::error::Error;
use crate::node::Node;
use crate::node_type::{Key, KeyValuePair, NodeType, Offset};
use crate::page::Page;
use crate::pager::Pager;
use crate::wal::Wal;
use std::convert::TryFrom;
use std::path::Path;
use arrayvec::ArrayVec;
/// B+Tree properties.
pub const MAX_BRANCHING_FACTOR: usize = 200;
pub const NODE_KEYS_LIMIT: usize = MAX_BRANCHING_FACTOR - 1;


/// BtreeBuilder is a Builder for the BTree struct.
pub struct BTreeBuilder {
    /// Path to the tree file.
    path: &'static Path,
    /// The BTree parameter, an inner node contains no more than 2*b-1 keys and no less than b-1 keys
    /// and no more than 2*b children and no less than b children.
    b: usize,
}

impl BTreeBuilder {
    pub fn new() -> BTreeBuilder {
        BTreeBuilder {
            path: Path::new(""),
            b: 0,
        }
    }

    pub fn path(mut self, path: &'static Path) -> BTreeBuilder {
        self.path = path;
        self
    }

    pub fn b_parameter(mut self, b: usize) -> BTreeBuilder {
        self.b = b;
        self
    }

    pub fn build(&self) -> Result<BTree, Error> {
        if self.path.to_string_lossy() == "" {
            return Err(Error::UnexpectedError);
        }
        if self.b == 0 {
            return Err(Error::UnexpectedError);
        }

        let mut pager = Pager::new(self.path)?;
        let root = Node::new(NodeType::Leaf(ArrayVec::<KeyValuePair,MAX_BRANCHING_FACTOR>::new()), true, None);
        let root_offset = pager.write_page(Page::try_from(&root)?)?;
        let parent_directory = self.path.parent().unwrap_or_else(|| Path::new("/tmp"));
        let mut wal = Wal::new(parent_directory.to_path_buf())?;
        wal.set_root(root_offset)?;

        Ok(BTree {
            pager,
            b: self.b,
            wal,
        })
    }
}

impl Default for BTreeBuilder {
    // A default BTreeBuilder provides a builder with:
    // - b parameter set to 200
    // - path set to '/tmp/db'.
    fn default() -> Self {
        BTreeBuilder::new()
            .b_parameter(200)
            .path(Path::new("/tmp/db"))
    }
}

/// BTree struct represents an on-disk B+tree.
/// Each node is persisted in the table file, the leaf nodes contain the values.
pub struct BTree {
    pager: Pager,
    b: usize,
    wal: Wal,
}

impl BTree {

    fn is_node_full(&self, node: &Node) -> Result<bool, Error> {
        match &node.node_type {
            NodeType::Leaf(pairs) => Ok(pairs.len() == (2 * self.b - 1)),
            NodeType::Internal(_, keys) => Ok(keys.len() == (2 * self.b - 1)),
            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
    }

    /// insert a key value pair possibly splitting nodes along the way.
    pub fn insert(&mut self, kv: KeyValuePair) -> Result<(), Error> {
        let root_offset = self.wal.get_root_offset()?;
        let root_page = self.pager.get_page(&root_offset)?;
        let new_root_offset: Offset;
        let mut new_root: Node;
        let mut root = Node::try_from(root_page)?;
        if self.is_node_full(&root)? {
            // split the root creating a new root and child nodes along the way.
            new_root = Node::new(NodeType::Internal(ArrayVec::<Offset, MAX_BRANCHING_FACTOR>::new(), ArrayVec::<Key, MAX_BRANCHING_FACTOR>::new()), true, None);
            // write the new root to disk to aquire an offset for the new root.
            new_root_offset = self.pager.write_page(Page::try_from(&new_root)?)?;
            // set the old roots parent to the new root.
            root.parent_offset = Some(new_root_offset.clone());
            root.is_root = false;
            // split the old root.
            let (median, sibling) = root.split(self.b)?;
            // write the old root with its new data to disk in a *new* location.
            let old_root_offset = self.pager.write_page(Page::try_from(&root)?)?;
            // write the newly created sibling to disk.
            let sibling_offset = self.pager.write_page(Page::try_from(&sibling)?)?;
            // update the new root with its children and key.
            let mut offsets = ::<Offset, MAX_BRANCHING_FACTOR>::new();
            let mut keys = ArrayVec::<Key, MAX_BRANCHING_FACTOR>::new();
            offsets.push(old_root_offset);
            offsets.push(sibling_offset);
            keys.push(median);
            new_root.node_type =  NodeType::Internal(offsets, keys);
            // Populate the ArrayVec with the desired values
            

            // new_root.node_type = NodeType::Internal(vec![old_root_offset, sibling_offset], vec![median]);
            // write the new_root to disk.
            self.pager
                .write_page_at_offset(Page::try_from(&new_root)?, &new_root_offset)?;
        } else {
            new_root = root.clone();
            new_root_offset = self.pager.write_page(Page::try_from(&new_root)?)?;
        }
        // continue recursively.
        self.insert_non_full(&mut new_root, new_root_offset.clone(), kv)?;
        // finish by setting the root to its new copy.
        self.wal.set_root(new_root_offset)
    }

    /// insert_non_full (recursively) finds a node rooted at a given non-full node.
    /// to insert a given key-value pair. Here we assume the node is
    /// already a copy of an existing node in a copy-on-write root to node traversal.
    fn insert_non_full(
        &mut self,
        node: &mut Node,
        node_offset: Offset,
        kv: KeyValuePair,
    ) -> Result<(), Error> {
        match &mut node.node_type {
            NodeType::Leaf(ref mut pairs) => {
                let idx = pairs.binary_search(&kv).unwrap_or_else(|x| x);
                pairs.insert(idx, kv);
                self.pager
                    .write_page_at_offset(Page::try_from(&*node)?, &node_offset)
            }
            NodeType::Internal(ref mut children, ref mut keys) => {
                let idx = keys
                    .binary_search(&Key(kv.key.clone()))
                    .unwrap_or_else(|x| x);
                let child_offset = children.get(idx).ok_or(Error::UnexpectedError)?.clone();
                let child_page = self.pager.get_page(&child_offset)?;
                let mut child = Node::try_from(child_page)?;
                // Copy each branching-node on the root-to-leaf walk.
                // write_page appends the given page to the db file thus creating a new node.
                let new_child_offset: Offset = self.pager.write_page(Page::try_from(&child)?)?;
                // Assign copied child at the proper place.
                children[idx] = new_child_offset.to_owned();
                if self.is_node_full(&child)? {
                    // split will split the child at b leaving the [0, b-1] keys
                    // while moving the set of [b, 2b-1] keys to the sibling.
                    let (median, mut sibling) = child.split(self.b)?;
                    self.pager
                        .write_page_at_offset(Page::try_from(&child)?, &new_child_offset)?;
                    // Write the newly created sibling to disk.
                    let sibling_offset = self.pager.write_page(Page::try_from(&sibling)?)?;
                    // Siblings keys are larger than the splitted child thus need to be inserted
                    // at the next index.
                    children.insert(idx + 1, sibling_offset.clone());
                    keys.insert(idx, median.clone());

                    // Write the parent page to disk.
                    self.pager
                        .write_page_at_offset(Page::try_from(&*node)?, &node_offset)?;
                    // Continue recursively.
                    if kv.key <= median.0 {
                        self.insert_non_full(&mut child, new_child_offset, kv)
                    } else {
                        self.insert_non_full(&mut sibling, sibling_offset, kv)
                    }
                } else {
                    self.pager
                        .write_page_at_offset(Page::try_from(&*node)?, &node_offset)?;
                    self.insert_non_full(&mut child, new_child_offset, kv)
                }
            }
            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
    }

    /// search searches for a specific key in the BTree.
    pub fn search(&mut self, key: String) -> Result<KeyValuePair, Error> {
        let root_offset = self.wal.get_root_offset()?;
        let root_page = self.pager.get_page(&root_offset)?;
        let root = Node::try_from(root_page)?;
        self.search_node(root, &key)
    }

    /// search_node recursively searches a sub tree rooted at node for a key.
    fn search_node(&mut self, node: Node, search: &str) -> Result<KeyValuePair, Error> {
        match node.node_type {
            NodeType::Internal(children, keys) => {
                let idx = keys
                    .binary_search(&Key(search.to_string()))
                    .unwrap_or_else(|x| x);
                // Retrieve child page from disk and deserialize.
                let child_offset = children.get(idx).ok_or(Error::UnexpectedError)?;
                let page = self.pager.get_page(child_offset)?;
                let child_node = Node::try_from(page)?;
                self.search_node(child_node, search)
            }
            NodeType::Leaf(pairs) => {
                if let Ok(idx) =
                    pairs.binary_search_by_key(&search.to_string(), |pair| pair.key.clone())
                {
                    return Ok(pairs[idx].clone());
                }
                Err(Error::KeyNotFound)
            }
            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
    }

    /// print_sub_tree is a helper function for recursively printing the nodes rooted at a node given by its offset.
    fn print_sub_tree(&mut self, prefix: String, offset: Offset) -> Result<(), Error> {
        println!("{}Node at offset: {}", prefix, offset.0);
        let curr_prefix = format!("{}|->", prefix);
        let page = self.pager.get_page(&offset)?;
        let node = Node::try_from(page)?;
        match node.node_type {
            NodeType::Internal(children, keys) => {
                println!("{}Keys: {:?}", curr_prefix, keys);
                println!("{}Children: {:?}", curr_prefix, children);
                let child_prefix = format!("{}   |  ", prefix);
                for child_offset in children {
                    self.print_sub_tree(child_prefix.clone(), child_offset)?;
                }
                Ok(())
            }
            NodeType::Leaf(pairs) => {
                println!("{}Key value pairs: {:?}", curr_prefix, pairs);
                Ok(())
            }
            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
    }

    /// print is a helper for recursively printing the tree.
    pub fn print(&mut self) -> Result<(), Error> {
        println!();
        let root_offset = self.wal.get_root_offset()?;
        self.print_sub_tree("".to_string(), root_offset)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;

    #[test]
    fn search_works() -> Result<(), Error> {
        use crate::btree::BTreeBuilder;
        use crate::node_type::KeyValuePair;
        use std::path::Path;

        let mut btree = BTreeBuilder::new()
            .path(Path::new("/tmp/db"))
            .b_parameter(2)
            .build()?;
        btree.insert(KeyValuePair::new("a".to_string(), "shalom".to_string()))?;
        btree.insert(KeyValuePair::new("b".to_string(), "hello".to_string()))?;
        btree.insert(KeyValuePair::new("c".to_string(), "marhaba".to_string()))?;

        let mut kv = btree.search("b".to_string())?;
        assert_eq!(kv.key, "b");
        assert_eq!(kv.value, "hello");

        kv = btree.search("c".to_string())?;
        assert_eq!(kv.key, "c");
        assert_eq!(kv.value, "marhaba");

        Ok(())
    }

    #[test]
    fn insert_works() -> Result<(), Error> {
        use crate::btree::BTreeBuilder;
        use crate::node_type::KeyValuePair;
        use std::path::Path;

        let mut btree = BTreeBuilder::new()
            .path(Path::new("/tmp/db"))
            .b_parameter(2)
            .build()?;
        btree.insert(KeyValuePair::new("a".to_string(), "shalom".to_string()))?;
        btree.insert(KeyValuePair::new("b".to_string(), "hello".to_string()))?;
        btree.insert(KeyValuePair::new("c".to_string(), "marhaba".to_string()))?;
        btree.insert(KeyValuePair::new("d".to_string(), "olah".to_string()))?;
        btree.insert(KeyValuePair::new("e".to_string(), "salam".to_string()))?;
        btree.insert(KeyValuePair::new("f".to_string(), "hallo".to_string()))?;
        btree.insert(KeyValuePair::new("g".to_string(), "Konnichiwa".to_string()))?;
        btree.insert(KeyValuePair::new("h".to_string(), "Ni hao".to_string()))?;
        btree.insert(KeyValuePair::new("i".to_string(), "Ciao".to_string()))?;

        let mut kv = btree.search("a".to_string())?;
        assert_eq!(kv.key, "a");
        assert_eq!(kv.value, "shalom");

        kv = btree.search("b".to_string())?;
        assert_eq!(kv.key, "b");
        assert_eq!(kv.value, "hello");

        kv = btree.search("c".to_string())?;
        assert_eq!(kv.key, "c");
        assert_eq!(kv.value, "marhaba");

        kv = btree.search("d".to_string())?;
        assert_eq!(kv.key, "d");
        assert_eq!(kv.value, "olah");

        kv = btree.search("e".to_string())?;
        assert_eq!(kv.key, "e");
        assert_eq!(kv.value, "salam");

        kv = btree.search("f".to_string())?;
        assert_eq!(kv.key, "f");
        assert_eq!(kv.value, "hallo");

        kv = btree.search("g".to_string())?;
        assert_eq!(kv.key, "g");
        assert_eq!(kv.value, "Konnichiwa");

        kv = btree.search("h".to_string())?;
        assert_eq!(kv.key, "h");
        assert_eq!(kv.value, "Ni hao");

        kv = btree.search("i".to_string())?;
        assert_eq!(kv.key, "i");
        assert_eq!(kv.value, "Ciao");
        Ok(())
    }
}
