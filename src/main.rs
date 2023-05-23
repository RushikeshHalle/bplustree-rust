use btree::btree::{BTree, BTreeBuilder};
use std::path::Path;
use btree::node_type::{Key, KeyValuePair, NodeType, Offset};
use btree::error::Error;
use std::mem::size_of;
use std::thread::Thread;
use cpu_time::{ProcessTime,ThreadTime};
use std::time::Duration;

fn main() ->  Result<(), Error>{
    // Initialize a new BTree;
    // The BTree nodes are stored in file '/tmp/db' (created if does not exist)
    // with parameter b=2.
    let mut btree =BTreeBuilder::new()
    .path(Path::new("/tmp/rust-bt"))
    .b_parameter(100)
    .build()?;

    let wall_clock_stamp_before = ProcessTime::now();
    let cpu_clock_stamp_before = ThreadTime::now();

    for i in 0..=200000 {
        btree.insert(KeyValuePair::new(format!("k{}", i), format!("v{}", i)));
    }
    // btree.insert(KeyValuePair::new("k1".to_string(), "v1".to_string()))?;
    // btree.insert(KeyValuePair::new("k2".to_string(), "v2".to_string()))?;
    // btree.insert(KeyValuePair::new("k3".to_string(), "v3".to_string()))?;
    // btree.insert(KeyValuePair::new("k4".to_string(), "v4".to_string()))?;
    // btree.insert(KeyValuePair::new("k5".to_string(), "v5".to_string()))?;
    // btree.insert(KeyValuePair::new("k6".to_string(), "v6".to_string()))?;

    let elapsed_wall_clock_time: Duration = wall_clock_stamp_before.elapsed();
    let elapsed_cpu_clock_time: Duration = cpu_clock_stamp_before.elapsed();
    println!("elapsed_wall_clock_time : {:?}", elapsed_wall_clock_time);
    println!("elapsed_cpu_clock_time  : {:?}", elapsed_cpu_clock_time);

    // Read it back.
    let mut kv = btree.search("k12345".to_string())?;
    assert_eq!(kv.key, "k12345");
    assert_eq!(kv.value, "v12345");

    Ok(())
}