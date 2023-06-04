//--------------tikv-jemalloc--------------//
// #[cfg(not(target_env = "msvc"))]
// use tikv_jemallocator::Jemalloc;

// #[cfg(not(target_env = "msvc"))]
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

//--------mimalloc---------//
//use mimalloc::MiMalloc;

//#[global_allocator]
//static GLOBAL: MiMalloc = MiMalloc;

//-----------jemalloc---------//
//#[cfg(not(target_env = "msvc"))]
//use jemallocator::Jemalloc;
//
//#[cfg(not(target_env = "msvc"))]
//#[global_allocator]
//static GLOBAL: Jemalloc = Jemalloc;
//
mod stopwatch;

use btree::error::Error;
use btree::btree::BTreeBuilder;
use std::path::Path;
use btree::node_type:: KeyValuePair ;
use std::time::Duration;
use stopwatch::Stopwatch;

fn main() ->  Result<(), Error> {
    // Initialize a new BTree;
    // The BTree nodes are stored in file '/tmp/db' (created if does not exist)
    // with parameter b=2.
    let mut btree = BTreeBuilder::new()
        .path(Path::new("/tmp/rust-bt"))
        .b_parameter(100)
        .build()?;
    {
        let mut stopwatch = Stopwatch::new();
        stopwatch.start();
        
        for i in 0..=1000000 {
            let _ = btree.insert(KeyValuePair::new(format!("k{}", i), format!("v{}", i))).unwrap();
        }

        stopwatch.stop();
        let elapsed_cpu_clock_time: Duration = stopwatch.get_total_time_as_duration();
        println!("insertion:");
        println!("elapsed_thread_clock_time  : {:?}", elapsed_cpu_clock_time);
        // println!("totalWriteTime          : {:?}s", (btree.getTotalWriteTime() as f64)/1000000000.0);
    }
    {
        let mut stopwatch = Stopwatch::new();
        stopwatch.start();
        
        for i in 0..=1000000 {
            let _ = btree.search(format!("k{}", i)).unwrap();
        }

        stopwatch.stop();
        let elapsed_cpu_clock_time: Duration = stopwatch.get_total_time_as_duration();
        println!("search:");
        println!("elapsed_thread_clock_time  : {:?}", elapsed_cpu_clock_time);
    }

    Ok(())
}
