//--------------tikv-jemalloc--------------//
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
use btree::node_type::KeyValuePair ;
use btree::node_type;
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
        
        let mut min_alloc_calls = u32::MAX;
        let mut max_alloc_calls = u32::MIN; 
        for i in 0..=1000000 {
            unsafe{
                node_type::TOTAL_ALLOC_CALLS = 0;
            }
            btree.insert(KeyValuePair::new(format!("k{}", i), format!("v{}", i))).unwrap();
            unsafe{
                if node_type::TOTAL_ALLOC_CALLS <  min_alloc_calls{
                    min_alloc_calls = node_type::TOTAL_ALLOC_CALLS;
                }
                if node_type::TOTAL_ALLOC_CALLS >  max_alloc_calls{
                    max_alloc_calls = node_type::TOTAL_ALLOC_CALLS;
                }
            }
        }

        stopwatch.stop();
        let elapsed_cpu_clock_time: Duration = stopwatch.get_total_time_as_duration();
        println!("insertion                  :");
        println!("elapsed_thread_clock_time  : {:?}", elapsed_cpu_clock_time);
        println!("min_alloc_calls            : {:?}", min_alloc_calls);
        println!("max_alloc_calls            : {:?}", max_alloc_calls);
        // println!("totalWriteTime          : {:?}s", (btree.getTotalWriteTime() as f64)/1000000000.0);
    }
    {
        let mut stopwatch = Stopwatch::new();
        stopwatch.start();
        
        let mut min_alloc_calls = u32::MAX;
        let mut max_alloc_calls = u32::MIN; 
        for i in 0..=1000000 {
            unsafe{
                node_type::TOTAL_ALLOC_CALLS = 0;
            }
            let _ = btree.search(format!("k{}", i)).unwrap();
            unsafe{
                if node_type::TOTAL_ALLOC_CALLS <  min_alloc_calls{
                    min_alloc_calls = node_type::TOTAL_ALLOC_CALLS;
                }
                if node_type::TOTAL_ALLOC_CALLS >  max_alloc_calls{
                    max_alloc_calls = node_type::TOTAL_ALLOC_CALLS;
                }
            }
        }

        stopwatch.stop();
        let elapsed_cpu_clock_time: Duration = stopwatch.get_total_time_as_duration();
        println!("search                     :");
        println!("elapsed_thread_clock_time  : {:?}", elapsed_cpu_clock_time);
        println!("min_alloc_calls            : {:?}", min_alloc_calls);
        println!("max_alloc_calls            : {:?}", max_alloc_calls);
    }

    Ok(())
}
