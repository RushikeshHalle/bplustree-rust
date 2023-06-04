use std::time::Duration;

use cpu_time::ThreadTime;

pub struct Stopwatch{
    total_time: Duration,
    start_snapshot: ThreadTime,
}

impl Stopwatch{
    pub fn new() -> Stopwatch {
        Stopwatch{
            total_time : Duration::from_nanos(0),
            start_snapshot : ThreadTime::now()
        }
    }
    pub fn start(&mut self) {
        self.start_snapshot = ThreadTime::now();
    }

    pub fn stop(&mut self) {
        self.total_time += self.start_snapshot.elapsed();
    }

    pub fn get_total_time_as_duration(&self) -> Duration{
        self.total_time
    }
}