use crate::error::Error;
use crate::node_type::Offset;
use crate::page::Page;
use crate::page_layout::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Duration;
use cpu_time::ThreadTime;

pub struct Pager {
    file: File,
    curser: usize,
    stopWatchAcc: u128,
}

impl Pager {
    pub fn new(path: &Path) -> Result<Pager, Error> {
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        Ok(Pager {
            file: fd,
            curser: 0,
            stopWatchAcc: 0
        })
    }

    pub fn get_page(&mut self, offset: &Offset) -> Result<Page, Error> {
        let mut page: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(offset.0 as u64))?;
        self.file.read_exact(&mut page)?;
        Ok(Page::new(page))
    }

    pub fn write_page(&mut self, page: Page) -> Result<Offset, Error> {
        let cpu_clock_stamp_before = ThreadTime::now();

        self.file.seek(SeekFrom::Start(self.curser as u64))?;
        self.file.write_all(&page.get_data())?;

        let elapsed_cpu_clock_time: Duration = cpu_clock_stamp_before.elapsed();

        self.stopWatchAcc+= elapsed_cpu_clock_time.as_nanos();
        let res = Offset(self.curser);
        self.curser += PAGE_SIZE;
        Ok(res)
    }

    pub fn write_page_at_offset(&mut self, page: Page, offset: &Offset) -> Result<(), Error> {
        // print!("page in bytes: {}", page.get_data());
        let cpu_clock_stamp_before = ThreadTime::now();

        self.file.seek(SeekFrom::Start(offset.0 as u64))?;
        self.file.write_all(&page.get_data())?;

        let elapsed_cpu_clock_time: Duration = cpu_clock_stamp_before.elapsed();
        self.stopWatchAcc+= elapsed_cpu_clock_time.as_nanos();

        Ok(())
    }

    pub fn getTotalWriteTime(&mut self) -> u128{
        return self.stopWatchAcc;
    }
}
