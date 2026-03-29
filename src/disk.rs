use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

pub const PAGE_SIZE: usize = 4096;

pub struct DiskManager {
    file: File,
}

impl DiskManager {
    pub fn new(filename: &str) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .unwrap();

        DiskManager { file }
    }

    pub fn read_page(&mut self, page_id: u32) -> [u8; PAGE_SIZE] {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset)).unwrap();

        let mut buffer = [0u8; PAGE_SIZE];
        if self.file.read_exact(&mut buffer).is_err() {
            return [0u8; PAGE_SIZE];
        }
        buffer
    }

    pub fn write_page(&mut self, page_id: u32, data: &[u8; PAGE_SIZE]) {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        self.file.write_all(data).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_write_page() {
        let filename = "test_disk.db";

        let mut dm = DiskManager::new(filename);

        // write a page
        let mut data = [0u8; PAGE_SIZE];
        data[0] = 42;
        data[4095] = 99;
        dm.write_page(0, &data);

        // read it back
        let result = dm.read_page(0);
        assert_eq!(result[0], 42);
        assert_eq!(result[4095], 99);

        // write page 3 (skipping 1 and 2)
        let mut data2 = [0u8; PAGE_SIZE];
        data2[0] = 7;
        dm.write_page(3, &data2);

        let result2 = dm.read_page(3);
        assert_eq!(result2[0], 7);

        // clean up
        std::fs::remove_file(filename).unwrap();
    }
}
