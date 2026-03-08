use crate::disk::{DiskManager, PAGE_SIZE};
use std::collections::HashMap;

pub struct BufferPool {
    disk: DiskManager,
    pages: HashMap<u32, [u8; PAGE_SIZE]>,
    dirty: HashMap<u32, bool>,
}

impl BufferPool {
    pub fn new(disk: DiskManager) -> Self {
        BufferPool {
            disk,
            pages: HashMap::new(),
            dirty: HashMap::new(),
        }
    }

    pub fn get_page(&mut self, page_id: u32) -> &[u8; PAGE_SIZE] {
        if !self.pages.contains_key(&page_id) {
            let data = self.disk.read_page(page_id);
            self.pages.insert(page_id, data);
        }

        &self.pages[&page_id]
    }

    pub fn write_page(&mut self, page_id: u32, data: [u8; PAGE_SIZE]) {
        self.pages.insert(page_id, data);
        self.dirty.insert(page_id, true);
    }

    pub fn flush(&mut self) {
        let dirty_ids: Vec<u32> = self.dirty.keys().cloned().collect();
        for page_id in dirty_ids {
            if let Some(data) = self.pages.get(&page_id) {
                self.disk.write_page(page_id, data);
            }
        }
        self.dirty.clear();
    }
}
