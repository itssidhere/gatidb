use crate::{disk::{DiskManager, PAGE_SIZE}, page};
use std::collections::HashMap;

pub struct BufferPool {
    disk: DiskManager,
    pages: HashMap<u32, [u8; PAGE_SIZE]>,
    dirty: HashMap<u32, bool>,
    capacity: usize,
    lru_order: Vec<u32>,
}

impl BufferPool {
    pub fn new(disk: DiskManager, capacity: usize) -> Self {
        BufferPool {
            disk,
            pages: HashMap::new(),
            dirty: HashMap::new(),
            capacity,
            lru_order: Vec::new(),
        }
    }

    pub fn get_page(&mut self, page_id: u32) -> &[u8; PAGE_SIZE] {
        if !self.pages.contains_key(&page_id) {

            // pool is full 
            while self.pages.len() >= self.capacity {
                self.evict();
            }
            let data = self.disk.read_page(page_id);
            self.pages.insert(page_id, data);
        }

        self.touch(page_id);
        &self.pages[&page_id]
    }

    pub fn write_page(&mut self, page_id: u32, data: [u8; PAGE_SIZE]) {
        if !self.pages.contains_key(&page_id){
            while self.pages.len() >= self.capacity {
                self.evict();
            }
        }
    
        self.pages.insert(page_id, data);
        self.dirty.insert(page_id, true);
        self.touch(page_id);
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

    fn touch(&mut self, page_id: u32){
        if let Some(pos) = self.lru_order.iter().position(|&id| id == page_id){
            self.lru_order.remove(pos);
        }
        self.lru_order.push(page_id);
    }

    fn evict(&mut self){
        if let Some(victim_id) = self.lru_order.first().copied(){
            // if dirty, flush to disk before evicting
            if self.dirty.get(&victim_id) == Some(&true) {
                if let Some(data) = self.pages.get(&victim_id){
                    self.disk.write_page(victim_id, data);
                }
                self.dirty.remove(&victim_id);
            }

            self.pages.remove(&victim_id);
            self.lru_order.remove(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::DiskManager;

    fn make_pool(filename: &str, capacity: usize) -> BufferPool {
        let dm = DiskManager::new(filename);
        BufferPool::new(dm, capacity)
    }

    fn make_page(marker: u8) -> [u8; PAGE_SIZE] {
        let mut data = [0u8; PAGE_SIZE];
        data[0] = marker;
        data
    }

    #[test]
    fn test_basic_get_and_write() {
        let filename = "test_buf_basic.db";
        let mut pool = make_pool(filename, 4);

        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));

        assert_eq!(pool.get_page(0)[0], 10);
        assert_eq!(pool.get_page(1)[0], 20);

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_capacity_respected() {
        let filename = "test_buf_capacity.db";
        let mut pool = make_pool(filename, 3);

        for i in 0..10 {
            pool.write_page(i, make_page(i as u8));
            assert!(pool.pages.len() <= 3, "pool exceeded capacity");
        }

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_eviction_flushes_dirty_page() {
        let filename = "test_buf_evict_dirty.db";
        let mut pool = make_pool(filename, 3);

        // Fill pool with 3 dirty pages
        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));
        pool.write_page(2, make_page(30));

        // Write page 3 — forces eviction of page 0 (LRU)
        pool.write_page(3, make_page(40));
        assert!(!pool.pages.contains_key(&0), "page 0 should be evicted");

        // Page 0 was dirty, so it should have been flushed to disk.
        // Re-read it — should get the written data back, not zeroes.
        let page = pool.get_page(0);
        assert_eq!(page[0], 10, "dirty page 0 should survive eviction via disk");

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_lru_order_updated_on_read() {
        let filename = "test_buf_lru_read.db";
        let mut pool = make_pool(filename, 3);

        // Fill: 0, 1, 2 — LRU order is [0, 1, 2]
        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));
        pool.write_page(2, make_page(30));

        // Read page 0 — moves it to most recent. LRU order: [1, 2, 0]
        pool.get_page(0);

        // Insert page 3 — should evict page 1 (now the LRU), not page 0
        pool.write_page(3, make_page(40));

        assert!(pool.pages.contains_key(&0), "page 0 was accessed recently, should not be evicted");
        assert!(!pool.pages.contains_key(&1), "page 1 is LRU, should be evicted");
        assert!(pool.pages.contains_key(&2));
        assert!(pool.pages.contains_key(&3));

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_lru_order_updated_on_write() {
        let filename = "test_buf_lru_write.db";
        let mut pool = make_pool(filename, 3);

        // Fill: 0, 1, 2
        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));
        pool.write_page(2, make_page(30));

        // Overwrite page 0 — moves it to most recent. LRU order: [1, 2, 0]
        pool.write_page(0, make_page(15));

        // Insert page 3 — should evict page 1
        pool.write_page(3, make_page(40));

        assert!(pool.pages.contains_key(&0));
        assert!(!pool.pages.contains_key(&1));
        assert_eq!(pool.get_page(0)[0], 15, "page 0 should have updated value");

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_evicted_clean_page_reloads_from_disk() {
        let filename = "test_buf_clean_reload.db";
        let mut pool = make_pool(filename, 3);

        // Write and flush page 0 (makes it clean on disk)
        pool.write_page(0, make_page(10));
        pool.flush();

        pool.write_page(1, make_page(20));
        pool.write_page(2, make_page(30));

        // Access page 0 so it's not LRU, then add pages to evict page 1
        pool.get_page(0);
        pool.write_page(3, make_page(40));
        pool.write_page(4, make_page(50));

        // Page 0 may have been evicted by now, but re-reading should work
        let page = pool.get_page(0);
        assert_eq!(page[0], 10, "clean page should reload from disk correctly");

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_many_evictions_data_integrity() {
        let filename = "test_buf_integrity.db";
        let mut pool = make_pool(filename, 4);

        // Write 20 pages through a pool of size 4
        for i in 0u32..20 {
            pool.write_page(i, make_page(i as u8));
        }

        // All data should be retrievable (from cache or disk)
        for i in 0u32..20 {
            let page = pool.get_page(i);
            assert_eq!(page[0], i as u8, "page {} has wrong data after evictions", i);
        }

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_flush_writes_all_dirty_pages() {
        let filename = "test_buf_flush.db";
        let mut pool = make_pool(filename, 4);

        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));
        pool.flush();

        // After flush, dirty map should be empty
        assert!(pool.dirty.is_empty(), "flush should clear dirty map");

        // Data should still be readable from pool
        assert_eq!(pool.get_page(0)[0], 10);
        assert_eq!(pool.get_page(1)[0], 20);

        std::fs::remove_file(filename).unwrap();
    }

    #[test]
    fn test_capacity_one() {
        let filename = "test_buf_cap1.db";
        let mut pool = make_pool(filename, 1);

        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));
        pool.write_page(2, make_page(30));

        assert_eq!(pool.pages.len(), 1);

        // All pages should still be retrievable via disk
        assert_eq!(pool.get_page(0)[0], 10);
        assert_eq!(pool.get_page(1)[0], 20);
        assert_eq!(pool.get_page(2)[0], 30);

        std::fs::remove_file(filename).unwrap();
    }
}
