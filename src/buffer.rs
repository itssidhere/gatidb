use crate::{disk::{DiskManager, PAGE_SIZE}, page::set_page_lsn, wal::Wal};
use std::collections::HashMap;

pub struct BufferPool {
    disk: DiskManager,
    wal: Wal,
    pages: HashMap<u32, [u8; PAGE_SIZE]>,
    dirty: HashMap<u32, bool>,
    page_lsn: HashMap<u32, u64>,
    capacity: usize,
    lru_order: Vec<u32>,
}

impl BufferPool {
    pub fn new(disk: DiskManager, wal: Wal, capacity: usize) -> Self {
        BufferPool {
            disk,
            wal,
            pages: HashMap::new(),
            dirty: HashMap::new(),
            page_lsn: HashMap::new(),
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

    pub fn write_page(&mut self, page_id: u32, mut data: [u8; PAGE_SIZE]) {
        if !self.pages.contains_key(&page_id){
            while self.pages.len() >= self.capacity {
                self.evict();
            }
        }

        let lsn = self.wal.log_page(page_id, &data);

        set_page_lsn(&mut data, lsn);
        
        
    
        self.pages.insert(page_id, data);
        self.dirty.insert(page_id, true);
        self.page_lsn.insert(page_id, lsn);
        self.touch(page_id);
    }

    pub fn flush(&mut self) {
        self.wal.flush();
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

                let page_lsn = self.page_lsn.get(&victim_id).copied().unwrap_or(0);
                self.wal.flush_to(page_lsn);
                if let Some(data) = self.pages.get(&victim_id){
                    self.disk.write_page(victim_id, data);
                }
                self.dirty.remove(&victim_id);
            }

            self.pages.remove(&victim_id);
            self.page_lsn.remove(&victim_id);
            self.lru_order.remove(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::DiskManager;
    use crate::page::{PAGE_HEADER_SIZE, get_page_lsn};
    use crate::wal::Wal;

    const M: usize = PAGE_HEADER_SIZE; // marker offset (bytes 0-7 are LSN)

    fn make_pool(db: &str, wal: &str, capacity: usize) -> BufferPool {
        let dm = DiskManager::new(db);
        let w = Wal::new(wal);
        BufferPool::new(dm, w, capacity)
    }

    fn make_page(marker: u8) -> [u8; PAGE_SIZE] {
        let mut data = [0u8; PAGE_SIZE];
        data[M] = marker; // put marker after the LSN header
        data
    }

    fn cleanup(files: &[&str]) {
        for f in files {
            let _ = std::fs::remove_file(f);
        }
    }

    // ---- LRU buffer pool tests ----

    #[test]
    fn test_basic_get_and_write() {
        let (db, wal) = ("test_buf_basic.db", "test_buf_basic.wal");
        let mut pool = make_pool(db, wal, 4);

        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));

        assert_eq!(pool.get_page(0)[M], 10);
        assert_eq!(pool.get_page(1)[M], 20);

        cleanup(&[db, wal]);
    }

    #[test]
    fn test_capacity_respected() {
        let (db, wal) = ("test_buf_capacity.db", "test_buf_capacity.wal");
        let mut pool = make_pool(db, wal, 3);

        for i in 0..10 {
            pool.write_page(i, make_page(i as u8));
            assert!(pool.pages.len() <= 3, "pool exceeded capacity");
        }

        cleanup(&[db, wal]);
    }

    #[test]
    fn test_eviction_flushes_dirty_page() {
        let (db, wal) = ("test_buf_evict_dirty.db", "test_buf_evict_dirty.wal");
        let mut pool = make_pool(db, wal, 3);

        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));
        pool.write_page(2, make_page(30));

        // Write page 3 — forces eviction of page 0 (LRU)
        pool.write_page(3, make_page(40));
        assert!(!pool.pages.contains_key(&0), "page 0 should be evicted");

        // Page 0 was dirty so it was flushed. Re-read should get original data.
        let page = pool.get_page(0);
        assert_eq!(page[M], 10, "dirty page 0 should survive eviction via disk");

        cleanup(&[db, wal]);
    }

    #[test]
    fn test_lru_order_updated_on_read() {
        let (db, wal) = ("test_buf_lru_read.db", "test_buf_lru_read.wal");
        let mut pool = make_pool(db, wal, 3);

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

        cleanup(&[db, wal]);
    }

    #[test]
    fn test_lru_order_updated_on_write() {
        let (db, wal) = ("test_buf_lru_write.db", "test_buf_lru_write.wal");
        let mut pool = make_pool(db, wal, 3);

        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));
        pool.write_page(2, make_page(30));

        // Overwrite page 0 — moves it to most recent. LRU order: [1, 2, 0]
        pool.write_page(0, make_page(15));

        // Insert page 3 — should evict page 1
        pool.write_page(3, make_page(40));

        assert!(pool.pages.contains_key(&0));
        assert!(!pool.pages.contains_key(&1));
        assert_eq!(pool.get_page(0)[M], 15, "page 0 should have updated value");

        cleanup(&[db, wal]);
    }

    #[test]
    fn test_evicted_clean_page_reloads_from_disk() {
        let (db, wal) = ("test_buf_clean_reload.db", "test_buf_clean_reload.wal");
        let mut pool = make_pool(db, wal, 3);

        pool.write_page(0, make_page(10));
        pool.flush();

        pool.write_page(1, make_page(20));
        pool.write_page(2, make_page(30));

        pool.get_page(0);
        pool.write_page(3, make_page(40));
        pool.write_page(4, make_page(50));

        let page = pool.get_page(0);
        assert_eq!(page[M], 10, "clean page should reload from disk correctly");

        cleanup(&[db, wal]);
    }

    #[test]
    fn test_many_evictions_data_integrity() {
        let (db, wal) = ("test_buf_integrity.db", "test_buf_integrity.wal");
        let mut pool = make_pool(db, wal, 4);

        for i in 0u32..20 {
            pool.write_page(i, make_page(i as u8));
        }

        for i in 0u32..20 {
            let page = pool.get_page(i);
            assert_eq!(page[M], i as u8, "page {} has wrong data after evictions", i);
        }

        cleanup(&[db, wal]);
    }

    #[test]
    fn test_flush_writes_all_dirty_pages() {
        let (db, wal) = ("test_buf_flush.db", "test_buf_flush.wal");
        let mut pool = make_pool(db, wal, 4);

        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));
        pool.flush();

        assert!(pool.dirty.is_empty(), "flush should clear dirty map");

        assert_eq!(pool.get_page(0)[M], 10);
        assert_eq!(pool.get_page(1)[M], 20);

        cleanup(&[db, wal]);
    }

    #[test]
    fn test_capacity_one() {
        let (db, wal) = ("test_buf_cap1.db", "test_buf_cap1.wal");
        let mut pool = make_pool(db, wal, 1);

        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));
        pool.write_page(2, make_page(30));

        assert_eq!(pool.pages.len(), 1);

        assert_eq!(pool.get_page(0)[M], 10);
        assert_eq!(pool.get_page(1)[M], 20);
        assert_eq!(pool.get_page(2)[M], 30);

        cleanup(&[db, wal]);
    }

    // ---- WAL integration tests ----

    #[test]
    fn test_write_page_stamps_lsn() {
        let (db, wal) = ("test_buf_lsn.db", "test_buf_lsn.wal");
        let mut pool = make_pool(db, wal, 4);

        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));

        let lsn0 = get_page_lsn(pool.get_page(0));
        let lsn1 = get_page_lsn(pool.get_page(1));

        assert_eq!(lsn0, 0, "first write should get LSN 0");
        assert_eq!(lsn1, 1, "second write should get LSN 1");

        cleanup(&[db, wal]);
    }

    #[test]
    fn test_lsn_survives_eviction() {
        let (db, wal) = ("test_buf_lsn_evict.db", "test_buf_lsn_evict.wal");
        let mut pool = make_pool(db, wal, 2);

        pool.write_page(0, make_page(10)); // LSN 0
        pool.write_page(1, make_page(20)); // LSN 1
        pool.write_page(2, make_page(30)); // LSN 2, evicts page 0

        // Page 0 was evicted and flushed to disk. Re-read it.
        let page = pool.get_page(0);
        let lsn = get_page_lsn(page);
        assert_eq!(lsn, 0, "LSN should survive eviction round-trip");
        assert_eq!(page[M], 10);

        cleanup(&[db, wal]);
    }

    #[test]
    fn test_wal_records_written() {
        let (db, wal_file) = ("test_buf_wal_records.db", "test_buf_wal_records.wal");
        let mut pool = make_pool(db, wal_file, 4);

        pool.write_page(0, make_page(10));
        pool.write_page(1, make_page(20));
        pool.write_page(2, make_page(30));
        pool.flush();

        // Reopen WAL and verify records are there
        let mut wal = Wal::new(wal_file);
        let records = wal.read_from(0);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].page_id, 0);
        assert_eq!(records[1].page_id, 1);
        assert_eq!(records[2].page_id, 2);

        cleanup(&[db, wal_file]);
    }
}
