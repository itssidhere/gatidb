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
            if let Some(data) = self.pages.get(&victim_id) {
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
