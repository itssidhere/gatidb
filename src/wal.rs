use std::{fs::{File, OpenOptions}, io::{Read, Seek, SeekFrom, Write}};

use crate::disk::PAGE_SIZE;

pub const WAL_RECORD_SIZE : usize = 8 + 4 + PAGE_SIZE;

pub struct WalRecord {
    pub lsn: u64,
    pub page_id: u32,
    pub page_data: [u8; PAGE_SIZE],
}

pub struct Wal {
    file: File,
    current_lsn: u64,
    flushed_lsn: u64
}

impl WalRecord {
    pub fn serialize(&self) -> [u8; WAL_RECORD_SIZE] {
        let mut buf = [0u8; WAL_RECORD_SIZE];
        buf[0..8].copy_from_slice(&self.lsn.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_id.to_le_bytes());
        buf[12..12 + PAGE_SIZE].copy_from_slice(&self.page_data);
        buf
    }
    pub fn deserialize(buf: [u8; WAL_RECORD_SIZE]) -> Self {
        let lsn = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let page_id = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let mut page_data = [0u8; PAGE_SIZE];
        page_data.copy_from_slice(&buf[12..12+PAGE_SIZE]); 
        WalRecord { lsn:lsn, page_id: page_id, page_data: page_data }
    }
}

impl Wal{
    pub fn new(filename: &str) -> Self {
        let file = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();

        let file_len = file.metadata().unwrap().len();
        let current_lsn = file_len / WAL_RECORD_SIZE as u64;

        Wal { file, current_lsn: current_lsn, flushed_lsn: current_lsn }
    }

    pub fn log_page(&mut self, page_id: u32, page_data: &[u8; PAGE_SIZE]) -> u64 {
        let lsn = self.current_lsn;
        self.current_lsn += 1;

        let record = WalRecord {
            lsn,
            page_id,
            page_data: *page_data,
        };

        let offset = lsn * WAL_RECORD_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset)).unwrap();

        self.file.write_all(&record.serialize()).unwrap();
        lsn

    }

    pub fn flush(&mut self){
        self.file.sync_data().unwrap();
        self.flushed_lsn = self.current_lsn;
    }

    pub fn flush_to(&mut self, target_lsn : u64) {
        if self.flushed_lsn <= target_lsn {
            self.flush();
        }
    }

    pub fn flushed_lsn(&self) -> u64 {
        self.flushed_lsn
    }

    pub fn current_lsn(&self) -> u64 {
        self.current_lsn
    }

    pub fn read_from(&mut self, start_lsn: u64) -> Vec<WalRecord> {
        let mut records = Vec::new();
        let mut lsn = start_lsn;

        loop {
            let offset = lsn * WAL_RECORD_SIZE as u64;
            self.file.seek(SeekFrom::Start(offset)).unwrap();

            let mut buf = [0u8; WAL_RECORD_SIZE];
            if self.file.read_exact(&mut buf).is_err() {
                break;
            }

            records.push(WalRecord::deserialize(buf));
            lsn += 1
        }

        records
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cleanup(files: &[&str]) {
        for f in files {
            let _ = std::fs::remove_file(f);
        }
    }

    // ---- WalRecord tests ----

    #[test]
    fn test_wal_record_round_trip() {
        let mut data = [0u8; PAGE_SIZE];
        data[0] = 42;
        data[100] = 99;
        data[4095] = 255;

        let record = WalRecord { lsn: 7, page_id: 5, page_data: data };
        let bytes = record.serialize();
        let recovered = WalRecord::deserialize(bytes);

        assert_eq!(recovered.lsn, 7);
        assert_eq!(recovered.page_id, 5);
        assert_eq!(recovered.page_data[0], 42);
        assert_eq!(recovered.page_data[100], 99);
        assert_eq!(recovered.page_data[4095], 255);
    }

    #[test]
    fn test_wal_record_zero_page() {
        let record = WalRecord { lsn: 0, page_id: 0, page_data: [0u8; PAGE_SIZE] };
        let bytes = record.serialize();
        let recovered = WalRecord::deserialize(bytes);
        assert_eq!(recovered.lsn, 0);
        assert_eq!(recovered.page_id, 0);
        assert_eq!(recovered.page_data, [0u8; PAGE_SIZE]);
    }

    // ---- Wal writer tests ----

    #[test]
    fn test_wal_log_and_read() {
        let f = "test_wal_log_read.wal";

        let mut wal = Wal::new(f);

        let mut page1 = [0u8; PAGE_SIZE];
        page1[0] = 10;
        let lsn1 = wal.log_page(0, &page1);

        let mut page2 = [0u8; PAGE_SIZE];
        page2[0] = 20;
        let lsn2 = wal.log_page(1, &page2);

        assert_eq!(lsn1, 0);
        assert_eq!(lsn2, 1);

        let records = wal.read_from(0);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].lsn, 0);
        assert_eq!(records[0].page_id, 0);
        assert_eq!(records[0].page_data[0], 10);
        assert_eq!(records[1].lsn, 1);
        assert_eq!(records[1].page_id, 1);
        assert_eq!(records[1].page_data[0], 20);

        cleanup(&[f]);
    }

    #[test]
    fn test_wal_read_from_middle() {
        let f = "test_wal_read_mid.wal";
        let mut wal = Wal::new(f);

        for i in 0u8..5 {
            let mut page = [0u8; PAGE_SIZE];
            page[0] = i;
            wal.log_page(i as u32, &page);
        }

        let records = wal.read_from(3);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].lsn, 3);
        assert_eq!(records[0].page_data[0], 3);
        assert_eq!(records[1].lsn, 4);

        cleanup(&[f]);
    }

    #[test]
    fn test_wal_persists_across_reopen() {
        let f = "test_wal_persist.wal";

        {
            let mut wal = Wal::new(f);
            let mut page = [0u8; PAGE_SIZE];
            page[0] = 77;
            wal.log_page(5, &page);
            wal.flush();
        }

        {
            let mut wal = Wal::new(f);
            assert_eq!(wal.current_lsn(), 1);
            let records = wal.read_from(0);
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].page_id, 5);
            assert_eq!(records[0].page_data[0], 77);
        }

        cleanup(&[f]);
    }

    #[test]
    fn test_wal_flush_updates_flushed_lsn() {
        let f = "test_wal_flush_lsn.wal";
        let mut wal = Wal::new(f);

        assert_eq!(wal.flushed_lsn(), 0);

        wal.log_page(0, &[0u8; PAGE_SIZE]);
        wal.log_page(1, &[0u8; PAGE_SIZE]);

        // Not flushed yet — flushed_lsn still 0
        assert_eq!(wal.flushed_lsn(), 0);

        wal.flush();
        assert_eq!(wal.flushed_lsn(), 2);

        cleanup(&[f]);
    }

    #[test]
    fn test_wal_flush_to_conditional() {
        let f = "test_wal_flush_to.wal";
        let mut wal = Wal::new(f);

        wal.log_page(0, &[0u8; PAGE_SIZE]); // LSN 0
        wal.log_page(1, &[0u8; PAGE_SIZE]); // LSN 1
        wal.flush(); // flushed_lsn = 2

        wal.log_page(2, &[0u8; PAGE_SIZE]); // LSN 2

        // flush_to(1) should be a no-op since flushed_lsn(2) > 1
        let old_flushed = wal.flushed_lsn();
        wal.flush_to(1);
        assert_eq!(wal.flushed_lsn(), old_flushed);

        // flush_to(2) should actually flush since flushed_lsn(2) <= 2
        wal.flush_to(2);
        assert_eq!(wal.flushed_lsn(), 3);

        cleanup(&[f]);
    }

    #[test]
    fn test_wal_empty_read() {
        let f = "test_wal_empty.wal";
        let mut wal = Wal::new(f);

        let records = wal.read_from(0);
        assert_eq!(records.len(), 0);

        cleanup(&[f]);
    }

    #[test]
    fn test_wal_multiple_writes_same_page() {
        let f = "test_wal_multi_write.wal";
        let mut wal = Wal::new(f);

        // Write page 0 three times with different data
        for i in 0u8..3 {
            let mut page = [0u8; PAGE_SIZE];
            page[0] = i * 10;
            wal.log_page(0, &page);
        }

        let records = wal.read_from(0);
        assert_eq!(records.len(), 3);
        // All three records for page 0, each with different data
        assert_eq!(records[0].page_data[0], 0);
        assert_eq!(records[1].page_data[0], 10);
        assert_eq!(records[2].page_data[0], 20);

        cleanup(&[f]);
    }
}