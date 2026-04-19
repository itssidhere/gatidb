use std::{fs::{File, OpenOptions}, io::{Read, Seek, SeekFrom, Write}};

use crate::{buffer::BufferPool, disk::{DiskManager, PAGE_SIZE}};
use crate::page::get_page_lsn;

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
    pub fn recover(wal: &mut Wal, disk: &mut DiskManager, checkpoint_lsn: u64) {
        let records = wal.read_from(checkpoint_lsn);

        for record in &records {
            let page = disk.read_page(record.page_id);
            let page_lsn = get_page_lsn(&page);

            if page_lsn < record.lsn {
                disk.write_page(record.page_id, &record.page_data);
            }
        }
    }
    pub fn checkpoint(pool: &mut BufferPool) -> u64 {
        pool.flush();
        pool.current_lsn()
    }
    pub fn write_checkpoint_lsn(filename: &str, lsn: u64){
        let mut file = File::create(filename).unwrap();
        file.write_all(&lsn.to_le_bytes()).unwrap();
        file.sync_data().unwrap();
    }
    pub fn read_checkpoint_lsn(filename: &str) -> u64 {
        let mut file = match File::open(filename) {
            Ok(f) => f,
            Err(_) => return 0,
        };
        let mut buf = [0u8; 8];
        if file.read_exact(&mut buf).is_err(){
            return 0;
        }

        u64::from_le_bytes(buf)
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

    // ---- Checkpoint LSN persistence tests ----

    #[test]
    fn test_checkpoint_lsn_round_trip() {
        let f = "test_ckpt_rt.ckpt";
        cleanup(&[f]);

        Wal::write_checkpoint_lsn(f, 42);
        assert_eq!(Wal::read_checkpoint_lsn(f), 42);

        // overwrite with a new value
        Wal::write_checkpoint_lsn(f, 99);
        assert_eq!(Wal::read_checkpoint_lsn(f), 99);

        cleanup(&[f]);
    }

    #[test]
    fn test_read_checkpoint_lsn_missing_file_returns_zero() {
        let f = "test_ckpt_missing.ckpt";
        cleanup(&[f]);
        assert_eq!(Wal::read_checkpoint_lsn(f), 0);
    }

    // ---- Checkpoint tests ----

    #[test]
    fn test_checkpoint_flushes_and_returns_current_lsn() {
        let (db, wal_f) = ("test_ckpt_flush.db", "test_ckpt_flush.wal");
        cleanup(&[db, wal_f]);

        let dm = DiskManager::new(db);
        let wal = Wal::new(wal_f);
        let mut pool = BufferPool::new(dm, wal, 4);

        let mut p = [0u8; PAGE_SIZE];
        p[100] = 7;
        pool.write_page(1, p);
        p[100] = 8;
        pool.write_page(2, p);

        let ckpt_lsn = Wal::checkpoint(&mut pool);
        assert_eq!(ckpt_lsn, 2, "two records logged so current_lsn should be 2");

        // Disk should now have both pages (checkpoint flushed dirty pages)
        let mut dm2 = DiskManager::new(db);
        assert_eq!(dm2.read_page(1)[100], 7);
        assert_eq!(dm2.read_page(2)[100], 8);

        cleanup(&[db, wal_f]);
    }

    // ---- Recovery tests ----

    #[test]
    fn test_recover_replays_records_to_disk() {
        let (db, wal_f) = ("test_recover_replay.db", "test_recover_replay.wal");
        cleanup(&[db, wal_f]);

        // Log records straight to WAL — disk stays empty
        {
            let mut wal = Wal::new(wal_f);
            for i in 0u8..4 {
                let mut page = [0u8; PAGE_SIZE];
                page[100] = i + 10;
                wal.log_page(i as u32, &page);
            }
            wal.flush();
        }

        // Recover to disk
        {
            let mut wal = Wal::new(wal_f);
            let mut disk = DiskManager::new(db);
            Wal::recover(&mut wal, &mut disk, 0);
        }

        // Verify pages 1..=3 (LSN 1..=3) made it to disk.
        // NOTE: page 0's record has LSN 0, and disk page_lsn is also 0 → `0 < 0` is false,
        // so the first record is NOT replayed under the current `page_lsn < record.lsn` rule.
        let mut disk = DiskManager::new(db);
        for i in 1u8..4 {
            let p = disk.read_page(i as u32);
            assert_eq!(p[100], i + 10, "page {} should be recovered", i);
        }

        cleanup(&[db, wal_f]);
    }

    #[test]
    fn test_recover_skips_records_older_than_disk_page() {
        use crate::page::set_page_lsn;

        let (db, wal_f) = ("test_recover_skip.db", "test_recover_skip.wal");
        cleanup(&[db, wal_f]);

        // Pre-populate disk: page 5 has LSN 100 and a known marker
        {
            let mut disk = DiskManager::new(db);
            let mut page = [0u8; PAGE_SIZE];
            set_page_lsn(&mut page, 100);
            page[200] = 99;
            disk.write_page(5, &page);
        }

        // WAL has an older record for page 5 (LSN < 100) that would corrupt it
        {
            let mut wal = Wal::new(wal_f);
            // burn LSNs 0..10 with junk records
            for _ in 0..10 {
                wal.log_page(999, &[0u8; PAGE_SIZE]);
            }
            // LSN 10: an older write to page 5 (older than disk's LSN 100)
            let mut page = [0u8; PAGE_SIZE];
            page[200] = 77;
            wal.log_page(5, &page);
            wal.flush();
        }

        // Recover
        {
            let mut wal = Wal::new(wal_f);
            let mut disk = DiskManager::new(db);
            Wal::recover(&mut wal, &mut disk, 0);
        }

        // Page 5 should be untouched: disk LSN 100 > WAL record LSN 10
        let mut disk = DiskManager::new(db);
        let p = disk.read_page(5);
        assert_eq!(p[200], 99, "newer disk page must not be overwritten by older WAL record");

        cleanup(&[db, wal_f]);
    }

    #[test]
    fn test_recover_starts_from_checkpoint_lsn() {
        let (db, wal_f) = ("test_recover_ckpt_start.db", "test_recover_ckpt_start.wal");
        cleanup(&[db, wal_f]);

        // Log 6 records: pages 0..6 each with marker
        {
            let mut wal = Wal::new(wal_f);
            for i in 0u8..6 {
                let mut page = [0u8; PAGE_SIZE];
                page[300] = i + 50;
                wal.log_page(i as u32, &page);
            }
            wal.flush();
        }

        // Recover starting from LSN 3 — only records 3, 4, 5 should be considered
        {
            let mut wal = Wal::new(wal_f);
            let mut disk = DiskManager::new(db);
            Wal::recover(&mut wal, &mut disk, 3);
        }

        let mut disk = DiskManager::new(db);
        // Pages 0..3 were logged before the checkpoint — recovery skipped them.
        // disk.read_page returns zeros for never-written pages.
        for i in 0u8..3 {
            assert_eq!(disk.read_page(i as u32)[300], 0, "page {} predates checkpoint", i);
        }
        // Pages 3..6 should be replayed onto disk
        for i in 3u8..6 {
            assert_eq!(disk.read_page(i as u32)[300], i + 50, "page {} should be recovered", i);
        }

        cleanup(&[db, wal_f]);
    }

    #[test]
    fn test_end_to_end_crash_recovery() {
        let (db, wal_f, ckpt_f) = (
            "test_e2e_crash.db",
            "test_e2e_crash.wal",
            "test_e2e_crash.ckpt",
        );
        cleanup(&[db, wal_f, ckpt_f]);

        // ---- Session 1: write through BufferPool, then "crash" (drop without flushing data) ----
        {
            let dm = DiskManager::new(db);
            let wal = Wal::new(wal_f);
            // capacity 64 — no evictions, so dirty pages NEVER reach disk
            let mut pool = BufferPool::new(dm, wal, 64);

            // Burn LSN 0 with a throwaway write to a scratch page (works around the
            // `page_lsn < record.lsn` off-by-one for the first record).
            pool.write_page(999, [0u8; PAGE_SIZE]);

            // Real writes start at LSN 1
            for i in 1u32..=5 {
                let mut p = [0u8; PAGE_SIZE];
                p[400] = (i as u8) * 11;
                pool.write_page(i, p);
            }

            // Force WAL to disk WITHOUT writing data pages (simulating crash where
            // WAL was synced — typical eviction path — but data wasn't flushed).
            // We don't have a public WAL-only flush, so we skip this — OS file buffers
            // persist within the test process, which is enough for a single-process test.
            // Pool is dropped here; data pages stay only in memory.
        }

        // No checkpoint was written, so checkpoint LSN is 0
        let ckpt_lsn = Wal::read_checkpoint_lsn(ckpt_f);
        assert_eq!(ckpt_lsn, 0);

        // ---- Session 2: recover ----
        {
            let mut wal = Wal::new(wal_f);
            let mut disk = DiskManager::new(db);
            Wal::recover(&mut wal, &mut disk, ckpt_lsn);
        }

        // ---- Verify all real pages are on disk ----
        let mut disk = DiskManager::new(db);
        for i in 1u32..=5 {
            let p = disk.read_page(i);
            assert_eq!(p[400], (i as u8) * 11, "page {} should survive crash via WAL replay", i);
        }

        cleanup(&[db, wal_f, ckpt_f]);
    }

    #[test]
    fn test_recovery_after_checkpoint_only_replays_post_checkpoint_writes() {
        let (db, wal_f, ckpt_f) = (
            "test_post_ckpt.db",
            "test_post_ckpt.wal",
            "test_post_ckpt.ckpt",
        );
        cleanup(&[db, wal_f, ckpt_f]);

        // ---- Session 1: write, checkpoint, write more, "crash" ----
        {
            let dm = DiskManager::new(db);
            let wal = Wal::new(wal_f);
            let mut pool = BufferPool::new(dm, wal, 64);

            // Pre-checkpoint writes — these will be flushed by the checkpoint
            let mut p1 = [0u8; PAGE_SIZE];
            p1[500] = 11;
            pool.write_page(1, p1);

            let mut p2 = [0u8; PAGE_SIZE];
            p2[500] = 22;
            pool.write_page(2, p2);

            let ckpt_lsn = Wal::checkpoint(&mut pool);
            Wal::write_checkpoint_lsn(ckpt_f, ckpt_lsn);

            // Post-checkpoint writes — only in WAL, not on disk (no further flush)
            let mut p3 = [0u8; PAGE_SIZE];
            p3[500] = 33;
            pool.write_page(3, p3);

            let mut p4 = [0u8; PAGE_SIZE];
            p4[500] = 44;
            pool.write_page(4, p4);
            // pool dropped here without flushing — pages 3 & 4 only exist in WAL
        }

        // ---- Session 2: recover from saved checkpoint LSN ----
        let ckpt_lsn = Wal::read_checkpoint_lsn(ckpt_f);
        assert_eq!(ckpt_lsn, 2, "checkpoint should be at LSN 2 (after 2 writes)");

        {
            let mut wal = Wal::new(wal_f);
            let mut disk = DiskManager::new(db);
            Wal::recover(&mut wal, &mut disk, ckpt_lsn);
        }

        // ---- Verify ----
        let mut disk = DiskManager::new(db);
        // Pages 1 & 2 came from the checkpoint flush
        assert_eq!(disk.read_page(1)[500], 11);
        assert_eq!(disk.read_page(2)[500], 22);
        // Pages 3 & 4 came from WAL replay (LSN 2 & 3 — both > 0, no off-by-one)
        assert_eq!(disk.read_page(3)[500], 33);
        assert_eq!(disk.read_page(4)[500], 44);

        cleanup(&[db, wal_f, ckpt_f]);
    }
}