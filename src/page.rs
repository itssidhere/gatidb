use crate::disk::PAGE_SIZE;

pub const VALUE_SIZE: usize = 256;

pub const PAGE_HEADER_SIZE: usize = 8; // 8 bytes for u64 LSN

pub fn serialize_node(
    is_leaf: bool,
    keys: &[i32],
    values: &[Vec<u8>],
    children: &[u32], // page ids, not btreenode pointers
) -> [u8; PAGE_SIZE] {
    let mut buf = [0u8; PAGE_SIZE];
    let mut offset = PAGE_HEADER_SIZE;

    // is_leaf
    buf[offset] = is_leaf as u8;
    offset += 1;

    // num_keys
    let num_keys = keys.len() as u16;
    buf[offset..offset + 2].copy_from_slice(&num_keys.to_le_bytes());
    offset += 2;

    // keys
    for key in keys {
        buf[offset..offset + 4].copy_from_slice(&key.to_le_bytes());
        offset += 4;
    }

    // values (VALUE_SIZE bytes each: 2-byte length prefix + data + zero padding)
    for val in values {
        let len = val.len().min(VALUE_SIZE - 2);
        buf[offset..offset + 2].copy_from_slice(&(len as u16).to_le_bytes());
        buf[offset + 2..offset + 2 + len].copy_from_slice(&val[..len]);
        offset += VALUE_SIZE;
    }

    for child_id in children {
        buf[offset..offset + 4].copy_from_slice(&child_id.to_le_bytes());
        offset += 4;
    }

    buf
}

pub fn deserialize_node(buf: &[u8; PAGE_SIZE]) -> (bool, Vec<i32>, Vec<Vec<u8>>, Vec<u32>) {
    let mut offset = PAGE_HEADER_SIZE;

    let is_leaf = buf[offset] != 0;
    offset += 1;

    let num_keys = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
    offset += 2;

    let mut keys = Vec::new();
    for _ in 0..num_keys {
        let key = i32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);

        keys.push(key);
        offset += 4;
    }

    let mut values: Vec<Vec<u8>> = Vec::new();
    for _ in 0..num_keys {
        let len = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
        let val = buf[offset + 2..offset + 2 + len].to_vec();
        values.push(val);
        offset += VALUE_SIZE;
    }

    let num_children = if is_leaf { 0 } else { num_keys + 1 };
    let mut children = Vec::new();

    for _ in 0..num_children {
        let id = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);

        children.push(id);
        offset += 4;
    }

    (is_leaf, keys, values, children)
}

pub fn get_page_lsn(page: &[u8; PAGE_SIZE]) -> u64{
    u64::from_le_bytes([
        page[0], page[1], page[2], page[3],
        page[4], page[5], page[6], page[7],
    ])
}
pub fn set_page_lsn(page: &mut [u8; PAGE_SIZE], lsn:u64){
    page[0..8].copy_from_slice(&lsn.to_le_bytes());
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_leaf() {
        let keys = vec![10, 20, 30];
        let values = vec![b"hello".to_vec(), b"world".to_vec(), b"foo".to_vec()];
        let children: Vec<u32> = vec![];

        let buf = serialize_node(true, &keys, &values, &children);
        let (is_leaf, out_keys, out_values, out_children) = deserialize_node(&buf);

        assert_eq!(is_leaf, true);
        assert_eq!(out_keys, keys);
        assert_eq!(out_values, values);
        assert_eq!(out_children, children);
    }

    #[test]
    fn test_serialize_deserialize_internal() {
        let keys = vec![10, 20];
        let values = vec![b"a".to_vec(), b"b".to_vec()];
        let children: Vec<u32> = vec![0, 1, 2];

        let buf = serialize_node(false, &keys, &values, &children);
        let (is_leaf, out_keys, out_values, out_children) = deserialize_node(&buf);

        assert_eq!(is_leaf, false);
        assert_eq!(out_keys, keys);
        assert_eq!(out_values, values);
        assert_eq!(out_children, children);
    }

    #[test]
    fn test_page_lsn_roundtrip(){
        let mut page = serialize_node(true, &[10, 20], &[b"a".to_vec(), b"b".to_vec()], &[]);
        assert_eq!(get_page_lsn(&page), 0); // default

        set_page_lsn(&mut page, 42);
        assert_eq!(get_page_lsn(&page), 42);

        // LSN doesn't interfere with node data
        let (is_leaf, keys, values, _) = deserialize_node(&page);
        assert!(is_leaf);
        assert_eq!(keys, vec![10, 20]);
    }
}
