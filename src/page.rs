use crate::disk::PAGE_SIZE;

pub fn serialize_node(
    is_leaf: bool,
    keys: &[i32],
    values: &[String],
    children: &[u32], // page ids, not btreenode pointers
) -> [u8; PAGE_SIZE] {
    let mut buf = [0u8; PAGE_SIZE];
    let mut offset = 0;

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

    // values ( 64 bytes each, zero-padded )
    for val in values {
        let bytes = val.as_bytes();
        let len = bytes.len().min(64);
        buf[offset..offset + len].copy_from_slice(&bytes[..len]);
        offset += 64;
    }

    for child_id in children {
        buf[offset..offset + 4].copy_from_slice(&child_id.to_le_bytes());
        offset += 4;
    }

    buf
}

pub fn deserialize_node(buf: &[u8; PAGE_SIZE]) -> (bool, Vec<i32>, Vec<String>, Vec<u32>) {
    let mut offset = 0;

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

    let mut values = Vec::new();
    for _ in 0..num_keys {
        let end = buf[offset..offset + 64]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(64);

        let val = String::from_utf8_lossy(&buf[offset..offset + end]).to_string();

        values.push(val);
        offset += 64;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_leaf() {
        let keys = vec![10, 20, 30];
        let values = vec!["hello".to_string(), "world".to_string(), "foo".to_string()];
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
        let values = vec!["a".to_string(), "b".to_string()];
        let children: Vec<u32> = vec![0, 1, 2];

        let buf = serialize_node(false, &keys, &values, &children);
        let (is_leaf, out_keys, out_values, out_children) = deserialize_node(&buf);

        assert_eq!(is_leaf, false);
        assert_eq!(out_keys, keys);
        assert_eq!(out_values, values);
        assert_eq!(out_children, children);
    }
}
