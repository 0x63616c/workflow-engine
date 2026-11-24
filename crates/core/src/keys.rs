//! Order-preserving key encoding for the storage engine.
//!
//! Keys are built from typed segments so that byte-wise comparison of the
//! encoded key equals lexicographic comparison of the segment tuple:
//! - integers are big-endian;
//! - strings/bytes are escaped (`0x00` -> `0x00 0xFF`) and terminated by
//!   `0x00 0x01`, so no string is a prefix of another segment's encoding.

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key(pub Vec<u8>);

impl Key {
    pub fn new(prefix: u8) -> Key {
        Key(vec![prefix])
    }

    pub fn u8(mut self, v: u8) -> Key {
        self.0.push(v);
        self
    }

    pub fn u32(mut self, v: u32) -> Key {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn u64(mut self, v: u64) -> Key {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn u128(mut self, v: u128) -> Key {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn str(self, v: &str) -> Key {
        self.bytes(v.as_bytes())
    }

    pub fn bytes(mut self, v: &[u8]) -> Key {
        for &b in v {
            if b == 0 {
                self.0.extend_from_slice(&[0x00, 0xFF]);
            } else {
                self.0.push(b);
            }
        }
        self.0.extend_from_slice(&[0x00, 0x01]);
        self
    }

    /// Append raw, already-encoded bytes (for composing with a prefix).
    pub fn raw(mut self, v: &[u8]) -> Key {
        self.0.extend_from_slice(v);
        self
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// The smallest key strictly greater than every key that has this key as
    /// a prefix. Use as the exclusive end bound of a prefix scan.
    pub fn prefix_end(&self) -> Vec<u8> {
        prefix_end(&self.0)
    }
}

/// The exclusive upper bound of the keys that start with `prefix`. An empty or
/// all-`0xFF` prefix has no such bound; it gets `0xFF` repeated one byte longer.
pub fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 0xFF {
            end.push(last + 1);
            return end;
        }
    }
    vec![0xFF; prefix.len() + 1]
}

/// Sequential decoder for keys produced by [`Key`].
pub struct KeyReader<'a> {
    rest: &'a [u8],
}

impl<'a> KeyReader<'a> {
    pub fn new(key: &'a [u8]) -> KeyReader<'a> {
        KeyReader { rest: key }
    }

    pub fn rest(&self) -> &'a [u8] {
        self.rest
    }

    pub fn is_empty(&self) -> bool {
        self.rest.is_empty()
    }

    pub fn u8(&mut self) -> Option<u8> {
        let (&b, rest) = self.rest.split_first()?;
        self.rest = rest;
        Some(b)
    }

    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.rest.split_first_chunk::<N>()?;
        self.rest = rest;
        Some(*head)
    }

    pub fn u32(&mut self) -> Option<u32> {
        self.take::<4>().map(u32::from_be_bytes)
    }

    pub fn u64(&mut self) -> Option<u64> {
        self.take::<8>().map(u64::from_be_bytes)
    }

    pub fn u128(&mut self) -> Option<u128> {
        self.take::<16>().map(u128::from_be_bytes)
    }

    pub fn bytes(&mut self) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        let mut i = 0;
        loop {
            let b = *self.rest.get(i)?;
            if b == 0 {
                match *self.rest.get(i + 1)? {
                    0xFF => {
                        out.push(0);
                        i += 2;
                    }
                    0x01 => {
                        self.rest = &self.rest[i + 2..];
                        return Some(out);
                    }
                    _ => return None,
                }
            } else {
                out.push(b);
                i += 1;
            }
        }
    }

    pub fn str(&mut self) -> Option<String> {
        String::from_utf8(self.bytes()?).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_matches_tuple_ordering() {
        let a = Key::new(1).str("a").u64(5).into_vec();
        let b = Key::new(1).str("a").u64(6).into_vec();
        let c = Key::new(1).str("ab").u64(0).into_vec();
        let d = Key::new(1).str("b").u64(0).into_vec();
        assert!(a < b && b < c && c < d);
    }

    #[test]
    fn round_trip_with_embedded_zero() {
        let k = Key::new(9).bytes(b"x\0y").u32(7).str("z").into_vec();
        let mut r = KeyReader::new(&k);
        assert_eq!(r.u8(), Some(9));
        assert_eq!(r.bytes().as_deref(), Some(&b"x\0y"[..]));
        assert_eq!(r.u32(), Some(7));
        assert_eq!(r.str().as_deref(), Some("z"));
        assert!(r.is_empty());
    }

    #[test]
    fn prefix_end_bounds_prefix() {
        let p = Key::new(3).str("q");
        let inside = Key::new(3).str("q").u64(u64::MAX).into_vec();
        assert!(p.as_slice() <= inside.as_slice());
        assert!(inside < p.prefix_end());
    }
}
