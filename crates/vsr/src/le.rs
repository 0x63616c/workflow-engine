fn array<const N: usize>(buf: &[u8], at: usize) -> [u8; N] {
    buf[at..at + N].try_into().expect("a slice of exactly N bytes always converts to [u8; N]")
}

pub(crate) fn u32_at(buf: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(array(buf, at))
}

pub(crate) fn u64_at(buf: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(array(buf, at))
}

pub(crate) fn u128_at(buf: &[u8], at: usize) -> u128 {
    u128::from_le_bytes(array(buf, at))
}
