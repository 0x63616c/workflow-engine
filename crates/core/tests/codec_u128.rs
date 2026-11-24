use serde::{Deserialize, Serialize};
use wfe_core::codec::{decode, encode};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct S {
    a: u128,
    b: Option<u128>,
    c: Vec<u128>,
}

#[test]
fn u128_round_trips_through_codec() {
    let s = S { a: u128::MAX - 5, b: Some(7), c: vec![0, 1, u128::MAX] };
    let bytes = encode(&s);
    assert_eq!(decode::<S>(&bytes).unwrap(), s);
    let json = serde_json::to_string(&s).unwrap();
    assert_eq!(serde_json::from_str::<S>(&json).unwrap(), s);
}
