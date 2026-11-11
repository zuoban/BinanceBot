use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::fmt::Write;

pub(crate) type HmacSha256 = Hmac<Sha256>;

pub(crate) fn new_hmac_key(secret: &str) -> HmacSha256 {
    HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size")
}

pub(super) fn build_signed_url_fmt(
    base_url: &str,
    hmac_key: &HmacSha256,
    recv_window: u64,
    ts: i64,
    path: &str,
    query: std::fmt::Arguments<'_>,
) -> String {
    let mut url = String::with_capacity(base_url.len() + path.len() + 256);
    url.push_str(base_url);
    url.push_str(path);
    url.push('?');
    let query_start = url.len();
    let _ = url.write_fmt(query);
    if url.len() == query_start {
        let _ = write!(url, "recvWindow={}&timestamp={}", recv_window, ts);
    } else {
        let _ = write!(url, "&recvWindow={}&timestamp={}", recv_window, ts);
    }
    let mut mac = hmac_key.clone();
    mac.update(&url.as_bytes()[query_start..]);
    let digest = mac.finalize().into_bytes();
    let mut hex_buf = [0u8; 64];
    hex::encode_to_slice(digest, &mut hex_buf).expect("SHA-256 digest is 32 bytes");
    url.push_str("&signature=");
    url.push_str(std::str::from_utf8(&hex_buf).expect("hex is valid ASCII"));
    url
}

fn hash_length_prefixed(parts: &[&[u8]]) -> [u8; 32] {
    let mut hash = Sha256::new();
    for value in parts {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    hash.finalize().into()
}

pub(super) fn compute_account_cache_key(
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    recv_window: u64,
) -> [u8; 32] {
    hash_length_prefixed(&[
        base_url.as_bytes(),
        api_key.as_bytes(),
        api_secret.as_bytes(),
        &recv_window.to_be_bytes(),
    ])
}

pub(super) fn identity_key(base_url: &str, api_key: &str) -> [u8; 32] {
    hash_length_prefixed(&[base_url.as_bytes(), api_key.as_bytes()])
}

#[cfg(test)]
mod tests;
