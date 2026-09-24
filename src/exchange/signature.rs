use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_query(secret: &str, query: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(query.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature() {
        let secret = "NhqPtMDaFdflRRPLUhsc8nvRhgLtDxbiqNob0aQgRUrKIT2MTvmUXOy6AZcDU0RJ";
        let query = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        let signature = sign_query(secret, query);
        assert_eq!(
            signature,
            "9b97f1a66e9e323ecb322bc4c16241fe01f3afab58cc80b67f9ddc68548944a1"
        );
    }
}
