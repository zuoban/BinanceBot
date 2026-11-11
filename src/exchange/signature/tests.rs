use super::*;

fn sign_query(secret: &str, query: &str) -> String {
    let mut mac = new_hmac_key(secret);
    mac.update(query.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

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

#[test]
fn signed_url_fmt_matches_expected_hmac_for_empty_and_parameterized_queries() {
    let secret = "NhqPtMDaFdflRRPLUhsc8nvRhgLtDxbiqNob0aQgRUrKIT2MTvmUXOy6AZcDU0RJ";
    let key = new_hmac_key(secret);
    let url = build_signed_url_fmt(
        "https://fapi.binance.com",
        &key,
        5000,
        1499827319559,
        "/fapi/v1/order",
        format_args!(
            "symbol={}&side={}&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1",
            "LTCBTC", "BUY"
        ),
    );
    assert_eq!(
        url,
        "https://fapi.binance.com/fapi/v1/order?symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559&signature=9b97f1a66e9e323ecb322bc4c16241fe01f3afab58cc80b67f9ddc68548944a1"
    );

    let empty = build_signed_url_fmt(
        "https://fapi.binance.com",
        &key,
        5000,
        1499827319559,
        "/fapi/v2/account",
        format_args!(""),
    );
    let expected_sig = sign_query(secret, "recvWindow=5000&timestamp=1499827319559");
    assert_eq!(
        empty,
        format!(
            "https://fapi.binance.com/fapi/v2/account?recvWindow=5000&timestamp=1499827319559&signature={expected_sig}"
        )
    );
}
