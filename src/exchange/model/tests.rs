use super::{
    BinanceAccountInfoResponse, BinanceExchangeInfo, BinanceMarkPrice, BinanceOrderResponse,
    BinancePositionRisk, BinanceUserTrade,
};
use rust_decimal_macros::dec;

#[test]
fn premium_index_uses_mark_price_field() {
    let response = r#"{"symbol":"SOLUSDC","markPrice":"114.56873000","indexPrice":"114.57000000"}"#;
    let mark: BinanceMarkPrice = serde_json::from_str(response).unwrap();
    assert_eq!(mark.mark_price, dec!(114.56873000));
}

#[test]
fn symbol_info_defaults_contract_type_when_omitted() {
    let info: BinanceExchangeInfo = serde_json::from_str(
        r#"{"symbols":[
            {"symbol":"SOLUSDC","status":"TRADING","filters":[]},
            {"symbol":"BTCUSDT_261225","status":"TRADING","contractType":"CURRENT_QUARTER","filters":[]}
        ]}"#,
    )
    .unwrap();
    assert_eq!(info.symbols[0].contract_type, "");
    assert_eq!(info.symbols[1].contract_type, "CURRENT_QUARTER");
}

#[test]
fn user_trade_reads_exchange_realized_pnl_and_fee() {
    let response =
        r#"{"realizedPnl":"-3.25","commission":"0.80","commissionAsset":"USDC","maker":true}"#;
    let trade: BinanceUserTrade = serde_json::from_str(response).unwrap();
    assert_eq!(trade.realized_pnl, dec!(-3.25));
    assert_eq!(trade.commission, dec!(0.80));
    assert_eq!(trade.commission_asset, "USDC");
}

#[test]
fn usdc_account_balance_comes_from_matching_asset() {
    let response = r#"{
        "totalWalletBalance":"0.00000000",
        "totalMarginBalance":"0.00000000",
        "totalUnrealizedProfit":"0.00000000",
        "availableBalance":"0.00000000",
        "assets":[{"asset":"USDT","walletBalance":"0","marginBalance":"0","availableBalance":"0","unrealizedProfit":"0"},
                  {"asset":"USDC","walletBalance":"10091.02762121","marginBalance":"9852.87142345","availableBalance":"6358.01937745","unrealizedProfit":"-238.15619776"}],
        "positions":[{"symbol":"SOLUSDC","positionAmt":"20.00"}]
    }"#;
    let account: BinanceAccountInfoResponse = serde_json::from_str(response).unwrap();
    let asset = account.margin_asset_for_symbol("SOLUSDC").unwrap();
    assert_eq!(asset.asset, "USDC");
    assert_eq!(asset.margin_balance, dec!(9852.87142345));
    assert_eq!(asset.available_balance, dec!(6358.01937745));
}

#[test]
fn order_status_and_position_risk_helpers_classify_responses() {
    assert!(BinanceOrderResponse::is_terminal_status("FILLED"));
    assert!(!BinanceOrderResponse::is_unfilled_terminal_status("FILLED"));
    assert!(BinanceOrderResponse::is_unfilled_terminal_status(
        "CANCELED"
    ));
    assert!(BinanceOrderResponse::is_known_status("PARTIALLY_FILLED"));
    assert!(!BinanceOrderResponse::is_known_status("PENDING_CANCEL"));

    let mut order = BinanceOrderResponse {
        order_id: 1,
        client_order_id: "gb_b_1".into(),
        symbol: "SOLUSDC".into(),
        status: "PARTIALLY_FILLED".into(),
        price: dec!(100),
        avg_price: None,
        orig_qty: dec!(2.5),
        executed_qty: dec!(1.0),
        side: "BUY".into(),
        order_type: "LIMIT".into(),
        time_in_force: "GTX".into(),
        update_time: None,
    };
    assert_eq!(order.remaining_qty(), dec!(1.5));
    assert!(order.has_valid_symbol_and_side("SOLUSDC"));
    assert!(!order.has_valid_symbol_and_side("BTCUSDT"));
    order.side = "INVALID".into();
    assert!(!order.has_valid_symbol_and_side("SOLUSDC"));
    order.executed_qty = dec!(3.0);
    assert_eq!(order.remaining_qty(), dec!(0));
    let risk = BinancePositionRisk {
        symbol: "SOLUSDC".into(),
        position_side: None,
        position_amt: dec!(3.5),
        entry_price: dec!(120),
        mark_price: dec!(125),
        un_realized_profit: dec!(17.5),
        liquidation_price: dec!(80),
        leverage: "15".into(),
    };
    let pos = risk.clone().into_position_info(None);
    assert_eq!(pos.symbol, "SOLUSDC");
    assert_eq!(pos.size, dec!(3.5));
    assert_eq!(pos.leverage, 15);
    for invalid in ["0", "", "invalid"] {
        let mut fallback = risk.clone();
        fallback.leverage = invalid.into();
        assert_eq!(fallback.into_position_info(None).leverage, 1);
    }
}
