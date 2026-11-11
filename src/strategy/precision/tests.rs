use super::*;

#[test]
fn exchange_rules_require_price_and_quantity_filters() {
    let mut info: BinanceExchangeInfo = serde_json::from_value(serde_json::json!({
        "symbols": [{
            "symbol": "SOLUSDC", "status": "TRADING", "pricePrecision": 2,
            "quantityPrecision": 2, "filters": [
                {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "1000", "stepSize": "0.01"}
            ]
        }]
    }))
    .unwrap();
    assert!(SymbolRules::from_exchange_info(&info, "SOLUSDC").is_none());
    info.symbols[0].filters.push(BinanceFilter::PriceFilter {
        min_price: dec!(0.01),
        max_price: dec!(1000),
        tick_size: dec!(0.00001),
    });
    assert_eq!(
        SymbolRules::from_exchange_info(&info, "SOLUSDC")
            .unwrap()
            .tick_size,
        dec!(0.00001)
    );
}

#[test]
fn symbol_rules_require_perpetual_or_omitted_contract_type() {
    let filters = serde_json::json!([
        {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "100000", "tickSize": "0.01"},
        {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "100000", "stepSize": "0.01"}
    ]);
    let info: BinanceExchangeInfo = serde_json::from_value(serde_json::json!({
        "symbols": [
            {"symbol": "SOLUSDC", "status": "TRADING", "contractType": "PERPETUAL", "filters": filters},
            {"symbol": "ETHUSDC", "status": "TRADING", "filters": filters},
            {"symbol": "BTCUSDT_261225", "status": "TRADING", "contractType": "CURRENT_QUARTER", "filters": filters}
        ]
    }))
    .unwrap();
    assert!(SymbolRules::from_trading_exchange_info(&info, "SOLUSDC").is_ok());
    assert!(SymbolRules::from_trading_exchange_info(&info, "ETHUSDC").is_ok());
    assert!(SymbolRules::from_exchange_info(&info, "BTCUSDT_261225").is_none());
    assert!(SymbolRules::from_trading_exchange_info(&info, "BTCUSDT_261225").is_err());
}

#[test]
fn format_price_and_quantity_handle_decimal_max_without_panicking() {
    let direct_max_rules = SymbolRules {
        tick_size: Decimal::ONE,
        price_scale: 4,
        step_size: Decimal::ONE,
        quantity_scale: 4,
        ..SymbolRules::default()
    };
    let expected = format!("{}.0000", Decimal::MAX);
    assert_eq!(direct_max_rules.format_price(Decimal::MAX), expected);
    assert_eq!(direct_max_rules.format_quantity(Decimal::MAX), expected);

    let fractional_rules = SymbolRules {
        tick_size: dec!(0.0001),
        price_scale: 4,
        step_size: dec!(0.0001),
        quantity_scale: 4,
        ..SymbolRules::default()
    };
    assert_eq!(fractional_rules.format_price(Decimal::MAX), "0.0000");
    assert_eq!(fractional_rules.format_quantity(Decimal::MAX), "0.0000");
}

#[test]
fn trading_exchange_rules_distinguish_not_found_not_trading_and_missing_filters() {
    let mut info: BinanceExchangeInfo = serde_json::from_value(serde_json::json!({
        "symbols": [{
            "symbol": "SOLUSDC", "status": "BREAK", "pricePrecision": 2,
            "quantityPrecision": 2, "filters": [
                {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "1000", "stepSize": "0.01"}
            ]
        }]
    }))
    .unwrap();
    assert_eq!(
        SymbolRules::from_trading_exchange_info(&info, "BTCUSDC").unwrap_err(),
        SymbolRulesError::NotFound
    );
    assert_eq!(
        SymbolRules::from_trading_exchange_info(&info, "SOLUSDC").unwrap_err(),
        SymbolRulesError::NotTrading
    );
    info.symbols[0].status = "TRADING".into();
    assert_eq!(
        SymbolRules::from_trading_exchange_info(&info, "SOLUSDC").unwrap_err(),
        SymbolRulesError::MissingFilters
    );
    info.symbols[0].filters.push(BinanceFilter::PriceFilter {
        min_price: dec!(0.01),
        max_price: dec!(1000),
        tick_size: dec!(0.01),
    });
    let rules = SymbolRules::from_trading_exchange_info(&info, "solusdc").unwrap();
    assert_eq!(rules.symbol, "SOLUSDC");
    assert_eq!(rules.tick_size, dec!(0.01));
}

#[test]
fn test_symbol_rules_rounding() {
    let rules = SymbolRules {
        symbol: "SOLUSDC".to_string(),
        tick_size: dec!(0.01),
        price_scale: 2,
        min_price: dec!(0.1),
        max_price: dec!(10000.0),
        step_size: dec!(0.01),
        quantity_scale: 2,
        min_qty: dec!(0.01),
        max_qty: dec!(1000.0),
        min_notional: dec!(5.0),
    };

    assert_eq!(rules.round_price(dec!(115.4849)), dec!(115.48));
    assert_eq!(rules.round_price(dec!(115.486)), dec!(115.49));
    assert_eq!(rules.round_quantity(dec!(0.8674)), dec!(0.86));

    let qty = rules.calculate_quantity(dec!(115.48), dec!(100.0)).unwrap();
    assert_eq!(qty, dec!(0.86));
    assert_eq!(rules.format_price(dec!(115.48)), "115.48");
    assert_eq!(rules.format_quantity(qty), "0.86");
}

#[test]
fn grid_window_uses_fixed_levels_and_excludes_market_price() {
    let rules = SymbolRules::default();
    for market in [dec!(118.81), dec!(118.84), dec!(118.89)] {
        let (buys, sells) =
            rules.grid_window_prices(market, dec!(0.1), (3, 3), (None, None), |_, _| true);
        assert_eq!(buys, vec![dec!(118.8), dec!(118.7), dec!(118.6)]);
        assert_eq!(sells, vec![dec!(118.9), dec!(119.0), dec!(119.1)]);
    }
    let (buys, sells) =
        rules.grid_window_prices(dec!(118.9), dec!(0.1), (2, 2), (None, None), |_, _| true);
    assert_eq!(buys, vec![dec!(118.8), dec!(118.7)]);
    assert_eq!(sells, vec![dec!(119.0), dec!(119.1)]);
    let (buys, sells) =
        rules.grid_window_prices(dec!(118.91), dec!(0.1), (2, 2), (None, None), |_, _| true);
    assert_eq!(buys, vec![dec!(118.9), dec!(118.8)]);
    assert_eq!(sells, vec![dec!(119.0), dec!(119.1)]);
    let (buys, _) =
        rules.grid_window_prices(dec!(0.11), dec!(0.1), (5, 0), (None, None), |_, _| true);
    assert_eq!(buys, vec![dec!(0.1)]);
}

#[test]
fn eligible_window_stops_at_bounds_and_jumps_to_allowed_range() {
    let rules = SymbolRules::default();
    let bounds = (Some(dec!(119.3)), Some(dec!(119.75)));
    let (buys, sells) =
        rules.grid_window_prices(dec!(119.21), dec!(0.1), (3, 3), bounds, |side, price| {
            side != OrderSide::Sell || price == dec!(119.5)
        });
    assert!(buys.is_empty());
    assert_eq!(sells, vec![dec!(119.5)]);
    let (buys, sells) = rules.grid_window_prices(dec!(200), dec!(0.1), (3, 3), bounds, |_, _| true);
    assert_eq!(buys, vec![dec!(119.7), dec!(119.6), dec!(119.5)]);
    assert!(sells.is_empty());
    let (buys, sells) =
        rules.grid_window_prices(dec!(100), dec!(0.1), (3, 3), bounds, |_, _| false);
    assert!(buys.is_empty() && sells.is_empty());
}

#[test]
fn fixed_spacing_must_be_representable_by_price_ticks() {
    let mut rules = SymbolRules::default();
    assert!(rules.validate_grid_interval(dec!(0.1)).is_ok());
    assert!(rules.validate_grid_interval(dec!(0.015)).is_err());
    assert!(rules.validate_grid_interval(dec!(0.001)).is_err());
    assert!(rules.validate_grid_interval(dec!(0)).is_err());
    rules.tick_size = dec!(0.05);
    assert!(rules.validate_grid_interval(dec!(0.1)).is_ok());
    assert!(rules.validate_grid_interval(dec!(0.12)).is_err());
}

#[test]
fn exchange_filters_ignore_invalid_bounds_and_extremes_do_not_overflow() {
    let info: BinanceExchangeInfo = serde_json::from_value(serde_json::json!({
        "symbols": [{
            "symbol": "SOLUSDC", "status": "TRADING", "pricePrecision": 2,
            "quantityPrecision": 2, "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "-1", "maxPrice": "0", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "minQty": "0", "maxQty": "0", "stepSize": "0.01"},
                {"filterType": "MIN_NOTIONAL", "notional": "0"}
            ]
        }]
    }))
    .unwrap();
    let rules = SymbolRules::from_exchange_info(&info, "SOLUSDC").unwrap();
    assert_eq!(rules.min_price, dec!(0.0001));
    assert_eq!(rules.max_price, dec!(1000000.0));
    assert_eq!(rules.min_qty, dec!(0.01));
    assert_eq!(rules.max_qty, dec!(1000000.0));
    assert_eq!(rules.min_notional, dec!(5.0));
    assert_eq!(rules.calculate_quantity(dec!(-100), dec!(100)), None);
    assert_eq!(rules.calculate_quantity(dec!(100), dec!(-100)), None);

    let tiny = Decimal::new(1, 28);
    let (buys, sells) =
        rules.grid_window_prices(dec!(100), tiny, (3, 3), (None, None), |_, _| true);
    assert!(buys.is_empty() && sells.is_empty());

    let grid = crate::config::GridConfig {
        grid_interval: dec!(0.1),
        min_price: Some(dec!(100)),
        max_price: Some(dec!(120)),
        ..crate::config::GridConfig::default()
    };
    assert!(rules.is_valid_maker_grid_price(&grid, OrderSide::Buy, dec!(110.0), dec!(110.1)));
    assert!(!rules.is_valid_maker_grid_price(&grid, OrderSide::Buy, dec!(110.1), dec!(110.1)));
    assert!(rules.is_valid_maker_grid_price(&grid, OrderSide::Sell, dec!(110.2), dec!(110.1)));
    assert!(!rules.is_valid_maker_grid_price(&grid, OrderSide::Sell, dec!(110.25), dec!(110.1)));
    assert!(!rules.is_valid_maker_grid_price(&grid, OrderSide::Buy, dec!(99.9), dec!(110.1)));
    assert!(rules.validate_grid_interval(Decimal::MAX).is_err());
    assert!(!rules.is_grid_price(Decimal::MAX, tiny));
    assert_eq!(rules.round_price(Decimal::MAX), Decimal::ZERO);
    assert_eq!(rules.round_quantity(Decimal::MAX), Decimal::ZERO);
    assert_eq!(rules.calculate_quantity(tiny, Decimal::MAX), None);
    assert_eq!(
        rules.plan_remainder_quantity(dec!(100), dec!(100), Decimal::MAX),
        None
    );
}
