use super::*;
use rust_decimal_macros::dec;

fn params(grid: impl FnOnce(&mut GridConfig)) -> BacktestParams {
    let mut config = GridConfig {
        grid_interval: dec!(1),
        order_amount_usdc: dec!(100),
        buy_window: 3,
        sell_window: 3,
        max_position_usdc: None,
        ..GridConfig::default()
    };
    grid(&mut config);
    BacktestParams {
        grid: config,
        rules: SymbolRules::default(),
        maker_fee_rate: dec!(0.0002),
        funding_rate_8h: Decimal::ZERO,
        capital: dec!(1000),
        exit_floor_spacings: None,
    }
}

fn ticks(prices: &[Decimal]) -> Vec<Candle> {
    prices
        .iter()
        .enumerate()
        .map(|(i, price)| Candle::tick(i as i64 * 60_000, *price))
        .collect()
}

#[test]
fn reaching_the_cap_pauses_before_a_closing_fill() {
    let mut p = params(|g| {
        g.buy_window = 1;
        g.sell_window = 1;
        g.min_price = Some(dec!(99));
        g.max_position_usdc = Some(dec!(100));
        g.pause_on_max_position = true;
    });
    p.rules.step_size = Decimal::ONE;
    p.rules.min_qty = Decimal::ONE;
    let report = run(&p, &ticks(&[dec!(99.5), dec!(99), dec!(100)])).unwrap();

    assert_eq!((report.buys, report.sells), (1, 0));
    assert_eq!(report.final_position, Decimal::ONE);
    assert_eq!(report.realized_pnl, Decimal::ZERO);
    assert_eq!(report.paused_at_ms, Some(120_000));
    assert_eq!(report.peak_position_notional, dec!(100));
}

#[test]
fn regular_closing_allocation_preserves_the_remainder_queue() {
    let p = params(|g| g.grid_interval = dec!(10));
    let mut sim = Sim::new(&p, dec!(20), 0);
    sim.maintain();
    sim.move_to(dec!(10));
    assert!(sim.orders.iter().any(|o| {
        o.side == OrderSide::Sell && o.price == dec!(40) && o.qty == dec!(1.67) && o.remainder
    }));

    sim.move_to(dec!(30));
    for _ in 0..3 {
        sim.maintain();
        let exits: Vec<_> = sim
            .orders
            .iter()
            .filter(|o| o.side == OrderSide::Sell)
            .collect();
        assert_eq!(exits.len(), 1);
        assert_eq!((exits[0].price, exits[0].qty), (dec!(40), dec!(1.67)));
        assert!(exits[0].remainder);
    }

    let report = run(&p, &ticks(&[dec!(20), dec!(10), dec!(30), dec!(40)])).unwrap();
    assert_eq!((report.buys, report.sells), (1, 3));
    assert_eq!(report.final_position, Decimal::ZERO);
    assert_eq!(report.realized_pnl, dec!(166.7));
}

#[test]
fn remainder_requires_a_valid_ordinary_quantity_at_its_price() {
    let mut p = params(|g| {
        g.buy_window = 1;
        g.sell_window = 1;
    });
    p.rules.step_size = Decimal::ONE;
    p.rules.min_qty = Decimal::ONE;
    let report = run(&p, &ticks(&[dec!(101), dec!(100), dec!(101)])).unwrap();

    assert_eq!((report.buys, report.sells), (1, 0));
    assert_eq!(report.final_position, Decimal::ONE);
    assert_eq!(report.realized_pnl, Decimal::ZERO);
}

#[test]
fn remainder_top_up_is_capped_and_promoted_to_an_ordinary_order() {
    let p = params(|g| {
        g.grid_interval = dec!(10);
        g.buy_window = 1;
        g.sell_window = 1;
    });
    let mut sim = Sim::new(&p, dec!(30), 0);
    sim.size = dec!(3);
    sim.entry = dec!(10);
    sim.orders.push(BtOrder {
        side: OrderSide::Sell,
        price: dec!(40),
        qty: dec!(1.67),
        remainder: true,
    });
    sim.maintain();

    let exits: Vec<_> = sim
        .orders
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .collect();
    assert_eq!(exits.len(), 1);
    assert_eq!((exits[0].price, exits[0].qty), (dec!(40), dec!(2.5)));
    assert!(!exits[0].remainder);
    sim.move_to(dec!(40));
    assert_eq!(sim.size, dec!(0.5));
    assert_eq!(sim.sells, 1);
}

#[test]
fn replay_rejects_invalid_caps_and_price_bounds_in_restrict_only_mode() {
    let candles = ticks(&[dec!(100)]);
    for cap in [Decimal::ZERO, -Decimal::ONE] {
        let p = params(|g| g.max_position_usdc = Some(cap));
        assert!(run(&p, &candles).is_err());
    }
    for (minimum, maximum) in [
        (Some(dec!(110)), Some(dec!(90))),
        (Some(Decimal::ZERO), None),
        (Some(-Decimal::ONE), None),
        (None, Some(Decimal::ZERO)),
        (None, Some(-Decimal::ONE)),
    ] {
        let p = params(|g| {
            g.min_price = minimum;
            g.max_price = maximum;
        });
        assert!(run(&p, &candles).is_err());
    }
}

#[test]
fn permissive_custom_filters_cannot_create_zero_quantity_fills() {
    let mut p = params(|g| {
        g.buy_window = 1;
        g.sell_window = 1;
    });
    p.rules.step_size = Decimal::ONE;
    p.rules.min_qty = Decimal::ZERO;
    p.rules.min_notional = Decimal::ZERO;
    let report = run(&p, &ticks(&[dec!(102), dec!(101), dec!(102)])).unwrap();

    assert_eq!(report.fills, 0);
    assert_eq!(report.final_position, Decimal::ZERO);
    assert_eq!(report.fees, Decimal::ZERO);
}

#[test]
fn closing_fills_preserve_pre_fill_peak_notional_and_equity() {
    let mut p = params(|g| {
        g.buy_window = 1;
        g.sell_window = 1;
    });
    p.rules.step_size = Decimal::ONE;
    p.rules.min_qty = Decimal::ONE;
    p.maker_fee_rate = Decimal::ZERO;
    let candles = ticks(&[dec!(100), dec!(99), dec!(100)]);
    let report = run(&p, &candles).unwrap();

    assert_eq!(report.final_position, Decimal::ZERO);
    assert_eq!(report.peak_position_notional, dec!(100));
    assert_eq!(report.net_total, Decimal::ONE);

    // Before paying the closing fee, marked equity peaks at 0.01. The next
    // fee lowers it to -0.99, a drawdown of 1 rather than 0.99.
    p.maker_fee_rate = dec!(0.01);
    let with_fees = run(&p, &candles).unwrap();
    assert_eq!(with_fees.max_drawdown, Decimal::ONE);
    assert_eq!(with_fees.net_total, dec!(-0.99));

    p.maker_fee_rate = dec!(-0.0002);
    assert!(run(&p, &candles).is_err());
}

#[test]
fn one_round_trip_earns_one_spacing_minus_fees() {
    let report = run(
        &params(|_| {}),
        &ticks(&[dec!(100), dec!(98.5), dec!(100.5)]),
    )
    .unwrap();
    // Buy 1.01 @ 99, sell 1.00 @ 100 (quantity rounds per price).
    assert_eq!((report.buys, report.sells), (1, 1));
    assert_eq!(report.realized_pnl, dec!(1));
    // 1.01 * 99 + 1.00 * 100 = 199.99 notional at 0.02%.
    assert_eq!(report.fees, dec!(0.0400));
    assert_eq!(report.final_position, dec!(0.01));
}

#[test]
fn a_fast_move_keeps_filling_through_new_levels() {
    // Window is 3 levels deep, yet a drop of 6 levels must fill more than 3 buys because
    // the window is re-maintained after every fill.
    let report = run(&params(|_| {}), &ticks(&[dec!(100), dec!(93.5)])).unwrap();
    assert_eq!(report.buys, 6);
    assert_eq!(report.sells, 0);
}

#[test]
fn pause_at_the_cap_cancels_everything() {
    let report = run(
        &params(|g| {
            g.max_position_usdc = Some(dec!(150));
            g.pause_on_max_position = true;
        }),
        &ticks(&[dec!(100), dec!(95)]),
    )
    .unwrap();
    assert_eq!(report.buys, 1);
    assert!(report.paused_at_ms.is_some());
}

#[test]
fn cancel_opening_at_the_cap_keeps_exits_and_resumes() {
    let p = params(|g| {
        g.max_position_usdc = Some(dec!(150));
        g.cancel_opening_on_max_position = true;
    });
    let mut sim = Sim::new(&p, dec!(100), 0);
    sim.maintain();
    assert!(sim.orders.iter().any(|o| o.side == OrderSide::Buy));

    // Position value 200 is above the 150 cap: openings are cancelled, exits stay.
    sim.size = dec!(2);
    sim.entry = dec!(100);
    sim.maintain();
    assert!(sim.paused.is_none());
    assert!(sim.orders.iter().all(|o| o.side == OrderSide::Sell));
    assert!(!sim.orders.is_empty());

    // Back under the cap with room for another order: openings return.
    sim.size = dec!(0.5);
    sim.maintain();
    assert!(sim.orders.iter().any(|o| o.side == OrderSide::Buy));
}

#[test]
fn short_grid_profits_when_price_falls_back() {
    let report = run(
        &params(|g| g.direction = GridDirection::Short),
        &ticks(&[dec!(100), dec!(101.5), dec!(99.5)]),
    )
    .unwrap();
    assert_eq!((report.sells, report.buys), (1, 1));
    assert!(report.realized_pnl > Decimal::ZERO);
}

#[test]
fn funding_is_charged_to_longs_each_eight_hours() {
    let mut p = params(|_| {});
    p.funding_rate_8h = dec!(0.001);
    let eight_hours = FUNDING_PERIOD_MS;
    let candles = vec![
        Candle::tick(eight_hours - 60_000, dec!(100)),
        Candle::tick(eight_hours - 30_000, dec!(98.5)),
        Candle::tick(eight_hours + 60_000, dec!(98.5)),
    ];
    let report = run(&p, &candles).unwrap();
    // Long 1.01 @ 99 held across one boundary: 1.01 * 98.5 * 0.001.
    assert_eq!(report.funding, dec!(0.0995));
}

#[test]
fn csv_accepts_ticks_candles_and_a_header() {
    let ticks = parse_csv("time,price\n1000,100.5\n2000,101\n").unwrap();
    assert_eq!(
        ticks,
        vec![
            Candle::tick(1000, dec!(100.5)),
            Candle::tick(2000, dec!(101))
        ]
    );
    let candles = parse_csv("1000,100,102,99,101,12.5,x\n").unwrap();
    assert_eq!(
        candles,
        vec![Candle {
            time_ms: 1000,
            open: dec!(100),
            high: dec!(102),
            low: dec!(99),
            close: dec!(101)
        }]
    );
    assert!(parse_csv("1000,100,101\n").is_err());
}

#[test]
fn average_cost_accounting_matches_cash_flow_on_a_noisy_path() {
    for direction in [GridDirection::Long, GridDirection::Short] {
        let p = params(|g| {
            g.direction = direction;
            g.grid_interval = dec!(0.5);
        });
        let mut sim = Sim::new(&p, dec!(100), 0);
        sim.maintain();
        let mut seed: u64 = 12345;
        let mut price = dec!(100);
        for _ in 0..3000 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let step = Decimal::from((seed >> 33) % 21) / dec!(10) - dec!(1);
            price = (price + step).max(dec!(60));
            sim.move_to(price);
        }
        assert!(sim.fills > 100);
        let gross = sim.realized + sim.unrealized();
        // The weighted-average entry divides, so allow Decimal rounding dust.
        let expected = sim.cash_flow + sim.size * sim.market;
        assert!(
            (gross - expected).abs() < dec!(0.000000001),
            "{gross} vs {expected}"
        );
    }
}

#[test]
fn sweep_runs_every_combination_over_the_same_candles_best_first() {
    let candles = ticks(&[dec!(100), dec!(97), dec!(103), dec!(97), dec!(103)]);
    let base = params(|_| {});
    let intervals = [dec!(1), dec!(2), dec!(0.5)];
    let rows = sweep(
        &base,
        &intervals,
        &[dec!(100), dec!(200)],
        &[3],
        &[3, 5],
        &candles,
    )
    .unwrap();
    assert_eq!(rows.len(), 3 * 2 * 2);
    assert!(rows
        .windows(2)
        .all(|w| w[0].report.net_total >= w[1].report.net_total));
    // Each row matches a standalone replay of the same parameters.
    for row in &rows {
        let mut single = base.clone();
        single.grid.grid_interval = row.grid_interval;
        single.grid.order_amount_usdc = row.order_amount_usdc;
        single.grid.buy_window = row.buy_window;
        single.grid.sell_window = row.sell_window;
        let alone = run(&single, &candles).unwrap();
        assert_eq!(alone.net_total, row.report.net_total);
        assert_eq!(alone.fills, row.report.fills);
    }
    // Spacing changes the result, so the sweep is not just repeating one run.
    assert!(rows.iter().any(|r| r.report.fills != rows[0].report.fills));
}

#[test]
fn sweep_fails_on_any_invalid_combination_and_caps_its_size() {
    let candles = ticks(&[dec!(100), dec!(98)]);
    let base = params(|_| {});
    // 0.005 is below the 0.01 tick: the whole sweep must fail, not skip it.
    assert!(sweep(
        &base,
        &[dec!(1), dec!(0.005)],
        &[dec!(100)],
        &[3],
        &[3],
        &candles
    )
    .is_err());
    assert!(sweep(&base, &[], &[dec!(100)], &[3], &[3], &candles).is_err());
    let many: Vec<usize> = (1..=21).collect();
    assert!(sweep(&base, &[dec!(1)], &[dec!(100)], &many, &many, &candles).is_err());
    assert!(checked_sweep_combinations(usize::MAX, 2, 1, 1).is_err());
    assert!(checked_sweep_combinations(usize::MAX, 2, 0, 1).is_err());
}

#[test]
fn exit_floor_blocks_exits_below_average_entry_plus_the_margin() {
    let sells = |floor: Option<Decimal>, direction: GridDirection| {
        let mut p = params(|g| g.direction = direction);
        p.exit_floor_spacings = floor;
        let mut sim = Sim::new(&p, dec!(100), 0);
        // Long 2 at 103 or short 2 at 97, market back at 100.
        sim.size = if direction == GridDirection::Long {
            dec!(2)
        } else {
            dec!(-2)
        };
        sim.entry = if direction == GridDirection::Long {
            dec!(103)
        } else {
            dec!(97)
        };
        sim.maintain();
        let side = direction.closing_side();
        let mut exits: Vec<Decimal> = sim
            .orders
            .iter()
            .filter(|o| o.side == side)
            .map(|o| o.price)
            .collect();
        exits.sort();
        exits
    };
    // Without a floor the long exits sit at 101..103 although entry was 103.
    assert!(sells(None, GridDirection::Long)
        .iter()
        .any(|p| *p < dec!(103)));
    // Floor of 1 spacing: nothing below 104 survives, and the market is below it.
    assert!(sells(Some(dec!(1)), GridDirection::Long)
        .iter()
        .all(|p| *p >= dec!(104)));
    assert!(sells(Some(dec!(0)), GridDirection::Long)
        .iter()
        .all(|p| *p >= dec!(103)));
    // Short mirror: entry 97, floor 1 spacing means exits at or below 96.
    assert!(sells(None, GridDirection::Short)
        .iter()
        .any(|p| *p > dec!(97)));
    assert!(sells(Some(dec!(1)), GridDirection::Short)
        .iter()
        .all(|p| *p <= dec!(96)));
}

#[test]
fn exit_floor_never_realizes_a_loss_on_a_whipsaw() {
    // Long-only grid over a down-then-up path: with a zero floor every exit is at or above
    // the average entry, so realized PnL cannot be negative.
    let mut path = Vec::new();
    for i in 0..40 {
        path.push(dec!(100) - Decimal::from(i) / dec!(4));
    }
    for i in 0..40 {
        path.push(dec!(90) + Decimal::from(i) / dec!(8));
    }
    let mut p = params(|g| g.max_position_usdc = Some(dec!(800)));
    p.maker_fee_rate = Decimal::ZERO;
    let plain = run(&p, &ticks(&path)).unwrap();
    p.exit_floor_spacings = Some(Decimal::ZERO);
    let floored = run(&p, &ticks(&path)).unwrap();
    assert!(
        plain.realized_pnl < Decimal::ZERO,
        "setup should lose without a floor"
    );
    assert!(floored.realized_pnl >= Decimal::ZERO);
}

#[test]
fn prices_outside_the_rules_price_range_fail_loudly_instead_of_trading_nothing() {
    let mut p = params(|_| {});
    p.rules.max_price = dec!(100000);
    let error = run(&p, &ticks(&[dec!(118000), dec!(118100)])).unwrap_err();
    assert!(error.to_string().contains("价格范围"));
    p.rules.max_price = dec!(1000000);
    assert!(run(&p, &ticks(&[dec!(118000), dec!(118100)])).is_ok());
}

#[test]
fn invalid_input_is_rejected() {
    let good = ticks(&[dec!(100)]);
    assert!(run(&params(|_| {}), &[]).is_err());
    assert!(run(&params(|g| g.grid_interval = dec!(0.005)), &good).is_err());
    assert!(run(&params(|g| g.pause_on_max_position = true), &good).is_err());
    let mut negative_fee = params(|_| {});
    negative_fee.maker_fee_rate = dec!(-0.0001);
    assert!(run(&negative_fee, &good).is_err());
    let backwards = vec![Candle::tick(2, dec!(100)), Candle::tick(1, dec!(100))];
    assert!(run(&params(|_| {}), &backwards).is_err());

    // When a fill in the opposite direction crosses zero, entry resets to fill price.
    let p = params(|g| g.direction = GridDirection::Short);
    let mut sim = Sim::new(&p, dec!(100), 0);
    sim.size = dec!(1);
    sim.entry = dec!(95);
    sim.orders.push(BtOrder {
        side: OrderSide::Sell,
        price: dec!(105),
        qty: dec!(2),
        remainder: false,
    });
    sim.fill_at(OrderSide::Sell, dec!(105));
    assert_eq!(sim.size, dec!(-1));
    assert_eq!(sim.entry, dec!(105));
}

#[tokio::test]
async fn cli_rules_reject_invalid_fee_and_filter_overrides() {
    let base = BacktestArgs {
        symbol: None,
        csv: None,
        interval: "1m".into(),
        days: 7,
        direction: DirectionArg::Long,
        grid_interval: vec![dec!(1)],
        amount: vec![dec!(100)],
        buy_window: vec![3],
        sell_window: vec![3],
        min_price: None,
        max_price: None,
        max_position: None,
        pause_on_max_position: false,
        cancel_opening_on_max_position: false,
        maker_fee: dec!(0.0002),
        funding_rate: Decimal::ZERO,
        capital: dec!(1000),
        tick_size: None,
        step_size: None,
        min_qty: None,
        min_notional: None,
        exit_floor: None,
        json: false,
    };
    assert!(load_rules(&BacktestArgs {
        maker_fee: dec!(-0.0001),
        ..base.clone()
    })
    .await
    .is_err());
    assert!(load_rules(&BacktestArgs {
        min_qty: Some(Decimal::ZERO),
        ..base.clone()
    })
    .await
    .is_err());
    assert!(load_rules(&BacktestArgs {
        min_notional: Some(Decimal::ZERO),
        ..base.clone()
    })
    .await
    .is_err());
    assert!(load_rules(&BacktestArgs {
        symbol: Some("solusdc".into()),
        ..base.clone()
    })
    .await
    .is_err());
    assert!(fetch_klines("SOLUSDC&limit=1", "1m", 0, 60_000)
        .await
        .is_err());
    assert!(fetch_klines("SOLUSDC", "2m", 0, 60_000).await.is_err());
    assert!(fetch_klines("SOLUSDC", "1m", 60_000, 60_000).await.is_err());
    assert!(run_cli(&BacktestArgs {
        symbol: Some("SOLUSDC".into()),
        days: 0,
        ..base.clone()
    })
    .await
    .is_err());
    assert!(run_cli(&BacktestArgs {
        symbol: Some("SOLUSDC".into()),
        days: MAX_BACKTEST_DAYS + 1,
        ..base.clone()
    })
    .await
    .is_err());
    assert!(run_cli(&BacktestArgs {
        symbol: Some("SOLUSDC".into()),
        interval: "invalid".into(),
        ..base
    })
    .await
    .is_err());
}
