use super::*;

#[tokio::test]
async fn merges_remainders_at_nearest_available_sell_level_and_is_idempotent() {
    let mut engine = engine(database(), None).await;
    seed(&engine, &two_sources()).await;
    engine.maintain_grid_window().await;
    let merged = engine
        .state
        .active_orders
        .read()
        .await
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(engine.state.active_orders.read().await.len(), 1);
    assert_eq!(merged.price, dec!(122.4));
    assert_eq!(merged.quantity, dec!(0.48));
    assert_eq!(merged.amount_usdc, dec!(58.752));
    assert_eq!(merged.purpose, OrderPurpose::Remainder);
    assert_eq!(merged.merge_sources.len(), 2);
    for _ in 0..3 {
        engine.maintain_grid_window().await;
    }
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&merged.client_order_id));
    assert!(engine.load_remainder_plan().await.unwrap().is_none());
}

#[tokio::test]
async fn legacy_partial_grid_and_take_profit_orders_are_not_merge_candidates() {
    let mut engine = engine(database(), None).await;
    let mut legacy = source("legacy", dec!(123.6), dec!(0.24));
    legacy.purpose = OrderPurpose::Legacy;
    let mut partial = source("partial", dec!(123.6), dec!(0.24));
    partial.purpose = OrderPurpose::Grid;
    partial.status = OrderStatus::PartiallyFilled;
    let mut tp = source("tp", dec!(124.38), dec!(24.11));
    tp.purpose = OrderPurpose::TakeProfit;
    tp.is_take_profit = true;
    tp.paired_client_order_id = Some("gb_b_parent".into());
    engine.state.position.write().await.size = dec!(25.07);
    let mut orders = two_sources();
    orders.extend([legacy.clone(), partial.clone(), tp.clone()]);
    seed(&engine, &orders).await;
    engine
        .reconcile_remainder(&[dec!(122.4), dec!(123.6), dec!(124.8), dec!(126)])
        .await
        .unwrap();
    let active = engine.state.active_orders.read().await;
    assert_eq!(active.len(), 4);
    for preserved in [legacy, partial, tp] {
        assert_eq!(
            active[&preserved.client_order_id].quantity,
            preserved.quantity
        );
    }
    assert_eq!(
        active
            .values()
            .find(|o| o.purpose == OrderPurpose::Remainder)
            .unwrap()
            .quantity,
        dec!(0.48)
    );
}

#[tokio::test]
async fn tops_up_one_remainder_after_coalescing_delay_without_creating_another() {
    let mut engine = engine(database(), None).await;
    seed(&engine, &two_sources()).await;
    engine.maintain_grid_window().await;
    engine.state.position.write().await.size = dec!(0.72);
    engine.maintain_grid_window().await;
    assert_eq!(engine.state.active_orders.read().await.len(), 1);
    assert_eq!(
        engine
            .state
            .active_orders
            .read()
            .await
            .values()
            .next()
            .unwrap()
            .quantity,
        dec!(0.48)
    );
    engine.last_remainder_change = Some(Instant::now() - Duration::from_secs(31));
    engine.maintain_grid_window().await;
    let orders = engine.state.active_orders.read().await;
    assert_eq!(orders.len(), 1);
    assert_eq!(orders.values().next().unwrap().quantity, dec!(0.72));
    assert_eq!(orders.values().next().unwrap().price, dec!(122.4));
}

#[tokio::test]
async fn remainder_fill_never_places_full_buy_even_when_configured_amount_matches() {
    let mut engine = engine(database(), None).await;
    let mut order = source("filled", dec!(123.6), dec!(0.48));
    engine.state.config.write().await.grid.order_amount_usdc = dec!(59.328);
    seed(&engine, &[order.clone()]).await;
    assert!(engine.on_order_filled(&mut order).await);
    assert!(engine.state.active_orders.read().await.is_empty());
    assert!(engine
        .state
        .db
        .load_pair_intents("SOLUSDC", TradingMode::Paper)
        .unwrap()
        .is_empty());
    assert_eq!(engine.state.position.read().await.size, Decimal::ZERO);
}

#[tokio::test]
async fn promotes_whole_grid_then_allocates_only_one_final_remainder() {
    let mut engine = engine(database(), None).await;
    let sources = vec![
        source("a", dec!(123.6), dec!(12)),
        source("b", dec!(126), dec!(12)),
    ];
    engine.state.config.write().await.grid.order_amount_usdc = dec!(2000);
    engine.state.position.write().await.size = dec!(24);
    seed(&engine, &sources).await;
    engine.maintain_grid_window().await;
    let promoted = engine
        .state
        .active_orders
        .read()
        .await
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(promoted.purpose, OrderPurpose::Grid);
    assert_eq!(promoted.price, dec!(122.4));
    assert_eq!(promoted.quantity, dec!(16.33));
    engine.maintain_grid_window().await;
    let orders: Vec<_> = engine
        .state
        .active_orders
        .read()
        .await
        .values()
        .cloned()
        .collect();
    assert_eq!(
        orders
            .iter()
            .filter(|o| o.purpose == OrderPurpose::Remainder)
            .count(),
        1
    );
    assert_eq!(reserved_sell_quantity(&orders), dec!(24));
}

#[test]
fn legacy_json_retains_unknown_provenance() {
    let mut value = serde_json::to_value(source("legacy", dec!(126), dec!(0.24))).unwrap();
    value.as_object_mut().unwrap().remove("purpose");
    value.as_object_mut().unwrap().remove("merge_sources");
    let old: GridOrder = serde_json::from_value(value).unwrap();
    assert_eq!(old.purpose, OrderPurpose::Legacy);
    assert!(old.merge_sources.is_empty());
}

#[test]
fn plans_survive_database_reopen_and_are_scoped_and_unique() {
    let directory = crate::test_support::TempTestDir::new("remainder-test");
    let path = directory.join("test.db");
    let mut plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: two_sources(),
        target: source("target", dec!(123.6), dec!(0.48)),
        phase: RemainderPhase::Canceling,
    };
    {
        let db = Database::open(&path).unwrap();
        db.save_remainder_plan(&plan).unwrap();
        let mut conflicting = plan.clone();
        conflicting.target.client_order_id = "gb_s_other_target".into();
        assert!(db.save_remainder_plan(&conflicting).is_err());
    }
    {
        let db = Database::open(&path).unwrap();
        let restored = db
            .load_remainder_plan("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap();
        assert_eq!(restored.sources.len(), 2);
        assert_eq!(restored.target.quantity, dec!(0.48));
        assert_eq!(restored.phase, RemainderPhase::Canceling);
        assert!(db
            .load_remainder_plan("BTCUSDC", TradingMode::Live)
            .unwrap()
            .is_none());
        plan.phase = RemainderPhase::Complete;
        db.save_remainder_plan(&plan).unwrap();
        assert!(db
            .load_remainder_plan("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_none());
    }
}
