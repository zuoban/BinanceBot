use super::*;

#[derive(serde::Deserialize)]
struct SymbolMarketResponse<T> {
    symbol: String,
    #[serde(flatten)]
    data: T,
}

pub(crate) struct NewOrderRequest<'a> {
    pub(crate) symbol: &'a str,
    pub(crate) side: &'a str,
    pub(crate) price: &'a str,
    pub(crate) quantity: &'a str,
    pub(crate) client_order_id: &'a str,
    pub(crate) post_only: bool,
    pub(crate) reduce_only: bool,
}

#[derive(serde::Serialize)]
struct BatchOrderWireItem<'a> {
    symbol: &'a str,
    side: &'a str,
    #[serde(rename = "type")]
    order_type: &'static str,
    #[serde(rename = "timeInForce")]
    time_in_force: &'static str,
    price: &'a str,
    quantity: &'a str,
    #[serde(rename = "newClientOrderId")]
    new_client_order_id: &'a str,
    #[serde(rename = "reduceOnly", skip_serializing_if = "Option::is_none")]
    reduce_only: Option<&'static str>,
}

fn is_valid_listen_key(listen_key: &str) -> bool {
    !listen_key.is_empty()
        && listen_key.len() <= 128
        && listen_key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}

fn percent_encode_query_value(input: &str) -> String {
    use std::fmt::Write;
    let mut encoded = String::with_capacity(input.len() * 2);
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            let _ = write!(encoded, "%{:02X}", byte);
        }
    }
    encoded
}

impl BinanceFuturesClient {
    pub(crate) async fn get_all_mark_prices_for_fallback(
        &self,
    ) -> Result<Vec<BinanceMarkPriceWithSymbol>> {
        self.get_public_json(
            10,
            RequestClass::MarketFallback,
            "/fapi/v1/premiumIndex",
            &[],
        )
        .await
    }

    /// Fetch exchange info (symbol filters)
    pub(crate) async fn get_exchange_info(
        &self,
        symbol: Option<&str>,
    ) -> Result<BinanceExchangeInfo> {
        let query: &[(&str, &str)] = match symbol {
            Some(sym) => &[("symbol", sym)],
            None => &[],
        };
        let info: BinanceExchangeInfo = self
            .get_public_json(1, RequestClass::Normal, "/fapi/v1/exchangeInfo", query)
            .await?;
        self.limiter
            .update_exchange_limits(&self.base_url, &info.rate_limits);
        Ok(info)
    }

    async fn get_symbol_public<T: serde::de::DeserializeOwned>(
        &self,
        class: RequestClass,
        path: &str,
        symbol: &str,
    ) -> Result<T> {
        let response: SymbolMarketResponse<T> = self
            .get_public_json(1, class, path, &[("symbol", symbol)])
            .await?;
        anyhow::ensure!(
            response.symbol.eq_ignore_ascii_case(symbol),
            "Binance market response symbol does not match requested symbol {symbol}"
        );
        Ok(response.data)
    }

    /// Fetch the latest traded price.
    pub(crate) async fn get_ticker_price(&self, symbol: &str) -> Result<Decimal> {
        let ticker: BinanceTickerPrice = self
            .get_symbol_public(RequestClass::Normal, "/fapi/v1/ticker/price", symbol)
            .await?;
        Ok(ticker.price)
    }

    pub(crate) async fn get_ticker_price_for_fallback(&self, symbol: &str) -> Result<Decimal> {
        let ticker: BinanceTickerPrice = self
            .get_symbol_public(
                RequestClass::MarketFallback,
                "/fapi/v1/ticker/price",
                symbol,
            )
            .await?;
        Ok(ticker.price)
    }

    /// Fetch the futures mark price, which differs from the latest traded price.
    pub(crate) async fn get_mark_price(&self, symbol: &str) -> Result<Decimal> {
        let mark: BinanceMarkPrice = self
            .get_symbol_public(RequestClass::Normal, "/fapi/v1/premiumIndex", symbol)
            .await?;
        Ok(mark.mark_price)
    }

    pub(crate) async fn get_mark_price_for_fallback(&self, symbol: &str) -> Result<Decimal> {
        let mark: BinanceMarkPrice = self
            .get_symbol_public(
                RequestClass::MarketFallback,
                "/fapi/v1/premiumIndex",
                symbol,
            )
            .await?;
        Ok(mark.mark_price)
    }

    /// Fetch 24-hour ticker statistics
    pub(crate) async fn get_24hr_ticker(&self, symbol: &str) -> Result<Binance24hrTicker> {
        self.get_symbol_public(RequestClass::Normal, "/fapi/v1/ticker/24hr", symbol)
            .await
    }

    pub(crate) async fn get_24hr_ticker_for_fallback(
        &self,
        symbol: &str,
    ) -> Result<Binance24hrTicker> {
        self.get_symbol_public(RequestClass::MarketFallback, "/fapi/v1/ticker/24hr", symbol)
            .await
    }

    /// Fetch open orders for symbol
    pub(crate) async fn get_open_orders(&self, symbol: &str) -> Result<Vec<BinanceOrderResponse>> {
        self.get_open_orders_with_class(symbol, RequestClass::Critical)
            .await
    }

    pub(crate) async fn get_open_orders_for_periodic_sync(
        &self,
        symbol: &str,
    ) -> Result<Vec<BinanceOrderResponse>> {
        self.get_open_orders_with_class(symbol, RequestClass::Normal)
            .await
    }

    async fn get_open_orders_with_class(
        &self,
        symbol: &str,
        class: RequestClass,
    ) -> Result<Vec<BinanceOrderResponse>> {
        self.reserve(1, class)?;
        let url = self.signed_url_fmt("/fapi/v1/openOrders", format_args!("symbol={}", symbol));

        let resp = self
            .checked_signed_response(self.send(self.client.get(&url)).await?)
            .await?;

        read_bounded_json(resp, 8 * 1024 * 1024).await
    }

    /// Fetch an order's final status before treating it as a fill.
    pub(crate) async fn get_order(
        &self,
        symbol: &str,
        order_id: i64,
    ) -> Result<BinanceOrderResponse> {
        let url = self.prepare_signed_get_url(
            1,
            RequestClass::Critical,
            "/fapi/v1/order",
            format_args!("symbol={}&orderId={}", symbol, order_id),
        )?;
        self.get_signed_json_url(&url, "Failed to get order").await
    }

    /// Resolve a persisted client order ID after a restart.
    pub(crate) async fn get_order_by_client_id(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<BinanceOrderResponse> {
        self.require_order_by_client_id(symbol, client_order_id, RequestClass::Critical)
            .await
    }

    /// Restore a historical order ID without consuming emergency request capacity.
    pub(crate) async fn get_order_by_client_id_for_pnl(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<BinanceOrderResponse> {
        self.require_order_by_client_id(symbol, client_order_id, RequestClass::PnlReconciliation)
            .await
    }

    async fn require_order_by_client_id(
        &self,
        symbol: &str,
        client_order_id: &str,
        class: RequestClass,
    ) -> Result<BinanceOrderResponse> {
        self.lookup_order_by_client_id_with_class(symbol, client_order_id, class)
            .await?
            .ok_or_else(|| anyhow!("Order {} was not found on Binance", client_order_id))
    }

    /// Only Binance's explicit unknown-order code means it is safe to submit this ID.
    pub(crate) async fn lookup_order_by_client_id(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Option<BinanceOrderResponse>> {
        self.lookup_order_by_client_id_with_class(symbol, client_order_id, RequestClass::Critical)
            .await
    }

    async fn lookup_order_by_client_id_with_class(
        &self,
        symbol: &str,
        client_order_id: &str,
        class: RequestClass,
    ) -> Result<Option<BinanceOrderResponse>> {
        anyhow::ensure!(
            is_valid_client_order_id(client_order_id),
            "Invalid client_order_id"
        );
        self.reserve(1, class)?;
        let url = self.signed_url_fmt(
            "/fapi/v1/order",
            format_args!("symbol={}&origClientOrderId={}", symbol, client_order_id),
        );
        let resp = self.send(self.client.get(&url)).await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_bounded_error_body(resp).await?;
            if status == reqwest::StatusCode::BAD_REQUEST
                && serde_json::from_str::<BinanceApiError>(&body)
                    .is_ok_and(|error| error.code == -2013)
            {
                return Ok(None);
            }
            let context = format!("Failed to resolve order {client_order_id}");
            return Err(self
                .signed_error_from_body(status, &body, Some(&context))
                .await);
        }
        Ok(Some(read_bounded_json(resp, 64 * 1024).await?))
    }

    /// Place an order. If post_only is true, timeInForce is set to GTX (Maker Only).
    /// reduce_only prevents a closing order from opening the opposite position.
    pub(crate) async fn place_order(
        &self,
        order: NewOrderRequest<'_>,
    ) -> std::result::Result<BinanceOrderResponse, ExchangeError> {
        if !is_valid_client_order_id(order.client_order_id) {
            return Err(ExchangeError::RequestRejected(
                "Invalid client_order_id".to_string(),
            ));
        }
        self.reserve(
            0,
            if order.reduce_only {
                RequestClass::ClosingOrder
            } else {
                RequestClass::OpeningOrder
            },
        )
        .map_err(|error| ExchangeError::RequestRejected(error.to_string()))?;
        let tif = if order.post_only { "GTX" } else { "GTC" };
        let reduce_only_param = if order.reduce_only {
            "&reduceOnly=true"
        } else {
            ""
        };
        let url = self.signed_url_fmt(
            "/fapi/v1/order",
            format_args!(
                "symbol={}&side={}&type=LIMIT&timeInForce={}&price={}&quantity={}&newClientOrderId={}{}",
                order.symbol,
                order.side,
                tif,
                order.price,
                order.quantity,
                order.client_order_id,
                reduce_only_param
            ),
        );

        debug!(
            "Sending Binance order request: side={}, price={}, qty={}, tif={}",
            order.side, order.price, order.quantity, tif
        );
        let resp = self.send(self.client.post(&url)).await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_bounded_error_body(resp).await?;
            self.maybe_resync_on_invalid_timestamp(&body).await;
            return Err(order_response_error(status, &body));
        }

        let order: BinanceOrderResponse = read_bounded_json(resp, 64 * 1024)
            .await
            .map_err(|e| ExchangeError::Other(format!("Failed to parse order response: {e}")))?;
        Ok(order)
    }

    /// Cancel a single order by orderId or origClientOrderId
    pub(crate) async fn cancel_order(
        &self,
        symbol: &str,
        order_id: Option<i64>,
        client_order_id: Option<&str>,
    ) -> Result<BinanceOrderResponse> {
        if order_id.is_none() {
            if let Some(cid) = client_order_id {
                anyhow::ensure!(is_valid_client_order_id(cid), "Invalid client_order_id");
            }
        }
        self.reserve(1, RequestClass::Critical)?;
        let url = if let Some(id) = order_id {
            self.signed_url_fmt(
                "/fapi/v1/order",
                format_args!("symbol={}&orderId={}", symbol, id),
            )
        } else if let Some(cid) = client_order_id {
            self.signed_url_fmt(
                "/fapi/v1/order",
                format_args!("symbol={}&origClientOrderId={}", symbol, cid),
            )
        } else {
            self.signed_url_fmt("/fapi/v1/order", format_args!("symbol={}", symbol))
        };

        let resp = self
            .ensure_signed_success(
                self.send(self.client.delete(&url)).await?,
                "Failed to cancel order",
            )
            .await?;
        read_bounded_json(resp, 64 * 1024).await
    }

    /// Place 1..=5 limit orders in a single `/fapi/v1/batchOrders` call.
    #[allow(dead_code)]
    pub(crate) async fn place_batch_orders(
        &self,
        orders: &[NewOrderRequest<'_>],
    ) -> std::result::Result<Vec<BinanceBatchOrderItem>, ExchangeError> {
        if orders.is_empty() || orders.len() > 5 {
            return Err(ExchangeError::RequestRejected(
                "batchOrders requires 1..=5 orders".to_string(),
            ));
        }
        let mut wire_items = Vec::with_capacity(orders.len());
        let mut all_reduce_only = true;
        for order in orders {
            if !is_valid_client_order_id(order.client_order_id) {
                return Err(ExchangeError::RequestRejected(
                    "Invalid client_order_id".to_string(),
                ));
            }
            if order.symbol.is_empty()
                || order.side.is_empty()
                || order.price.is_empty()
                || order.quantity.is_empty()
            {
                return Err(ExchangeError::RequestRejected(
                    "Batch order contains empty required field".to_string(),
                ));
            }
            all_reduce_only &= order.reduce_only;
            wire_items.push(BatchOrderWireItem {
                symbol: order.symbol,
                side: order.side,
                order_type: "LIMIT",
                time_in_force: if order.post_only { "GTX" } else { "GTC" },
                price: order.price,
                quantity: order.quantity,
                new_client_order_id: order.client_order_id,
                reduce_only: order.reduce_only.then_some("true"),
            });
        }
        let class = if all_reduce_only {
            RequestClass::ClosingOrder
        } else {
            RequestClass::OpeningOrder
        };
        self.reserve_with_orders(5, orders.len() as u64, class)
            .map_err(|error| ExchangeError::RequestRejected(error.to_string()))?;

        let batch_json = serde_json::to_string(&wire_items).map_err(|error| {
            ExchangeError::RequestRejected(format!("Failed to serialize batchOrders: {error}"))
        })?;
        let encoded_batch = percent_encode_query_value(&batch_json);
        let url = self.signed_url_fmt(
            "/fapi/v1/batchOrders",
            format_args!("batchOrders={}", encoded_batch),
        );
        let resp = self.send(self.client.post(&url)).await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = read_bounded_error_body(resp).await?;
            self.maybe_resync_on_invalid_timestamp(&body).await;
            return Err(order_response_error(status, &body));
        }
        let items: Vec<BinanceBatchOrderItem> =
            read_bounded_json(resp, 256 * 1024).await.map_err(|error| {
                ExchangeError::Other(format!("Failed to parse batchOrders response: {error}"))
            })?;
        if items.len() != orders.len() {
            return Err(ExchangeError::Other(format!(
                "batchOrders response length mismatch: expected {}, got {}",
                orders.len(),
                items.len()
            )));
        }
        Ok(items)
    }

    /// Cancel 1..=10 orders for a symbol in a single `/fapi/v1/batchOrders` call.
    #[allow(dead_code)]
    pub(crate) async fn cancel_batch_orders(
        &self,
        symbol: &str,
        target: CancelBatchTarget<'_>,
    ) -> Result<Vec<BinanceBatchOrderItem>> {
        anyhow::ensure!(!symbol.is_empty(), "Symbol cannot be empty");
        let (param_name, raw_json, expected_len) = match target {
            CancelBatchTarget::OrderIds(ids) => {
                anyhow::ensure!(
                    (1..=10).contains(&ids.len()),
                    "cancel_batch_orders requires 1..=10 order IDs"
                );
                anyhow::ensure!(ids.iter().all(|&id| id > 0), "Invalid order_id in batch");
                ("orderIdList", serde_json::to_string(ids)?, ids.len())
            }
            CancelBatchTarget::ClientOrderIds(cids) => {
                anyhow::ensure!(
                    (1..=10).contains(&cids.len()),
                    "cancel_batch_orders requires 1..=10 client order IDs"
                );
                anyhow::ensure!(
                    cids.iter().all(|cid| is_valid_client_order_id(cid)),
                    "Invalid client_order_id in batch"
                );
                (
                    "origClientOrderIdList",
                    serde_json::to_string(cids)?,
                    cids.len(),
                )
            }
        };
        self.reserve(1, RequestClass::Critical)?;
        let encoded_list = percent_encode_query_value(&raw_json);
        let url = self.signed_url_fmt(
            "/fapi/v1/batchOrders",
            format_args!("symbol={}&{}={}", symbol, param_name, encoded_list),
        );
        let resp = self
            .ensure_signed_success(
                self.send(self.client.delete(&url)).await?,
                "Failed to cancel batch orders",
            )
            .await?;
        let items: Vec<BinanceBatchOrderItem> = read_bounded_json(resp, 256 * 1024).await?;
        anyhow::ensure!(
            items.len() == expected_len,
            "cancel_batch_orders response length mismatch: expected {expected_len}, got {}",
            items.len()
        );
        Ok(items)
    }

    /// Create a user data stream listenKey (`POST /fapi/v1/listenKey`).
    pub(crate) async fn create_listen_key(&self) -> Result<String> {
        self.reserve(1, RequestClass::Normal)?;
        let url = format!("{}/fapi/v1/listenKey", self.base_url);
        let resp = checked_response(self.send(self.client.post(&url)).await?).await?;
        let parsed: BinanceListenKeyResponse = read_bounded_json(resp, 16 * 1024).await?;
        anyhow::ensure!(
            is_valid_listen_key(&parsed.listen_key),
            "Invalid listenKey returned by Binance"
        );
        Ok(parsed.listen_key)
    }

    /// Keepalive an active user data stream listenKey (`PUT /fapi/v1/listenKey`).
    pub(crate) async fn keepalive_listen_key(&self, listen_key: &str) -> Result<()> {
        anyhow::ensure!(is_valid_listen_key(listen_key), "Invalid listenKey");
        self.reserve(1, RequestClass::Normal)?;
        let url = format!("{}/fapi/v1/listenKey", self.base_url);
        let req = self.client.put(&url).query(&[("listenKey", listen_key)]);
        let _ = checked_response(self.send(req).await?).await?;
        Ok(())
    }

    /// Close a user data stream listenKey (`DELETE /fapi/v1/listenKey`).
    #[allow(dead_code)]
    pub(crate) async fn close_listen_key(&self, listen_key: &str) -> Result<()> {
        anyhow::ensure!(is_valid_listen_key(listen_key), "Invalid listenKey");
        self.reserve(1, RequestClass::Normal)?;
        let url = format!("{}/fapi/v1/listenKey", self.base_url);
        let req = self.client.delete(&url).query(&[("listenKey", listen_key)]);
        let _ = checked_response(self.send(req).await?).await?;
        Ok(())
    }

    /// Fetch position risk information for symbol
    pub(crate) async fn get_position(&self, symbol: &str) -> Result<Option<BinancePositionRisk>> {
        self.get_position_with_class(symbol, RequestClass::Critical)
            .await
    }

    pub(crate) async fn get_position_for_periodic_sync(
        &self,
        symbol: &str,
    ) -> Result<Option<BinancePositionRisk>> {
        self.get_position_with_class(symbol, RequestClass::Normal)
            .await
    }

    async fn get_position_with_class(
        &self,
        symbol: &str,
        class: RequestClass,
    ) -> Result<Option<BinancePositionRisk>> {
        let url = self.prepare_signed_get_url(
            5,
            class,
            "/fapi/v2/positionRisk",
            format_args!("symbol={}", symbol),
        )?;
        let positions: Vec<BinancePositionRisk> = self
            .get_signed_json_url(&url, "Failed to get position risk")
            .await?;
        one_way_position(positions, symbol)
    }

    /// Fetch position risk for all symbols on the account.
    pub(crate) async fn get_all_positions(&self) -> Result<Vec<BinancePositionRisk>> {
        let url = self.prepare_signed_get_url(
            5,
            RequestClass::Normal,
            "/fapi/v2/positionRisk",
            format_args!(""),
        )?;
        self.get_signed_json_url(&url, "Failed to get position risk")
            .await
    }

    /// Fetch account balance summary
    pub(crate) async fn get_account(&self) -> Result<BinanceAccountInfoResponse> {
        self.get_account_with_class(RequestClass::Normal).await
    }

    pub(crate) async fn get_account_critical(&self) -> Result<BinanceAccountInfoResponse> {
        self.get_account_with_class(RequestClass::Critical).await
    }

    async fn get_account_with_class(
        &self,
        class: RequestClass,
    ) -> Result<BinanceAccountInfoResponse> {
        let url = self.prepare_signed_get_url(5, class, "/fapi/v2/account", format_args!(""))?;
        self.get_signed_json_url(&url, "Failed to get account info")
            .await
    }
}

pub(super) fn one_way_position(
    positions: Vec<BinancePositionRisk>,
    symbol: &str,
) -> Result<Option<BinancePositionRisk>> {
    let mut matching = positions
        .into_iter()
        .filter(|position| position.symbol.eq_ignore_ascii_case(symbol));
    let position = matching.next();
    anyhow::ensure!(
        position.as_ref().is_none_or(|position| position
            .position_side
            .as_deref()
            .is_none_or(|side| side == "BOTH"))
            && matching.next().is_none(),
        ONE_WAY_POSITION_ERROR
    );
    Ok(position)
}
