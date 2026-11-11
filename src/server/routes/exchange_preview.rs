use super::{json_ok, status_err, ApiResponse};
use crate::config::AppConfig;
use crate::exchange::rate_limit::RateLimiter;
use crate::exchange::BinanceFuturesClient;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::warn;

const EXCHANGE_INFO_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

struct CachedExchangeCatalog {
    fetched_at: Instant,
    info: Arc<crate::exchange::BinanceExchangeInfo>,
    trading_symbols: Vec<String>,
}

pub(super) struct ExchangePreviewService {
    client: BinanceFuturesClient,
    catalog_cache: RwLock<Option<CachedExchangeCatalog>>,
}

impl ExchangePreviewService {
    pub(super) fn new(limiter: Arc<RateLimiter>) -> Arc<Self> {
        let config = AppConfig::default().exchange;
        Arc::new(Self {
            client: BinanceFuturesClient::new_with_limiter(&config, limiter),
            catalog_cache: RwLock::new(None),
        })
    }

    async fn load_or_fetch_catalog(
        &self,
    ) -> Result<(Arc<crate::exchange::BinanceExchangeInfo>, Vec<String>), anyhow::Error> {
        if let Some(cached) = self.catalog_cache.read().await.as_ref() {
            if cached.fetched_at.elapsed() < EXCHANGE_INFO_CACHE_TTL {
                return Ok((Arc::clone(&cached.info), cached.trading_symbols.clone()));
            }
        }
        let info = Arc::new(self.client.get_exchange_info(None).await?);
        let mut trading_symbols: Vec<String> = info
            .symbols
            .iter()
            .filter(|symbol| symbol.status == "TRADING")
            .map(|symbol| symbol.symbol.clone())
            .collect();
        trading_symbols.sort_unstable();
        *self.catalog_cache.write().await = Some(CachedExchangeCatalog {
            fetched_at: Instant::now(),
            info: Arc::clone(&info),
            trading_symbols: trading_symbols.clone(),
        });
        Ok((info, trading_symbols))
    }
}

#[derive(Deserialize)]
pub(super) struct SymbolsQuery {
    environment: String,
}

#[derive(Deserialize)]
pub(super) struct SymbolPreviewQuery {
    environment: String,
    symbol: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(super) struct ExchangeSymbolPreview {
    pub symbol: String,
    pub last_price: Option<rust_decimal::Decimal>,
    pub mark_price: Option<rust_decimal::Decimal>,
    pub tick_size: rust_decimal::Decimal,
    pub step_size: rust_decimal::Decimal,
    pub min_qty: rust_decimal::Decimal,
    pub min_notional: rust_decimal::Decimal,
}

pub(super) async fn exchange_symbols_handler(
    Extension(service): Extension<Arc<ExchangePreviewService>>,
    query: Result<Query<SymbolsQuery>, axum::extract::rejection::QueryRejection>,
) -> (StatusCode, Json<ApiResponse<Vec<String>>>) {
    match query {
        Ok(Query(SymbolsQuery { environment })) if environment == "mainnet" => {}
        _ => return status_err(StatusCode::BAD_REQUEST, "无效的交易所环境，请使用 mainnet"),
    };
    match service.load_or_fetch_catalog().await {
        Ok((_, symbols)) => (StatusCode::OK, json_ok(symbols)),
        Err(error) => {
            warn!(%error, "获取交易所交易对失败");
            status_err(StatusCode::BAD_GATEWAY, "获取交易所交易对失败，请稍后重试")
        }
    }
}

#[cfg(test)]
pub(super) async fn fetch_exchange_symbols(
    client: &BinanceFuturesClient,
) -> (StatusCode, Json<ApiResponse<Vec<String>>>) {
    match client.get_exchange_info(None).await {
        Ok(info) => {
            let mut symbols: Vec<String> = info
                .symbols
                .into_iter()
                .filter(|symbol| symbol.status == "TRADING")
                .map(|symbol| symbol.symbol)
                .collect();
            symbols.sort_unstable();
            (StatusCode::OK, json_ok(symbols))
        }
        Err(error) => {
            warn!(%error, "获取交易所交易对失败");
            status_err(StatusCode::BAD_GATEWAY, "获取交易所交易对失败，请稍后重试")
        }
    }
}

pub(super) async fn exchange_symbol_preview_handler(
    Extension(service): Extension<Arc<ExchangePreviewService>>,
    query: Result<Query<SymbolPreviewQuery>, axum::extract::rejection::QueryRejection>,
) -> (StatusCode, Json<ApiResponse<ExchangeSymbolPreview>>) {
    let SymbolPreviewQuery {
        environment: _,
        symbol,
    } = match query {
        Ok(Query(q)) if q.environment == "mainnet" => q,
        _ => return status_err(StatusCode::BAD_REQUEST, "无效的交易所环境或交易对参数"),
    };
    let symbol = symbol.trim().to_ascii_uppercase();
    if crate::config::validate_symbol(&symbol).is_err() {
        return status_err(StatusCode::BAD_REQUEST, "无效的交易对代码");
    }
    fetch_exchange_symbol_preview_with_cache(&service, &symbol).await
}

async fn fetch_exchange_symbol_preview_with_cache(
    service: &ExchangePreviewService,
    symbol: &str,
) -> (StatusCode, Json<ApiResponse<ExchangeSymbolPreview>>) {
    let cached_info = service
        .catalog_cache
        .read()
        .await
        .as_ref()
        .filter(|cached| cached.fetched_at.elapsed() < EXCHANGE_INFO_CACHE_TTL)
        .map(|cached| Arc::clone(&cached.info));
    if let Some(info) = cached_info {
        if let Ok(rules) =
            crate::strategy::precision::SymbolRules::from_trading_exchange_info(&info, symbol)
        {
            let (last_res, mark_res) = tokio::join!(
                service.client.get_ticker_price(symbol),
                service.client.get_mark_price(symbol)
            );
            return build_symbol_preview_response(rules, last_res, mark_res);
        }
    }
    fetch_exchange_symbol_preview(&service.client, symbol).await
}

pub(super) async fn fetch_exchange_symbol_preview(
    client: &BinanceFuturesClient,
    symbol: &str,
) -> (StatusCode, Json<ApiResponse<ExchangeSymbolPreview>>) {
    let (info_res, last_res, mark_res) = tokio::join!(
        client.get_exchange_info(Some(symbol)),
        client.get_ticker_price(symbol),
        client.get_mark_price(symbol)
    );
    let info = match info_res {
        Ok(info) => info,
        Err(error) => {
            warn!(%error, symbol, "获取交易对规则失败");
            return status_err(StatusCode::BAD_GATEWAY, "获取交易对规则失败，请稍后重试");
        }
    };
    let rules =
        match crate::strategy::precision::SymbolRules::from_trading_exchange_info(&info, symbol) {
            Ok(rules) => rules,
            Err(err) => {
                let (status, message) = err.preview_status_and_message();
                return status_err(status, message);
            }
        };
    build_symbol_preview_response(rules, last_res, mark_res)
}

fn build_symbol_preview_response(
    rules: crate::strategy::precision::SymbolRules,
    last_res: anyhow::Result<rust_decimal::Decimal>,
    mark_res: anyhow::Result<rust_decimal::Decimal>,
) -> (StatusCode, Json<ApiResponse<ExchangeSymbolPreview>>) {
    let last_price = last_res
        .ok()
        .filter(|price| *price > rust_decimal::Decimal::ZERO);
    let mark_price = mark_res
        .ok()
        .filter(|price| *price > rust_decimal::Decimal::ZERO);
    (
        StatusCode::OK,
        json_ok(ExchangeSymbolPreview {
            symbol: rules.symbol,
            last_price,
            mark_price,
            tick_size: rules.tick_size,
            step_size: rules.step_size,
            min_qty: rules.min_qty,
            min_notional: rules.min_notional,
        }),
    )
}
