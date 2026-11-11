use crate::exchange::model::BinanceApiError;
use anyhow::{Context, Result};

const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;
const MAX_SUMMARIZED_ERROR_CHARS: usize = 256;

pub(super) fn is_invalid_timestamp_body(body: &str) -> bool {
    serde_json::from_str::<BinanceApiError>(body).is_ok_and(|error| error.code == -1021)
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct LocalRateLimitError(pub(super) String);

#[derive(Debug, thiserror::Error)]
pub(crate) enum ExchangeError {
    #[error("Post-Only (GTX) order rejected as it would execute as taker: {0}")]
    PostOnlyRejected(String),

    #[error("Binance API error (code: {code}): {msg}")]
    ApiError { code: i64, msg: String },

    #[error("Order execution status is unknown (code: {code:?}): {msg}")]
    ExecutionUnknown { code: Option<i64>, msg: String },

    #[error("Order request was rejected: {0}")]
    RequestRejected(String),

    #[error("Network or HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("General error: {0}")]
    Other(String),
}

impl ExchangeError {
    #[must_use]
    #[inline]
    pub(crate) fn execution_status_unknown(&self) -> bool {
        matches!(
            self,
            Self::ExecutionUnknown { .. } | Self::HttpError(_) | Self::Other(_)
        )
    }
}

pub(crate) async fn read_bounded_bytes(
    mut resp: reqwest::Response,
    max_bytes: usize,
) -> std::result::Result<Vec<u8>, reqwest::Error> {
    let mut buf = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(reqwest::Error::without_url)? {
        let remaining = max_bytes.saturating_sub(buf.len());
        if remaining == 0 {
            break;
        }
        let take = chunk.len().min(remaining);
        buf.extend_from_slice(&chunk[..take]);
        if buf.len() >= max_bytes {
            break;
        }
    }
    Ok(buf)
}

pub(crate) async fn read_bounded_json<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
    max_bytes: usize,
) -> anyhow::Result<T> {
    anyhow::ensure!(
        resp.content_length()
            .is_none_or(|len| len <= max_bytes as u64),
        "HTTP response body exceeded {max_bytes} bytes"
    );
    let bytes = read_bounded_bytes(resp, max_bytes.saturating_add(1)).await?;
    anyhow::ensure!(
        bytes.len() <= max_bytes,
        "HTTP response body exceeded {max_bytes} bytes"
    );
    serde_json::from_slice(&bytes).context("Failed to parse JSON response")
}

pub(super) async fn read_bounded_error_body(
    resp: reqwest::Response,
) -> std::result::Result<String, reqwest::Error> {
    let buf = read_bounded_bytes(resp, MAX_ERROR_BODY_BYTES).await?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

pub(crate) fn summarize_non_json_body(body: &str) -> String {
    let trimmed = body.trim().trim_start_matches('\u{feff}').trim_start();
    if trimmed.is_empty() {
        return "empty response body".to_string();
    }
    let lower_prefix: String = trimmed.chars().take(9).collect();
    if trimmed.starts_with('<')
        || lower_prefix.eq_ignore_ascii_case("<!doctype")
        || lower_prefix
            .get(..5)
            .is_some_and(|p| p.eq_ignore_ascii_case("<html"))
    {
        return "upstream HTML error response".to_string();
    }
    let mut out = String::with_capacity(trimmed.len().min(MAX_SUMMARIZED_ERROR_CHARS));
    let mut chars = 0usize;
    for word in trimmed.split_whitespace() {
        if chars > 0 {
            if chars + 1 > MAX_SUMMARIZED_ERROR_CHARS {
                break;
            }
            out.push(' ');
            chars += 1;
        }
        for ch in word.chars() {
            if chars >= MAX_SUMMARIZED_ERROR_CHARS {
                return out;
            }
            out.push(ch);
            chars += 1;
        }
    }
    out
}

pub(super) fn is_valid_client_order_id(cid: &str) -> bool {
    !cid.is_empty()
        && cid.len() <= 36
        && cid
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'/' | b'-'))
}

pub(super) fn order_response_error(status: reqwest::StatusCode, body: &str) -> ExchangeError {
    let api_error = serde_json::from_str::<BinanceApiError>(body).ok();
    let code = api_error.as_ref().map(|error| error.code);
    let raw_msg = api_error
        .as_ref()
        .map(|error| error.msg.as_str())
        .unwrap_or(body);
    let summary = summarize_non_json_body(raw_msg);
    // These overload responses explicitly mean the submission failed. Other
    // server errors and backend timeouts may have accepted an order already.
    let known_failure = status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        && (code == Some(-1008)
            || raw_msg == "Service Unavailable."
            || raw_msg == "Internal error; unable to process your request. Please try again.");
    if !known_failure
        && (matches!(code, Some(-1000 | -1001 | -1006 | -1007))
            || status == reqwest::StatusCode::REQUEST_TIMEOUT
            || status.is_server_error())
    {
        return ExchangeError::ExecutionUnknown { code, msg: summary };
    }
    match api_error {
        Some(error) if error.code == -5022 => ExchangeError::PostOnlyRejected(summary),
        Some(error) => ExchangeError::ApiError {
            code: error.code,
            msg: summary,
        },
        None if known_failure => {
            ExchangeError::RequestRejected(format!("HTTP {}: {}", status, summary))
        }
        None => ExchangeError::Other(format!("HTTP error {}: {}", status, summary)),
    }
}

pub(super) fn parse_api_error_body(status: reqwest::StatusCode, body: &str) -> anyhow::Error {
    match serde_json::from_str::<BinanceApiError>(body) {
        Ok(error) => ExchangeError::ApiError {
            code: error.code,
            msg: summarize_non_json_body(&error.msg),
        }
        .into(),
        Err(_) => ExchangeError::Other(format!(
            "HTTP error {}: {}",
            status,
            summarize_non_json_body(body)
        ))
        .into(),
    }
}

pub(super) async fn checked_response(response: reqwest::Response) -> Result<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = read_bounded_error_body(response).await?;
    Err(parse_api_error_body(status, &body))
}
