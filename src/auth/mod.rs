use argon2::password_hash::{
    rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
};
use argon2::Argon2;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::net::IpAddr;

/// Administrator credentials supplied at process startup.
///
/// Only the Argon2 hash is held in memory; the plaintext password is never
/// written to the database or configuration files.
pub struct AdminAuth {
    password_hash: String,
}

const MAX_ADMIN_PASSWORD_BYTES: usize = 1024;
pub(crate) const MIN_METRICS_TOKEN_BYTES: usize = 16;
pub(crate) const MAX_METRICS_TOKEN_BYTES: usize = 1024;

impl AdminAuth {
    pub fn from_password(password: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            password.len() >= 6,
            "管理员密码长度至少为 6 个字符（--admin-password 或 ADMIN_PASSWORD）"
        );
        anyhow::ensure!(
            password.len() <= MAX_ADMIN_PASSWORD_BYTES,
            "管理员密码长度不能超过 {} 字节",
            MAX_ADMIN_PASSWORD_BYTES
        );
        Ok(Self {
            password_hash: hash_password_argon2(password)?,
        })
    }

    #[must_use]
    #[inline]
    pub fn verify(&self, password: &str) -> bool {
        verify_password_argon2(password, &self.password_hash)
    }
}

/// Dedicated read-only scrape token for `GET /api/metrics`.
///
/// Stores only the SHA-256 digest in memory and verifies candidates in constant
/// time without touching SQLite sessions or granting access to other endpoints.
pub struct MetricsAuth {
    token_digest: [u8; 32],
}

impl MetricsAuth {
    pub fn from_token(token: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            token.len() >= MIN_METRICS_TOKEN_BYTES,
            "指标抓取令牌长度至少为 {} 个字符（--metrics-token 或 METRICS_TOKEN）",
            MIN_METRICS_TOKEN_BYTES
        );
        anyhow::ensure!(
            token.len() <= MAX_METRICS_TOKEN_BYTES,
            "指标抓取令牌长度不能超过 {} 字节",
            MAX_METRICS_TOKEN_BYTES
        );
        anyhow::ensure!(
            token.bytes().all(|b| b.is_ascii_graphic()),
            "指标抓取令牌仅允许可见 ASCII 字符且不能包含空白字符"
        );
        Ok(Self {
            token_digest: Sha256::digest(token.as_bytes()).into(),
        })
    }

    #[must_use]
    pub fn verify(&self, candidate: &str) -> bool {
        if !(MIN_METRICS_TOKEN_BYTES..=MAX_METRICS_TOKEN_BYTES).contains(&candidate.len())
            || !candidate.bytes().all(|b| b.is_ascii_graphic())
        {
            return false;
        }
        let digest: [u8; 32] = Sha256::digest(candidate.as_bytes()).into();
        let diff = self
            .token_digest
            .iter()
            .zip(digest.iter())
            .fold(0u8, |acc, (&a, &b)| std::hint::black_box(acc | (a ^ b)));
        diff == 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IpCidr {
    V4 { net: u32, mask: u32 },
    V6 { net: u128, mask: u128 },
}

/// Allowlist of reverse-proxy IPs or CIDRs permitted to supply `X-Real-IP` for login rate limiting.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrustedProxies {
    entries: Vec<IpCidr>,
}

pub(crate) fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    }
}

impl TrustedProxies {
    pub fn parse(spec: &str) -> anyhow::Result<Self> {
        let mut entries = Vec::new();
        for raw in spec.split(',') {
            let item = raw.trim();
            if item.is_empty() {
                continue;
            }
            let cidr = if let Some((addr_str, prefix_str)) = item.split_once('/') {
                let addr: IpAddr = addr_str
                    .trim()
                    .parse()
                    .map_err(|_| anyhow::anyhow!("无效的可信代理网段地址: {item:?}"))?;
                let prefix: u8 = prefix_str
                    .trim()
                    .parse()
                    .map_err(|_| anyhow::anyhow!("无效的可信代理掩码长度: {item:?}"))?;
                match addr {
                    IpAddr::V4(v4) => {
                        anyhow::ensure!(prefix <= 32, "IPv4 掩码长度不能超过 32: {item:?}");
                        let mask = if prefix == 0 {
                            0
                        } else {
                            u32::MAX << (32 - prefix)
                        };
                        IpCidr::V4 {
                            net: u32::from(v4) & mask,
                            mask,
                        }
                    }
                    IpAddr::V6(v6) => {
                        anyhow::ensure!(prefix <= 128, "IPv6 掩码长度不能超过 128: {item:?}");
                        if let (Some(v4), true) = (v6.to_ipv4_mapped(), prefix >= 96) {
                            let v4_prefix = prefix - 96;
                            let mask = if v4_prefix == 0 {
                                0
                            } else {
                                u32::MAX << (32 - v4_prefix)
                            };
                            IpCidr::V4 {
                                net: u32::from(v4) & mask,
                                mask,
                            }
                        } else {
                            let mask = if prefix == 0 {
                                0
                            } else {
                                u128::MAX << (128 - prefix)
                            };
                            IpCidr::V6 {
                                net: u128::from(v6) & mask,
                                mask,
                            }
                        }
                    }
                }
            } else {
                let addr: IpAddr = item
                    .parse()
                    .map_err(|_| anyhow::anyhow!("无效的可信代理 IP 地址: {item:?}"))?;
                match canonical_ip(addr) {
                    IpAddr::V4(v4) => IpCidr::V4 {
                        net: u32::from(v4),
                        mask: u32::MAX,
                    },
                    IpAddr::V6(v6) => IpCidr::V6 {
                        net: u128::from(v6),
                        mask: u128::MAX,
                    },
                }
            };
            entries.push(cidr);
        }
        Ok(Self { entries })
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn contains(&self, ip: IpAddr) -> bool {
        match canonical_ip(ip) {
            IpAddr::V4(v4) => {
                let bits = u32::from(v4);
                self.entries.iter().any(|entry| match *entry {
                    IpCidr::V4 { net, mask } => (bits & mask) == net,
                    IpCidr::V6 { .. } => false,
                })
            }
            IpAddr::V6(v6) => {
                let bits = u128::from(v6);
                self.entries.iter().any(|entry| match *entry {
                    IpCidr::V6 { net, mask } => (bits & mask) == net,
                    IpCidr::V4 { .. } => false,
                })
            }
        }
    }
}

fn hash_password_argon2(password: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|error| anyhow::anyhow!("Could not hash admin password: {}", error))
}

fn verify_password_argon2(password: &str, stored_hash: &str) -> bool {
    if password.len() > MAX_ADMIN_PASSWORD_BYTES {
        return false;
    }
    PasswordHash::new(stored_hash).ok().is_some_and(|parsed| {
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    })
}

/// Generates a secure random 32-byte hex session token (64 hex characters).
#[must_use]
#[inline]
pub(crate) fn generate_session_token() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[cfg(test)]
mod tests;
