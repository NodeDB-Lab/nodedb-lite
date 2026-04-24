//! Sync client configuration types.

use std::sync::Arc;
use std::time::Duration;

/// Token provider callback type.
///
/// Called when the sync client needs a fresh JWT token — either proactively
/// before expiry or reactively after an auth rejection. The provider should
/// return a fresh JWT token string.
///
/// # Example
/// ```ignore
/// let provider: TokenProvider = Arc::new(|| Box::pin(async {
///     my_auth_service.get_token().await
/// }));
/// ```
pub type TokenProvider = Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send>>
        + Send
        + Sync,
>;

/// Sync client configuration.
#[derive(Clone)]
pub struct SyncConfig {
    /// WebSocket URL to the Origin sync endpoint (e.g., `wss://api.nodedb.cloud/sync`).
    pub url: String,
    /// JWT bearer token for initial authentication.
    pub jwt_token: String,
    /// Client version string (sent in handshake).
    pub client_version: String,
    /// Minimum backoff on reconnect.
    pub min_backoff: Duration,
    /// Maximum backoff on reconnect.
    pub max_backoff: Duration,
    /// Keepalive ping interval.
    pub ping_interval: Duration,
    /// Maximum deltas to batch in a single push.
    pub max_batch_size: usize,
    /// Token provider for automatic refresh. If `None`, no auto-refresh occurs.
    pub token_provider: Option<TokenProvider>,
    /// Token lifetime in seconds (used to schedule proactive refresh at 80%).
    /// If 0, no proactive refresh occurs — only reactive on auth rejection.
    pub token_lifetime_secs: u64,
}

impl std::fmt::Debug for SyncConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncConfig")
            .field("url", &self.url)
            .field("jwt_token", &"[REDACTED]")
            .field("client_version", &self.client_version)
            .field("min_backoff", &self.min_backoff)
            .field("max_backoff", &self.max_backoff)
            .field("ping_interval", &self.ping_interval)
            .field("max_batch_size", &self.max_batch_size)
            .field("token_provider", &self.token_provider.is_some())
            .field("token_lifetime_secs", &self.token_lifetime_secs)
            .finish()
    }
}

impl SyncConfig {
    pub fn new(url: impl Into<String>, jwt_token: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            jwt_token: jwt_token.into(),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            ping_interval: Duration::from_secs(30),
            max_batch_size: 100,
            token_provider: None,
            token_lifetime_secs: 0,
        }
    }

    /// Set a token provider for automatic JWT refresh.
    pub fn with_token_provider(mut self, provider: TokenProvider, lifetime_secs: u64) -> Self {
        self.token_provider = Some(provider);
        self.token_lifetime_secs = lifetime_secs;
        self
    }
}

/// Connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Not connected, not trying.
    Disconnected,
    /// Attempting to connect.
    Connecting,
    /// Connected and authenticated.
    Connected,
    /// Connection lost, backing off before retry.
    Reconnecting,
}
