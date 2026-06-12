//! JWT token refresh (proactive at 80% lifetime, reactive on auth failure).
//!
//! On a successful refresh both `token_refresh_pending` and
//! `push_paused_for_auth` are cleared.  On failure `push_paused_for_auth`
//! remains set (push stays paused) and an exponential backoff interval is
//! applied before the next refresh attempt is permitted.  The provider call
//! itself is wrapped in a 30-second timeout so a hung provider cannot block
//! the sync task indefinitely.

use nodedb_types::sync::wire::{TokenRefreshAckMsg, TokenRefreshMsg};

use super::state::SyncClient;

/// Minimum interval (ms) between consecutive token refresh attempts after failure.
pub const TOKEN_REFRESH_MIN_BACKOFF_MS: u64 = 5_000; // 5 s

/// Maximum backoff interval (ms) between refresh attempts.
const TOKEN_REFRESH_MAX_BACKOFF_MS: u64 = 300_000; // 5 min

/// Timeout for a single provider() call.
const TOKEN_PROVIDER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl SyncClient {
    /// Check if the JWT token needs proactive refresh (at 80% of lifetime).
    ///
    /// Returns `true` if a refresh should be initiated. Called from the
    /// ping loop to piggyback on the keepalive timer.
    pub async fn should_refresh_token(&self) -> bool {
        if self.config.token_provider.is_none() || self.config.token_lifetime_secs == 0 {
            return false;
        }
        if *self.token_refresh_pending.lock().await {
            return false;
        }
        let set_at = *self.token_set_at_ms.lock().await;
        let now = crate::runtime::now_millis();
        let elapsed_ms = now.saturating_sub(set_at);
        let threshold_ms = self.config.token_lifetime_secs * 800; // 80% of lifetime
        elapsed_ms >= threshold_ms
    }

    /// Check if a token refresh attempt is currently allowed given the backoff.
    ///
    /// Returns `false` if the minimum retry interval since the last attempt has
    /// not elapsed yet. Called by `push_control_messages` before invoking
    /// `initiate_token_refresh`.
    pub async fn is_refresh_backoff_elapsed(&self) -> bool {
        let last = *self.token_last_attempt_ms.lock().await;
        if last == 0 {
            return true;
        }
        let backoff = *self.token_refresh_backoff_ms.lock().await;
        let now = crate::runtime::now_millis();
        now.saturating_sub(last) >= backoff
    }

    /// Initiate a token refresh via the token provider.
    ///
    /// Marks a refresh as in-flight, calls the provider with a 30-second
    /// timeout, and returns the new token message on success.  On failure
    /// (provider returns `None` or times out) the `token_refresh_pending` flag
    /// is cleared, `push_paused_for_auth` is kept `true` (push remains paused),
    /// and the backoff doubles for the next attempt.
    pub async fn initiate_token_refresh(&self) -> Option<TokenRefreshMsg> {
        let provider = self.config.token_provider.as_ref()?;

        // Record that we are starting an attempt now.
        *self.token_last_attempt_ms.lock().await = crate::runtime::now_millis();
        *self.token_refresh_pending.lock().await = true;

        tracing::info!("initiating JWT token refresh");

        let fut = provider();
        let result = tokio::time::timeout(TOKEN_PROVIDER_TIMEOUT, fut).await;

        match result {
            Ok(Some(new_token)) => {
                // Success — backoff resets to minimum for the next cycle.
                *self.token_refresh_backoff_ms.lock().await = TOKEN_REFRESH_MIN_BACKOFF_MS;
                Some(TokenRefreshMsg { new_token })
            }
            Ok(None) => {
                // Provider signalled failure (returned None).
                tracing::warn!("token provider returned None; keeping push paused");
                self.on_refresh_failure().await;
                None
            }
            Err(_elapsed) => {
                // Provider call timed out.
                tracing::warn!(
                    timeout_secs = TOKEN_PROVIDER_TIMEOUT.as_secs(),
                    "token provider timed out; keeping push paused"
                );
                self.on_refresh_failure().await;
                None
            }
        }
    }

    /// Handle a TokenRefreshAck from Origin.
    pub async fn handle_token_refresh_ack(&self, ack: &TokenRefreshAckMsg) {
        *self.token_refresh_pending.lock().await = false;

        if ack.success {
            // Full success: clear auth pause and reset backoff.
            *self.token_set_at_ms.lock().await = crate::runtime::now_millis();
            *self.push_paused_for_auth.lock().await = false;
            *self.token_refresh_backoff_ms.lock().await = TOKEN_REFRESH_MIN_BACKOFF_MS;
            tracing::info!(
                expires_in_secs = ack.expires_in_secs,
                "JWT token refresh accepted by Origin"
            );
        } else {
            // Origin rejected the new token — stay paused and back off.
            tracing::warn!(
                error = ack.error.as_deref().unwrap_or("unknown"),
                "JWT token refresh rejected by Origin; keeping push paused"
            );
            self.apply_backoff().await;
        }
    }

    /// Pause delta push due to auth failure. Called when Origin rejects
    /// with PermissionDenied, indicating the token has expired.
    pub async fn pause_for_auth(&self) {
        *self.push_paused_for_auth.lock().await = true;
        tracing::warn!("delta push paused — auth failure, awaiting token refresh");
    }

    /// Check if push is paused for auth.
    pub async fn is_push_paused_for_auth(&self) -> bool {
        *self.push_paused_for_auth.lock().await
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Called when a token refresh attempt fails (provider error or timeout).
    ///
    /// Clears `token_refresh_pending` so the next ping-loop tick can retry,
    /// but keeps `push_paused_for_auth = true` so no deltas are sent.
    /// Doubles the backoff for the next attempt.
    async fn on_refresh_failure(&self) {
        *self.token_refresh_pending.lock().await = false;
        // push_paused_for_auth stays true — push remains paused until a
        // successful refresh+ack cycle clears it in handle_token_refresh_ack.
        self.apply_backoff().await;
    }

    /// Double the backoff, capped at `TOKEN_REFRESH_MAX_BACKOFF_MS`.
    async fn apply_backoff(&self) {
        let mut backoff = self.token_refresh_backoff_ms.lock().await;
        *backoff = (*backoff * 2).min(TOKEN_REFRESH_MAX_BACKOFF_MS);
        tracing::debug!(
            next_backoff_ms = *backoff,
            "token refresh backoff increased"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::client::SyncConfig;
    use std::sync::Arc;

    fn make_config_with_provider(returns: Option<&'static str>) -> SyncConfig {
        let provider: crate::sync::client::config::TokenProvider =
            Arc::new(move || Box::pin(async move { returns.map(|s| s.to_string()) }));
        SyncConfig::new("wss://localhost:9090/sync", "test.jwt.token")
            .with_token_provider(provider, 3600)
    }

    #[tokio::test]
    async fn refresh_failure_keeps_push_paused() {
        let config = make_config_with_provider(None); // provider always fails
        let client = SyncClient::new(config, 1);
        client.pause_for_auth().await;

        // Attempt a refresh — provider returns None.
        let result = client.initiate_token_refresh().await;
        assert!(result.is_none());

        // Push must still be paused.
        assert!(client.is_push_paused_for_auth().await);
        // token_refresh_pending must be cleared so the next tick can retry.
        assert!(!*client.token_refresh_pending.lock().await);
    }

    #[tokio::test]
    async fn refresh_failure_applies_backoff() {
        let config = make_config_with_provider(None);
        let client = SyncClient::new(config, 1);
        client.pause_for_auth().await;

        let initial_backoff = *client.token_refresh_backoff_ms.lock().await;

        client.initiate_token_refresh().await;
        let after_first = *client.token_refresh_backoff_ms.lock().await;
        assert_eq!(
            after_first,
            (initial_backoff * 2).min(TOKEN_REFRESH_MAX_BACKOFF_MS)
        );

        client.initiate_token_refresh().await;
        let after_second = *client.token_refresh_backoff_ms.lock().await;
        assert_eq!(
            after_second,
            (after_first * 2).min(TOKEN_REFRESH_MAX_BACKOFF_MS)
        );
    }

    #[tokio::test]
    async fn refresh_success_clears_pause_and_resets_backoff() {
        let config = make_config_with_provider(Some("fresh-jwt-token"));
        let client = SyncClient::new(config, 1);
        client.pause_for_auth().await;

        // Drive backoff up.
        *client.token_refresh_backoff_ms.lock().await = 60_000;

        let msg = client.initiate_token_refresh().await;
        assert!(msg.is_some());
        assert_eq!(msg.unwrap().new_token, "fresh-jwt-token");

        // Simulate a successful TokenRefreshAck from Origin.
        client
            .handle_token_refresh_ack(&TokenRefreshAckMsg {
                success: true,
                expires_in_secs: 3600,
                error: None,
            })
            .await;

        assert!(!client.is_push_paused_for_auth().await);
        assert_eq!(
            *client.token_refresh_backoff_ms.lock().await,
            TOKEN_REFRESH_MIN_BACKOFF_MS
        );
    }

    #[tokio::test]
    async fn backoff_enforced_between_attempts() {
        let config = make_config_with_provider(None);
        let client = SyncClient::new(config, 1);

        // Simulate a failed attempt just now.
        *client.token_last_attempt_ms.lock().await = crate::runtime::now_millis();
        *client.token_refresh_backoff_ms.lock().await = TOKEN_REFRESH_MIN_BACKOFF_MS;

        // Backoff not elapsed — should not be allowed.
        assert!(!client.is_refresh_backoff_elapsed().await);

        // Simulate a very old last attempt.
        *client.token_last_attempt_ms.lock().await = 0;
        assert!(client.is_refresh_backoff_elapsed().await);
    }

    #[tokio::test]
    async fn backoff_capped_at_max() {
        let config = make_config_with_provider(None);
        let client = SyncClient::new(config, 1);

        // Start near the cap.
        *client.token_refresh_backoff_ms.lock().await = TOKEN_REFRESH_MAX_BACKOFF_MS / 2 + 1;
        client.apply_backoff().await;
        assert_eq!(
            *client.token_refresh_backoff_ms.lock().await,
            TOKEN_REFRESH_MAX_BACKOFF_MS
        );

        // Another doubling stays at cap.
        client.apply_backoff().await;
        assert_eq!(
            *client.token_refresh_backoff_ms.lock().await,
            TOKEN_REFRESH_MAX_BACKOFF_MS
        );
    }
}
