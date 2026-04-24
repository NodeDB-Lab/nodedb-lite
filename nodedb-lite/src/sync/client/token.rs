//! JWT token refresh (proactive at 80% lifetime, reactive on auth failure).

use nodedb_types::sync::wire::{TokenRefreshAckMsg, TokenRefreshMsg};

use super::state::SyncClient;

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

    /// Initiate a token refresh via the token provider.
    pub async fn initiate_token_refresh(&self) -> Option<TokenRefreshMsg> {
        let provider = self.config.token_provider.as_ref()?;
        *self.token_refresh_pending.lock().await = true;

        tracing::info!("initiating proactive JWT token refresh");
        let new_token = provider().await?;

        Some(TokenRefreshMsg { new_token })
    }

    /// Handle a TokenRefreshAck from Origin.
    pub async fn handle_token_refresh_ack(&self, ack: &TokenRefreshAckMsg) {
        *self.token_refresh_pending.lock().await = false;

        if ack.success {
            *self.token_set_at_ms.lock().await = crate::runtime::now_millis();
            *self.push_paused_for_auth.lock().await = false;
            tracing::info!(
                expires_in_secs = ack.expires_in_secs,
                "JWT token refresh accepted by Origin"
            );
        } else {
            tracing::warn!(
                error = ack.error.as_deref().unwrap_or("unknown"),
                "JWT token refresh rejected by Origin"
            );
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
}
