//! Handshake: build outgoing `HandshakeMsg`, process incoming `HandshakeAckMsg`.

use nodedb_types::sync::wire::{HandshakeAckMsg, HandshakeMsg};

use super::config::SyncState;
use super::state::SyncClient;

impl SyncClient {
    /// Build a handshake message from current state.
    pub async fn build_handshake(&self) -> HandshakeMsg {
        let clock = self.clock.lock().await;
        let shapes = self.shapes.lock().await;

        let wire_clock = clock.to_wire();
        let mut vector_clock = std::collections::HashMap::new();
        // Origin expects: { collection: { doc_id: lamport_ts } }
        // We send a simplified version: { "_global": { peer_hex: counter } }
        vector_clock.insert("_global".to_string(), wire_clock);

        HandshakeMsg {
            jwt_token: self.config.jwt_token.clone(),
            vector_clock,
            subscribed_shapes: shapes.active_shape_ids(),
            client_version: self.config.client_version.clone(),
            lite_id: self.lite_id.clone().unwrap_or_default(),
            epoch: self.epoch.unwrap_or(0),
            wire_version: 1,
        }
    }

    /// Process a handshake acknowledgment from Origin.
    pub async fn handle_handshake_ack(&self, ack: &HandshakeAckMsg) -> bool {
        if !ack.success {
            tracing::warn!(
                error = ack.error.as_deref().unwrap_or("unknown"),
                "handshake rejected by Origin"
            );
            return false;
        }

        *self.session_id.lock().await = Some(ack.session_id.clone());

        let mut clock = self.clock.lock().await;
        for (peer_hex, &counter) in &ack.server_clock {
            if let Ok(peer_id) = u64::from_str_radix(peer_hex, 16) {
                clock.advance(peer_id, counter);
            }
        }

        *self.state.lock().await = SyncState::Connected;
        tracing::info!(session = %ack.session_id, "sync handshake accepted");
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::client::SyncConfig;

    fn make_config() -> SyncConfig {
        SyncConfig::new("wss://localhost:9090/sync", "test.jwt.token")
    }

    #[tokio::test]
    async fn build_handshake() {
        let client = SyncClient::new(make_config(), 1);

        {
            let mut shapes = client.shapes().lock().await;
            shapes.subscribe(nodedb_types::sync::shape::ShapeDefinition {
                shape_id: "s1".into(),
                tenant_id: 1,
                shape_type: nodedb_types::sync::shape::ShapeType::Document {
                    collection: "orders".into(),
                    predicate: Vec::new(),
                },
                description: "test".into(),
                field_filter: vec![],
            });
        }

        let hs = client.build_handshake().await;
        assert_eq!(hs.jwt_token, "test.jwt.token");
        assert_eq!(hs.subscribed_shapes, vec!["s1"]);
    }

    #[tokio::test]
    async fn handle_handshake_ack_success() {
        let client = SyncClient::new(make_config(), 1);
        let ack = HandshakeAckMsg {
            success: true,
            session_id: "sess-123".into(),
            server_clock: std::collections::HashMap::new(),
            error: None,
            fork_detected: false,
            server_wire_version: 1,
        };

        assert!(client.handle_handshake_ack(&ack).await);
        assert_eq!(client.state().await, SyncState::Connected);
    }

    #[tokio::test]
    async fn handle_handshake_ack_failure() {
        let client = SyncClient::new(make_config(), 1);
        let ack = HandshakeAckMsg {
            success: false,
            session_id: String::new(),
            server_clock: std::collections::HashMap::new(),
            error: Some("invalid token".into()),
            fork_detected: false,
            server_wire_version: 1,
        };

        assert!(!client.handle_handshake_ack(&ack).await);
        assert_eq!(client.state().await, SyncState::Disconnected);
    }
}
