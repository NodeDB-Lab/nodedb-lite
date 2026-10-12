//! Per-session state of the array push path.
//!
//! Origin sends no frame for an applied `ArrayDelta`. It sends one
//! `ArrayReject` for each refused frame. Origin handles one session's frames
//! strictly in order and answers a `PingPong` ping inline. A pong therefore
//! proves two facts: Origin finished every frame sent before the ping, and
//! every reject for those frames already arrived on this socket.
//!
//! The push loop sends a window of deltas, then a ping whose token marks the
//! window. On the matching pong, every op in the window that drew no reject
//! is confirmed, and the push loop retires it from the durable queue. One
//! window is open at a time.
//!
//! The window is reset on every connect. Unconfirmed ops stay in the durable
//! queue, so the next session re-sends them. Origin deduplicates a re-sent op
//! by its HLC.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use nodedb_array::sync::hlc::Hlc;
use nodedb_types::sync::wire::ArrayRejectMsg;

use crate::sync::array::{PendingArrayOp, RejectDisposition, reject_disposition};

/// Maximum ops sent in one window.
pub(crate) const ARRAY_PUSH_WINDOW: usize = 256;

/// How long a window waits for its pong before its ops are re-sent.
pub(crate) const ARRAY_BARRIER_TIMEOUT: Duration = Duration::from_secs(30);

/// Schema announces per array per session before the array is parked.
pub(crate) const MAX_SCHEMA_ANNOUNCES: u32 = 3;

/// Set on every barrier token. `ping_loop` stamps pings with wall-clock
/// milliseconds, which never set this bit, so its pongs never match a window.
const BARRIER_TOKEN_TAG: u64 = 1 << 63;

type HlcKey = [u8; 18];

/// What a push tick does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TickPlan {
    /// A window is open. Send nothing.
    Wait,
    /// Read queued ops strictly after `after` (`None`: the queue head).
    Drain { after: Option<Hlc> },
}

/// Whether an array's schema must go out before its ops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnnounceStep {
    /// Announced this session. Send the ops.
    Ready,
    /// Send the schema first.
    Send,
    /// The announce limit is reached. The array is parked.
    GiveUp,
}

/// Result of a completed window.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct WindowOutcome {
    /// Ops Origin applied. Retire them from the durable queue.
    pub(crate) confirmed: Vec<Hlc>,
    /// Ops held until the next session because a reject named no op.
    pub(crate) parked_unattributed: usize,
}

/// Array push state for one session.
#[derive(Default)]
pub(crate) struct ArrayPushWindow {
    /// Schema HLC announced per array this session.
    announced: HashMap<String, Hlc>,
    /// Announces sent per array this session.
    announce_count: HashMap<String, u32>,
    /// Arrays whose schema goes out again before their next op.
    reannounce: HashSet<String>,
    /// Arrays held until the next session: Origin refused their schema.
    parked_arrays: HashSet<String>,
    /// Ops held until the next session.
    parked_ops: HashSet<HlcKey>,
    /// Highest op HLC scanned this session. The next drain starts after it.
    cursor: Option<Hlc>,
    /// Restart the next drain at the queue head: some queued op needs another
    /// send this session.
    rewind: bool,
    /// Ops sent in the open window.
    in_flight: Vec<Hlc>,
    /// Ops Origin refused in the open window.
    rejected: HashSet<HlcKey>,
    /// A reject in the open window named no op.
    unattributed: bool,
    /// Token and send time of the open window's ping.
    barrier: Option<(u64, Instant)>,
    next_token: u64,
}

impl ArrayPushWindow {
    /// Forget all session state. The token counter keeps counting.
    pub(crate) fn reset(&mut self) {
        let next_token = self.next_token;
        *self = Self {
            next_token,
            ..Self::default()
        };
    }

    /// Start a push tick. A window past [`ARRAY_BARRIER_TIMEOUT`] is
    /// abandoned: its ops stay queued and the drain restarts at the head.
    pub(crate) fn begin_tick(&mut self, now: Instant) -> TickPlan {
        if let Some((token, sent_at)) = self.barrier {
            if now.duration_since(sent_at) < ARRAY_BARRIER_TIMEOUT {
                return TickPlan::Wait;
            }
            tracing::warn!(
                token,
                in_flight = self.in_flight.len(),
                "array push: no pong for the window; its ops stay queued and are re-sent"
            );
            self.abandon_window();
        }
        TickPlan::Drain { after: self.cursor }
    }

    /// Drop the open window without confirming any op, and rewind.
    pub(crate) fn abandon_window(&mut self) {
        self.in_flight.clear();
        self.rejected.clear();
        self.unattributed = false;
        self.barrier = None;
        self.cursor = None;
        self.rewind = false;
    }

    /// Whether `op` is held until the next session.
    pub(crate) fn is_held(&self, op: &PendingArrayOp) -> bool {
        self.parked_arrays.contains(&op.array) || self.parked_ops.contains(&op.hlc.to_bytes())
    }

    /// Move the drain cursor past a scanned op.
    pub(crate) fn advance(&mut self, hlc: Hlc) {
        if self.cursor.is_none_or(|cursor| hlc > cursor) {
            self.cursor = Some(hlc);
        }
    }

    /// Decide whether `array`'s schema goes out before its ops.
    pub(crate) fn announce_step(&mut self, array: &str) -> AnnounceStep {
        if self.parked_arrays.contains(array) {
            return AnnounceStep::GiveUp;
        }
        if self.announced.contains_key(array) && !self.reannounce.contains(array) {
            return AnnounceStep::Ready;
        }
        let sent = self.announce_count.get(array).copied().unwrap_or(0);
        if sent >= MAX_SCHEMA_ANNOUNCES {
            tracing::error!(
                array,
                announces = sent,
                "array push: Origin still refuses ops after repeated schema announces; \
                 the array's ops stay queued until the next session"
            );
            self.parked_arrays.insert(array.to_owned());
            return AnnounceStep::GiveUp;
        }
        AnnounceStep::Send
    }

    /// Record a schema announce. Call before the frame goes out, so a reject
    /// of the announce that arrives first still finds it.
    pub(crate) fn record_announced(&mut self, array: &str, schema_hlc: Hlc) {
        self.announced.insert(array.to_owned(), schema_hlc);
        self.reannounce.remove(array);
        *self.announce_count.entry(array.to_owned()).or_insert(0) += 1;
    }

    /// Hold every op of `array` until the next session.
    pub(crate) fn park_array(&mut self, array: &str) {
        self.parked_arrays.insert(array.to_owned());
    }

    /// Restart the drain at the queue head once no window is open.
    pub(crate) fn request_rewind(&mut self) {
        self.rewind = true;
    }

    /// Record an op as sent. Call before the frame goes out, so a reject that
    /// arrives first still finds the op in the window.
    pub(crate) fn record_sent(&mut self, hlc: Hlc) {
        self.in_flight.push(hlc);
    }

    /// Whether the current tick sent any op.
    pub(crate) fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
    }

    /// Open the window's barrier and return the ping token. Call before the
    /// ping goes out.
    pub(crate) fn open_barrier(&mut self, now: Instant) -> u64 {
        let token = BARRIER_TOKEN_TAG | (self.next_token & !BARRIER_TOKEN_TAG);
        self.next_token = self.next_token.wrapping_add(1);
        self.barrier = Some((token, now));
        token
    }

    /// End a tick that opened no window. Applies a requested rewind.
    pub(crate) fn finish_idle_tick(&mut self) {
        if self.barrier.is_none() && self.rewind {
            self.cursor = None;
            self.rewind = false;
        }
    }

    /// Apply an `ArrayReject` to the session state.
    ///
    /// The pending queue itself is the delegate's: only a `Drop` removes an
    /// op there. This records which ops of the open window Origin refused,
    /// and which arrays or ops wait for a schema announce or the next session.
    pub(crate) fn record_reject(&mut self, msg: &ArrayRejectMsg) {
        let disposition = reject_disposition(msg.reason);
        if disposition == RejectDisposition::CatchUp {
            return;
        }
        let key = msg.op_hlc_bytes;
        if key == Hlc::ZERO.to_bytes() {
            if self.has_in_flight() {
                self.unattributed = true;
            }
            return;
        }
        if self
            .announced
            .get(&msg.array)
            .is_some_and(|schema_hlc| schema_hlc.to_bytes() == key)
        {
            tracing::error!(
                array = %msg.array,
                reason = ?msg.reason,
                detail = %msg.detail,
                "array push: Origin refused the schema announce; the array's ops stay queued \
                 until the next session"
            );
            self.parked_arrays.insert(msg.array.clone());
            return;
        }
        self.rejected.insert(key);
        match disposition {
            RejectDisposition::ResendSchema if !self.parked_arrays.contains(&msg.array) => {
                self.reannounce.insert(msg.array.clone());
                self.rewind = true;
            }
            RejectDisposition::Park => {
                self.parked_ops.insert(key);
            }
            RejectDisposition::ResendSchema
            | RejectDisposition::Drop
            | RejectDisposition::CatchUp => {}
        }
    }

    /// Close the window if `token` is its barrier token.
    ///
    /// Returns `None` for any other pong. A reject that named no op leaves no
    /// way to tell which op it refused, so the whole window is held until the
    /// next session rather than confirmed.
    pub(crate) fn complete_barrier(&mut self, token: u64) -> Option<WindowOutcome> {
        let (open_token, _) = self.barrier?;
        if open_token != token {
            return None;
        }
        let in_flight = std::mem::take(&mut self.in_flight);
        let mut outcome = WindowOutcome::default();
        if self.unattributed {
            outcome.parked_unattributed = in_flight.len();
            self.parked_ops
                .extend(in_flight.iter().map(|hlc| hlc.to_bytes()));
        } else {
            outcome.confirmed = in_flight
                .into_iter()
                .filter(|hlc| !self.rejected.contains(&hlc.to_bytes()))
                .collect();
        }
        self.rejected.clear();
        self.unattributed = false;
        self.barrier = None;
        if self.rewind {
            self.cursor = None;
            self.rewind = false;
        }
        Some(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_types::sync::wire::ArrayRejectReason;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, ReplicaId::new(1)).expect("valid hlc")
    }

    fn op(array: &str, ms: u64) -> PendingArrayOp {
        PendingArrayOp {
            array: array.into(),
            hlc: hlc(ms),
            op_payload: Vec::new(),
            seq: 0,
        }
    }

    fn reject(array: &str, at: Hlc, reason: ArrayRejectReason) -> ArrayRejectMsg {
        ArrayRejectMsg {
            array: array.into(),
            op_hlc_bytes: at.to_bytes(),
            reason,
            detail: "refused".into(),
        }
    }

    /// Send ops at `ms` in one window and return its token.
    fn send_window(w: &mut ArrayPushWindow, ms: &[u64], now: Instant) -> u64 {
        for &m in ms {
            w.advance(hlc(m));
            w.record_sent(hlc(m));
        }
        w.open_barrier(now)
    }

    #[test]
    fn open_window_blocks_the_next_drain_until_its_pong() {
        let mut w = ArrayPushWindow::default();
        let now = Instant::now();
        assert_eq!(w.begin_tick(now), TickPlan::Drain { after: None });
        let token = send_window(&mut w, &[10, 20], now);
        assert_eq!(w.begin_tick(now), TickPlan::Wait);

        assert_eq!(w.complete_barrier(token ^ 1), None, "foreign pong ignored");
        let outcome = w.complete_barrier(token).expect("window closes");
        assert_eq!(outcome.confirmed, vec![hlc(10), hlc(20)]);
        assert_eq!(
            w.begin_tick(now),
            TickPlan::Drain {
                after: Some(hlc(20))
            }
        );
    }

    #[test]
    fn barrier_tokens_never_match_wall_clock_pings() {
        let mut w = ArrayPushWindow::default();
        let token = w.open_barrier(Instant::now());
        assert_ne!(token & BARRIER_TOKEN_TAG, 0);
        assert_eq!(w.complete_barrier(1_760_000_000_000), None);
    }

    #[test]
    fn rejected_ops_are_not_confirmed() {
        let mut w = ArrayPushWindow::default();
        let now = Instant::now();
        let token = send_window(&mut w, &[10, 20, 30], now);
        w.record_reject(&reject("a", hlc(20), ArrayRejectReason::ShapeInvalid));
        let outcome = w.complete_barrier(token).expect("window closes");
        assert_eq!(outcome.confirmed, vec![hlc(10), hlc(30)]);
    }

    #[test]
    fn array_unknown_reannounces_and_rewinds() {
        let mut w = ArrayPushWindow::default();
        let now = Instant::now();
        assert_eq!(w.announce_step("a"), AnnounceStep::Send);
        w.record_announced("a", hlc(1));
        assert_eq!(w.announce_step("a"), AnnounceStep::Ready);

        let token = send_window(&mut w, &[10], now);
        w.record_reject(&reject("a", hlc(10), ArrayRejectReason::ArrayUnknown));
        let outcome = w.complete_barrier(token).expect("window closes");
        assert!(outcome.confirmed.is_empty());
        assert_eq!(w.begin_tick(now), TickPlan::Drain { after: None });
        assert!(!w.is_held(&op("a", 10)), "the op is re-sent");
        assert_eq!(w.announce_step("a"), AnnounceStep::Send);
    }

    #[test]
    fn repeated_schema_rejects_park_the_array() {
        let mut w = ArrayPushWindow::default();
        for _ in 0..MAX_SCHEMA_ANNOUNCES {
            assert_eq!(w.announce_step("a"), AnnounceStep::Send);
            w.record_announced("a", hlc(1));
            w.record_reject(&reject("a", hlc(10), ArrayRejectReason::ArrayUnknown));
        }
        assert_eq!(w.announce_step("a"), AnnounceStep::GiveUp);
        assert!(w.is_held(&op("a", 10)));
    }

    #[test]
    fn refused_schema_announce_parks_the_array() {
        let mut w = ArrayPushWindow::default();
        w.record_announced("a", hlc(1));
        w.record_reject(&reject("a", hlc(1), ArrayRejectReason::EngineRejected));
        assert!(w.is_held(&op("a", 10)));
        assert!(!w.is_held(&op("b", 10)));
        assert_eq!(w.announce_step("a"), AnnounceStep::GiveUp);
    }

    #[test]
    fn engine_rejected_op_is_parked_for_the_session() {
        let mut w = ArrayPushWindow::default();
        let now = Instant::now();
        let token = send_window(&mut w, &[10, 20], now);
        w.record_reject(&reject("a", hlc(10), ArrayRejectReason::EngineRejected));
        let outcome = w.complete_barrier(token).expect("window closes");
        assert_eq!(outcome.confirmed, vec![hlc(20)]);
        assert!(w.is_held(&op("a", 10)));

        w.reset();
        assert!(!w.is_held(&op("a", 10)), "a new session retries it");
    }

    #[test]
    fn unattributed_reject_holds_the_whole_window() {
        let mut w = ArrayPushWindow::default();
        let now = Instant::now();
        let token = send_window(&mut w, &[10, 20], now);
        w.record_reject(&reject("", Hlc::ZERO, ArrayRejectReason::EngineRejected));
        let outcome = w.complete_barrier(token).expect("window closes");
        assert!(outcome.confirmed.is_empty());
        assert_eq!(outcome.parked_unattributed, 2);
        assert!(w.is_held(&op("a", 10)) && w.is_held(&op("a", 20)));
    }

    #[test]
    fn retention_floor_reject_leaves_the_window_untouched() {
        let mut w = ArrayPushWindow::default();
        let now = Instant::now();
        let token = send_window(&mut w, &[10], now);
        w.record_reject(&reject("a", Hlc::ZERO, ArrayRejectReason::RetentionFloor));
        let outcome = w.complete_barrier(token).expect("window closes");
        assert_eq!(outcome.confirmed, vec![hlc(10)]);
    }

    #[test]
    fn timed_out_window_is_abandoned_and_rewound() {
        let mut w = ArrayPushWindow::default();
        let start = Instant::now();
        let token = send_window(&mut w, &[10], start);
        let later = start + ARRAY_BARRIER_TIMEOUT;
        assert_eq!(w.begin_tick(later), TickPlan::Drain { after: None });
        assert_eq!(
            w.complete_barrier(token),
            None,
            "a late pong confirms nothing"
        );
    }

    #[test]
    fn idle_tick_applies_a_requested_rewind() {
        let mut w = ArrayPushWindow::default();
        w.advance(hlc(10));
        w.request_rewind();
        w.finish_idle_tick();
        assert_eq!(
            w.begin_tick(Instant::now()),
            TickPlan::Drain { after: None }
        );
    }
}
