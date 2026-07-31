// SPDX-License-Identifier: BUSL-1.1

//! Minting Loro peer ids.
//!
//! A peer id names the producer of every operation this replica authors, and
//! two live replicas sharing one have their operations merged as replays of
//! each other — one replica's writes vanish. The id must therefore be unique
//! across every replica that will ever sync into the same Origin, with no
//! coordination available at the moment it is minted.
//!
//! It is drawn from the same cryptographically-seeded generator as the
//! instance's UUID rather than from anything the environment supplies. Device
//! identifiers, install ids, and hostnames all repeat in the field — cloned VM
//! images, restored device backups, emulator fleets — and each repeat is a
//! collision that the CRDT merge resolves by discarding data.

/// Loro treats a peer id of `0` as "unset" and reserves the top bit, so the
/// usable space is `1..=(2^63 - 1)`.
const PEER_ID_MASK: u64 = (1u64 << 63) - 1;

/// Mint a fresh peer id for a replica.
///
/// Never returns `0`, and never sets the bit Loro reserves.
pub fn mint_peer_id() -> u64 {
    // UUID v7's low bits are the generator's random tail, which is what makes
    // two instances minting in the same millisecond distinct.
    let uuid = nodedb_types::id_gen::uuid_v7();
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in uuid.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    match hash & PEER_ID_MASK {
        0 => 1,
        id => id,
    }
}

/// Whether `peer_id` is usable as a Loro peer id.
///
/// Applied to ids read back from storage: a truncated or zeroed record must be
/// re-minted rather than handed to Loro, which would reject it or read it as
/// "unset" and author operations under an identity nothing owns.
pub fn is_valid_peer_id(peer_id: u64) -> bool {
    peer_id != 0 && peer_id & !PEER_ID_MASK == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn minted_ids_are_valid() {
        for _ in 0..1_000 {
            assert!(is_valid_peer_id(mint_peer_id()));
        }
    }

    #[test]
    fn minted_ids_do_not_repeat() {
        let ids: HashSet<u64> = (0..10_000).map(|_| mint_peer_id()).collect();
        assert_eq!(
            ids.len(),
            10_000,
            "a repeat within one process means two replicas can collide too"
        );
    }

    #[test]
    fn zero_and_reserved_bit_are_rejected() {
        assert!(!is_valid_peer_id(0));
        assert!(!is_valid_peer_id(1u64 << 63));
        assert!(!is_valid_peer_id(u64::MAX));
        assert!(is_valid_peer_id(1));
        assert!(is_valid_peer_id(PEER_ID_MASK));
    }
}
