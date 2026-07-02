// SPDX-License-Identifier: Apache-2.0
pub(crate) mod install;
pub(crate) mod persist;

pub(crate) use install::{ensure_sidecar, install_sidecar_for_index};
pub(crate) use persist::{persist_sidecar, try_restore_sidecar};
