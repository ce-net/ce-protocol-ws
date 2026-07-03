//! Library surface so the integration tests (and any embedder) can drive the transport core
//! directly with an in-test lane endpoint; the binary in `main.rs` is a thin env-config shell
//! over [`core::run`].

#[cfg(unix)]
pub mod core;
