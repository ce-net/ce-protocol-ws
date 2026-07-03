//! ce-protocol-ws — the reference PROTOCOL adapter: carries the mesh's node<->node link
//! traffic over plain WebSocket connections, so two nodes reach each other on media libp2p
//! does not speak (and the same adapter shape later carries serial/ESP-NOW/ethernet — see
//! `PLAN/ce-protocol-adapters-and-embedded.md`, Workstream B).
//!
//! Both machines run this ceapp; every ceapp on both nodes gets the new path with zero code
//! changes. The node routes directed traffic to registered peers through us BEFORE libp2p
//! and falls back if the link dies — we are an accelerant/reach-extender, never a hard
//! dependency. Registration is HOST-GATED: the node refuses `TARGET_TRANSPORT` unless
//! membrane-policy `[transport]` names us (or a `transport` capability is presented).
//!
//! Config via env:
//!   CE_WS_LISTEN         accept peer links here, e.g. `0.0.0.0:4820` (the public side)
//!   CE_WS_DIAL           comma-separated ws URLs to dial, e.g. `ws://relay:4820` (the NAT side)
//!   CE_WS_PEERS          comma-separated hex NodeIds this transport may register (REQUIRED)
//!   CE_TRANSPORT_NAME    registered transport name (default `ce-protocol-ws`)
//!   CE_LANE_SOCK         node lane socket (default `<data dir>/lane.sock`)
//!   CE_API_TOKEN         node api token (default: read `<data dir>/api.token`)
//!   CE_TRANSPORT_CAPS    optional hex capability chain granting the `transport` ability

#[cfg(unix)]
use ce_protocol_ws::core;

#[cfg(unix)]
fn main() -> anyhow::Result<()> {
    use anyhow::Context;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let data_dir = directories::ProjectDirs::from("", "", "ce")
        .map(|d| d.data_dir().to_path_buf())
        .context("no home directory")?;
    let sock = std::env::var("CE_LANE_SOCK")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| data_dir.join(ce_lane::bind::LANE_SOCKET));
    let token = match std::env::var("CE_API_TOKEN") {
        Ok(t) if !t.trim().is_empty() => t.trim().to_string(),
        _ => std::fs::read_to_string(data_dir.join("api.token"))
            .context("no node api token (set CE_API_TOKEN or run beside a node)")?
            .trim()
            .to_string(),
    };
    let name =
        std::env::var("CE_TRANSPORT_NAME").unwrap_or_else(|_| "ce-protocol-ws".to_string());
    let caps = std::env::var("CE_TRANSPORT_CAPS").ok().filter(|c| !c.trim().is_empty());

    let cfg = core::Config {
        listen: std::env::var("CE_WS_LISTEN").ok().filter(|s| !s.is_empty()),
        dial: split_csv(&std::env::var("CE_WS_DIAL").unwrap_or_default()),
        peers: split_csv(&std::env::var("CE_WS_PEERS").unwrap_or_default())
            .iter()
            .map(|h| parse_node_id(h))
            .collect::<anyhow::Result<Vec<_>>>()?,
    };

    let (ep, ack) = ce_lane::transport::register_transport(&sock, &token, &name, caps)
        .context("registering as a transport (is this name in membrane-policy [transport]?)")?;
    tracing::info!("registered transport '{name}' (flow {}, slots {}x{})", ack.flow_id, ack.n_slots, ack.slot_size);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(core::run(ep, ack.slot_size, cfg))
}

#[cfg(unix)]
fn split_csv(s: &str) -> Vec<String> {
    s.split(',').map(str::trim).filter(|p| !p.is_empty()).map(str::to_string).collect()
}

#[cfg(unix)]
fn parse_node_id(hex_id: &str) -> anyhow::Result<[u8; 32]> {
    let bytes = hex::decode(hex_id)
        .map_err(|e| anyhow::anyhow!("CE_WS_PEERS entry '{hex_id}' is not hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("CE_WS_PEERS entry '{hex_id}' is not 32 bytes"))
}

#[cfg(not(unix))]
fn main() {
    eprintln!("ce-protocol-ws runs on unix hosts only (the lane transport is unix)");
    std::process::exit(1);
}
