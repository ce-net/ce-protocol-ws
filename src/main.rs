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
//! Config via env, with a per-node file fallback so the ceapp install needs no stored env
//! (`<ce data dir>/protocol-ws.env`, KEY=VALUE lines, env wins over file):
//!   CE_WS_LISTEN         accept peer links here, e.g. `0.0.0.0:4820` (the public side)
//!   CE_WS_DIAL           comma-separated ws URLs to dial, e.g. `ws://relay:4820` (the NAT side)
//!   CE_WS_PEERS          comma-separated hex NodeIds this transport may register (REQUIRED)
//!   CE_TRANSPORT_NAME    registered transport name (default `ce-protocol-ws`)
//!   CE_LANE_SOCK         node lane socket (default `<data dir>/lane.sock`)
//!   CE_API_TOKEN         node api token (default: read `<data dir>/api.token`)
//!   CE_TRANSPORT_CAPS    optional hex capability chain granting the `transport` ability
//!   CE_WS_CONFIG         override the config-file path

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

    // Per-node config file (KEY=VALUE lines; env wins). This is what lets the appmgr
    // supervisor run us with a bare `ce app install ce-protocol-ws` and no stored env:
    // the operator writes the node's link config ONCE into the ce data dir.
    let cfg_path = std::env::var("CE_WS_CONFIG")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| data_dir.join("protocol-ws.env"));
    let file_cfg = load_env_file(&cfg_path);
    let var = |key: &str| -> Option<String> {
        std::env::var(key)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| file_cfg.get(key).cloned())
    };

    let sock = var("CE_LANE_SOCK")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| data_dir.join(ce_lane::bind::LANE_SOCKET));
    let token = match var("CE_API_TOKEN") {
        Some(t) => t.trim().to_string(),
        None => std::fs::read_to_string(data_dir.join("api.token"))
            .context("no node api token (set CE_API_TOKEN or run beside a node)")?
            .trim()
            .to_string(),
    };
    let name = var("CE_TRANSPORT_NAME").unwrap_or_else(|| "ce-protocol-ws".to_string());
    let caps = var("CE_TRANSPORT_CAPS");

    let cfg = core::Config {
        listen: var("CE_WS_LISTEN"),
        dial: split_csv(&var("CE_WS_DIAL").unwrap_or_default()),
        peers: split_csv(&var("CE_WS_PEERS").unwrap_or_default())
            .iter()
            .map(|h| parse_node_id(h))
            .collect::<anyhow::Result<Vec<_>>>()?,
    };
    if cfg.listen.is_none() && cfg.dial.is_empty() {
        anyhow::bail!(
            "no CE_WS_LISTEN and no CE_WS_DIAL — set env vars or write {} (KEY=VALUE lines)",
            cfg_path.display()
        );
    }

    let (ep, ack) = ce_lane::transport::register_transport(&sock, &token, &name, caps)
        .context("registering as a transport (is this name in membrane-policy [transport]?)")?;
    tracing::info!("registered transport '{name}' (flow {}, slots {}x{})", ack.flow_id, ack.n_slots, ack.slot_size);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(core::run(ep, ack.slot_size, cfg))
}

/// KEY=VALUE lines; `#` comments and blanks ignored. Missing file = empty config (env-only).
#[cfg(unix)]
fn load_env_file(path: &std::path::Path) -> std::collections::HashMap<String, String> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Default::default();
    };
    tracing::info!(path = %path.display(), "loaded link config file");
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| {
            l.split_once('=')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .filter(|(_, v)| !v.is_empty())
        .collect()
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

#[cfg(all(test, unix))]
mod tests {
    use super::load_env_file;

    #[test]
    fn env_file_parses_pairs_and_ignores_noise() {
        let dir = std::env::temp_dir().join(format!("ws-envfile-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("protocol-ws.env");
        std::fs::write(
            &path,
            "# link config\nCE_WS_LISTEN = 0.0.0.0:4820\n\nCE_WS_PEERS=aa,bb\nBROKEN LINE\nEMPTY=\n",
        )
        .unwrap();
        let m = load_env_file(&path);
        assert_eq!(m.get("CE_WS_LISTEN").unwrap(), "0.0.0.0:4820");
        assert_eq!(m.get("CE_WS_PEERS").unwrap(), "aa,bb");
        assert!(!m.contains_key("BROKEN LINE"));
        assert!(!m.contains_key("EMPTY"), "empty values must not shadow env defaults");
        assert!(load_env_file(&dir.join("missing")).is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }
}
