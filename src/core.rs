//! The transport core: pump signed link frames between this node's lane flow and a set of
//! WebSocket connections, one connection per far peer.
//!
//! Trust model (see `ce-node/src/link.rs`): the ws wire is UNTRUSTED media and this process
//! is a dumb byte mover — every frame inside is a LinkEnvelope signed end-to-end and verified
//! by the destination NODE, so the worst a broken wire (or a broken us) can do is drop or
//! replay traffic, which the node survives (libp2p fallback + anti-replay window). The one
//! adapter-level check is the HELLO allow list: each side's first ws message is its raw
//! 32-byte NodeId, and we only register peers named in `CE_WS_PEERS` — an UNAUTHENTICATED
//! claim (a liar can black-hole or observe frames for the id it claims until signatures give
//! it away), which is why peers are static operator config in v1 and why confidential
//! payloads belong on wss.

use anyhow::{Context, Result, bail};
use ce_lane::transport::{NodeToTransport, TransportToNode};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

pub type NodeId = [u8; 32];

#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Accept inbound ws connections here (`ip:port`), e.g. the relay side.
    pub listen: Option<String>,
    /// Dial these ws URLs (`ws://host:port` / `wss://...`), e.g. the laptop side.
    pub dial: Vec<String>,
    /// The ONLY peers this transport will register (static operator config, fail closed).
    pub peers: Vec<NodeId>,
}

/// Live connections by far peer. `gen` disambiguates reconnects: a superseded connection's
/// death must not tear down its replacement's registration.
#[derive(Default)]
struct Conns {
    map: HashMap<NodeId, (u64, tokio::sync::mpsc::Sender<Vec<u8>>)>,
    next_gen: u64,
}

type SharedConns = Arc<Mutex<Conns>>;
type ToNode = std::sync::mpsc::Sender<TransportToNode>;

/// Drive the transport until the lane flow dies (node shutdown or revocation kill) — the
/// binary treats that as fatal and lets the supervisor restart it.
pub async fn run(ep: ce_lane::Endpoint, slot_size: u32, cfg: Config) -> Result<()> {
    if cfg.listen.is_none() && cfg.dial.is_empty() {
        bail!("nothing to do: set CE_WS_LISTEN and/or CE_WS_DIAL");
    }
    if cfg.peers.is_empty() {
        bail!("CE_WS_PEERS is required: this transport only registers operator-named peers");
    }

    let ep = Arc::new(ep);
    let conns: SharedConns = Arc::new(Mutex::new(Conns::default()));

    // Single producer toward the node: every task funnels through this channel; one thread
    // owns the ring's tx side (SPSC discipline).
    let (to_node, from_tasks) = std::sync::mpsc::channel::<TransportToNode>();
    let writer_ep = ep.clone();
    // Margin for the bincode enum/len framing around the payload.
    let max_frame = slot_size.saturating_sub(128) as usize;
    std::thread::Builder::new()
        .name("ws-node-writer".into())
        .spawn(move || {
            loop {
                match from_tasks.recv_timeout(Duration::from_secs(1)) {
                    Ok(msg) => {
                        let bytes = msg.encode();
                        if bytes.len() > max_frame {
                            warn!("dropping {}-byte frame exceeding the lane slot", bytes.len());
                            continue;
                        }
                        if writer_ep.send(&bytes).is_err() {
                            break;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if writer_ep.is_closed() {
                            break;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            writer_ep.close();
        })
        .context("spawn node writer")?;

    // Sole ring consumer: the node's Hello first, then outbound frames routed to the right
    // ws connection by the address the node stamped (we never decode the envelope).
    let (hello_tx, hello_rx) = tokio::sync::oneshot::channel::<NodeId>();
    let (dead_tx, dead_rx) = tokio::sync::oneshot::channel::<()>();
    let reader_ep = ep.clone();
    let conns_r = conns.clone();
    std::thread::Builder::new()
        .name("ws-node-reader".into())
        .spawn(move || {
            let mut hello_tx = Some(hello_tx);
            let mut buf = Vec::new();
            loop {
                match reader_ep.recv_into(&mut buf, Some(Duration::from_secs(3600))) {
                    Ok(()) => match NodeToTransport::decode(&buf) {
                        Ok(NodeToTransport::Hello { node_id }) => {
                            if let Some(tx) = hello_tx.take() {
                                let _ = tx.send(node_id);
                            }
                        }
                        Ok(NodeToTransport::Frame { to, frame }) => {
                            let sender = conns_r
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .map
                                .get(&to)
                                .map(|(_, s)| s.clone());
                            match sender {
                                // A full conn queue sheds the frame; the node's pending call
                                // times out and falls back to libp2p (fail open).
                                Some(s) => match s.try_send(frame) {
                                    Ok(()) => debug!("frame -> {} queued", hex::encode(&to[..4])),
                                    Err(e) => {
                                        warn!("frame -> {} SHED ({e})", hex::encode(&to[..4]))
                                    }
                                },
                                None => {
                                    debug!("no live connection for {}", hex::encode(&to[..4]))
                                }
                            }
                        }
                        Err(e) => {
                            warn!("node flow: {e:#} — stopping");
                            break;
                        }
                    },
                    Err(ce_lane::RecvErr::Closed) => break,
                    Err(ce_lane::RecvErr::TimedOut) => {}
                }
            }
            let _ = dead_tx.send(());
        })
        .context("spawn node reader")?;

    let my_id = hello_rx.await.context("node hello never arrived")?;
    info!("transport up for node {}", hex::encode(&my_id[..8]));

    // Track every serving task so a dying lane flow tears the ws side down with it (the
    // binary would exit anyway; embedders and tests need the teardown to be structural).
    let mut serving = Vec::new();
    if let Some(listen) = cfg.listen.clone() {
        let (conns, to_node, peers) = (conns.clone(), to_node.clone(), cfg.peers.clone());
        serving.push(tokio::spawn(async move {
            if let Err(e) = listen_loop(&listen, my_id, peers, conns, to_node).await {
                warn!("listener failed: {e:#}");
            }
        }));
    }
    for url in cfg.dial.clone() {
        let (conns, to_node, peers) = (conns.clone(), to_node.clone(), cfg.peers.clone());
        serving.push(tokio::spawn(async move { dial_loop(&url, my_id, peers, conns, to_node).await }));
    }

    // Run until the lane flow dies.
    let _ = dead_rx.await;
    for task in serving {
        task.abort();
    }
    bail!("lane flow closed (node gone or flow revoked)")
}

async fn listen_loop(
    listen: &str,
    my_id: NodeId,
    peers: Vec<NodeId>,
    conns: SharedConns,
    to_node: ToNode,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind {listen}"))?;
    info!("listening for peer links on {listen}");
    // Children live in a JoinSet so aborting the listener aborts every accepted connection
    // with it (drop = abort-all).
    let mut children = tokio::task::JoinSet::new();
    loop {
        let (stream, from) = listener.accept().await.context("accept")?;
        let (peers, conns, to_node) = (peers.clone(), conns.clone(), to_node.clone());
        children.spawn(async move {
            let ws = match tokio_tungstenite::accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    debug!("ws handshake from {from} failed: {e}");
                    return;
                }
            };
            if let Err(e) = serve_conn(ws, my_id, &peers, conns, to_node).await {
                debug!("connection from {from} ended: {e:#}");
            }
        });
        while children.try_join_next().is_some() {} // reap finished, never block
    }
}

async fn dial_loop(url: &str, my_id: NodeId, peers: Vec<NodeId>, conns: SharedConns, to_node: ToNode) {
    let mut backoff = Duration::from_secs(1);
    loop {
        match tokio_tungstenite::connect_async(url).await {
            Ok((ws, _resp)) => {
                info!("dialed {url}");
                backoff = Duration::from_secs(1); // a completed handshake resets the backoff
                if let Err(e) = serve_conn(ws, my_id, &peers, conns.clone(), to_node.clone()).await
                {
                    warn!("link to {url} ended: {e:#}");
                }
            }
            Err(e) => debug!("dial {url}: {e}"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

/// One established ws connection: exchange raw-NodeId hellos, gate on the allow list,
/// register the peer with the node (`Up`), pump frames both ways, deregister on death
/// (`Down`) unless a reconnect superseded us.
async fn serve_conn<S>(
    ws: tokio_tungstenite::WebSocketStream<S>,
    my_id: NodeId,
    allowed: &[NodeId],
    conns: SharedConns,
    to_node: ToNode,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut sink, mut stream) = ws.split();
    sink.send(Message::Binary(my_id.to_vec())).await.context("send hello")?;
    let far: NodeId = match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
        Ok(Some(Ok(Message::Binary(b)))) if b.len() == 32 => {
            b[..].try_into().expect("length checked")
        }
        Ok(Some(Ok(_))) | Ok(Some(Err(_))) | Ok(None) => bail!("peer hello malformed or missing"),
        Err(_) => bail!("peer hello timed out"),
    };
    if far == my_id {
        bail!("peer claims OUR id — refusing");
    }
    if !allowed.contains(&far) {
        bail!("peer {} is not in CE_WS_PEERS — refusing", hex::encode(&far[..8]));
    }

    let (conn_tx, mut conn_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);
    let generation = {
        let mut c = conns.lock().unwrap_or_else(|p| p.into_inner());
        let generation = c.next_gen + 1;
        c.next_gen = generation;
        c.map.insert(far, (generation, conn_tx));
        generation
    };
    to_node.send(TransportToNode::Up { peers: vec![far] }).context("node writer gone")?;
    info!("link UP: {}", hex::encode(&far[..8]));

    let outbound = async {
        while let Some(frame) = conn_rx.recv().await {
            let n = frame.len();
            sink.send(Message::Binary(frame)).await.context("ws send")?;
            debug!("{n}B -> wire ({})", hex::encode(&far[..4]));
        }
        Ok::<_, anyhow::Error>(())
    };
    let to_node_in = to_node.clone();
    let inbound = async {
        while let Some(msg) = stream.next().await {
            match msg.context("ws recv")? {
                Message::Binary(frame) => {
                    // Opaque signed bytes; the NODE verifies. We only carry.
                    debug!("{}B <- wire ({})", frame.len(), hex::encode(&far[..4]));
                    to_node_in
                        .send(TransportToNode::Frame { frame: frame.to_vec() })
                        .context("node writer gone")?;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    let result = tokio::select! {
        r = outbound => r,
        r = inbound => r,
    };

    let still_ours = {
        let mut c = conns.lock().unwrap_or_else(|p| p.into_inner());
        if c.map.get(&far).map(|(g, _)| *g) == Some(generation) {
            c.map.remove(&far);
            true
        } else {
            false
        }
    };
    if still_ours {
        let _ = to_node.send(TransportToNode::Down { peers: vec![far] });
        info!("link DOWN: {}", hex::encode(&far[..8]));
    }
    result
}
