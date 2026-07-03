//! The adapter proven over a REAL WebSocket on localhost: two transport cores (one listening,
//! one dialing), each driven by a fake "node end" (the other half of an in-test lane flow).
//! Asserts the full contract the node relies on: hello handshake, allow-list gate, `Up` on
//! connect, addressed outbound frames crossing the wire intact and opaque, and `Down` on
//! disconnect. The node side of the seam (routing, signatures, replay, fallback) is proven in
//! ce-node's `lane_transport`/`link_transport` tests — together they cover the whole path.
#![cfg(unix)]

use ce_lane::transport::{NodeToTransport, TransportToNode};
use ce_protocol_ws::core::{Config, run};
use std::time::Duration;

const ID_A: [u8; 32] = [0xAA; 32];
const ID_B: [u8; 32] = [0xBB; 32];
const ID_EVIL: [u8; 32] = [0xEE; 32];

/// A fake node end: allocate a lane flow, hand the client end to the adapter core, drive the
/// node end from the test (send Hello like `transport_pump` does, then observe/inject).
fn fake_node(id: [u8; 32], cfg: Config) -> ce_lane::Endpoint {
    let lane_cfg = ce_lane::LaneConfig { slot_size: 64 * 1024, n_slots: 16 };
    let (client_ep, node_ep, _view) = ce_lane::flow(lane_cfg).expect("flow");
    tokio::spawn(async move {
        if let Err(e) = run(client_ep, lane_cfg.slot_size, cfg).await {
            tracing::debug!("adapter core exited: {e:#}");
        }
    });
    node_ep.send(&NodeToTransport::Hello { node_id: id }.encode()).expect("hello");
    node_ep
}

/// Read transport->node messages until `pred` matches (or panic after `secs`).
fn expect_msg(
    ep: &ce_lane::Endpoint,
    secs: u64,
    pred: impl Fn(&TransportToNode) -> bool,
) -> TransportToNode {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let left = deadline
            .checked_duration_since(std::time::Instant::now())
            .expect("expected transport->node message before the deadline");
        let bytes = match ep.recv(Some(left)) {
            Ok(b) => b,
            Err(ce_lane::RecvErr::TimedOut) => continue,
            Err(ce_lane::RecvErr::Closed) => panic!("flow closed while waiting"),
        };
        let msg = TransportToNode::decode(&bytes).expect("well-formed transport frame");
        if pred(&msg) {
            return msg;
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

#[tokio::test(flavor = "multi_thread")]
async fn frames_cross_a_real_ws_link_with_up_down_lifecycle() {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).try_init();
    let port = free_port();

    // A listens; B dials. Each side's allow list names exactly the other.
    let node_a = fake_node(ID_A, Config {
        listen: Some(format!("127.0.0.1:{port}")),
        dial: vec![],
        peers: vec![ID_B],
    });
    let node_b = fake_node(ID_B, Config {
        listen: None,
        dial: vec![format!("ws://127.0.0.1:{port}")],
        peers: vec![ID_A],
    });

    // Both nodes learn the link (hello exchange -> Up).
    let up_a = tokio::task::block_in_place(|| {
        expect_msg(&node_a, 10, |m| matches!(m, TransportToNode::Up { .. }))
    });
    assert_eq!(up_a, TransportToNode::Up { peers: vec![ID_B] });
    let up_b = tokio::task::block_in_place(|| {
        expect_msg(&node_b, 10, |m| matches!(m, TransportToNode::Up { .. }))
    });
    assert_eq!(up_b, TransportToNode::Up { peers: vec![ID_A] });

    // Node A addresses an opaque frame to B; it must arrive at node B byte-identical (the
    // adapter never interprets it — these are not even valid envelopes).
    let payload = b"opaque-signed-envelope-bytes".to_vec();
    node_a
        .send(&NodeToTransport::Frame { to: ID_B, frame: payload.clone() }.encode())
        .expect("send");
    let got = tokio::task::block_in_place(|| {
        expect_msg(&node_b, 10, |m| matches!(m, TransportToNode::Frame { .. }))
    });
    assert_eq!(got, TransportToNode::Frame { frame: payload });

    // ...and the reverse direction.
    let back = b"reply-bytes".to_vec();
    node_b
        .send(&NodeToTransport::Frame { to: ID_A, frame: back.clone() }.encode())
        .expect("send");
    let got = tokio::task::block_in_place(|| {
        expect_msg(&node_a, 10, |m| matches!(m, TransportToNode::Frame { .. }))
    });
    assert_eq!(got, TransportToNode::Frame { frame: back });

    // Kill B's side entirely (its fake node flow closes -> its core exits -> ws drops):
    // A's node must see Down for B.
    node_b.close();
    let down_a = tokio::task::block_in_place(|| {
        expect_msg(&node_a, 15, |m| matches!(m, TransportToNode::Down { .. }))
    });
    assert_eq!(down_a, TransportToNode::Down { peers: vec![ID_B] });
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_outside_the_allow_list_is_never_registered() {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).try_init();
    let port = free_port();

    // A allows only B...
    let node_a = fake_node(ID_A, Config {
        listen: Some(format!("127.0.0.1:{port}")),
        dial: vec![],
        peers: vec![ID_B],
    });
    // ...but EVIL dials in, correctly speaking the protocol and honestly claiming its id.
    let _node_evil = fake_node(ID_EVIL, Config {
        listen: None,
        dial: vec![format!("ws://127.0.0.1:{port}")],
        peers: vec![ID_A],
    });

    // A's node must see NO Up within a generous window (the connection is refused at hello).
    let saw = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| {
            expect_msg(&node_a, 3, |m| matches!(m, TransportToNode::Up { .. }))
        })
    }));
    assert!(saw.is_err(), "an unlisted peer must never produce an Up registration");
}
