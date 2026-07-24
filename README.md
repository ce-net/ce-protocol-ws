# ce-protocol-ws

The reference PROTOCOL adapter: it carries the CE mesh's node-to-node link traffic over plain
WebSocket connections, so two nodes reach each other on media libp2p does not speak. Both
machines run this ceapp; every app on both nodes gets the new path with zero code changes —
the transparency invariant applied to links. Protocol links are transport adapters: normal
ceapps that carry signed mesh frames over another medium, invisible to apps. The same adapter
shape later carries serial, ESP-NOW, and raw ethernet.

## How it works

- Registers with its node over the lane socket as a `transport`
  (`ce_lane::transport::register_transport`). The node REFUSES the registration unless the
  host's `membrane-policy.toml` names it under `[transport] allow = ["ce-protocol-ws"]` (or a
  capability chain granting the `transport` ability is presented) — becoming a transport means
  other nodes' traffic routes through this process, so it is host-composed, never self-claimed.
- Listens for and/or dials WebSocket connections. Each connection starts with a hello: each
  side's raw 32-byte NodeId. Only peers named in `CE_WS_PEERS` are registered (static operator
  config, fail closed).
- Declares each connected peer to the node (`Up`/`Down`); the node then routes directed
  requests and sends to that peer THROUGH this adapter before libp2p, falling back to the
  normal mesh path when the link is down. Frames are opaque here: every one is a
  `LinkEnvelope` signed end-to-end and verified (signature + anti-replay) by the destination
  node — this process can drop bytes, but never forge, alter, or impersonate.

## Security notes (v1 bounds, honest)

- The hello is an UNAUTHENTICATED claim gated by the `CE_WS_PEERS` allow list. A connecting
  liar claiming an allowed NodeId can black-hole or observe frames addressed to that id (the
  node's libp2p fallback and end-to-end signatures bound the damage to availability +
  wire-visibility). A node-signed hello is the planned fix.
- Frames are signed, not encrypted. On untrusted networks run the medium over `wss://` (TLS),
  or treat payloads as visible to the wire.

## Config (env, with a file fallback)

Installable-daemon path (B8): `ce app install ce-protocol-ws` runs the adapter under the
appmgr supervisor with no stored env — the adapter then reads `<ce data dir>/protocol-ws.env`
(KEY=VALUE lines, `#` comments; real env vars win over the file). Write that file once per
node (`CE_WS_LISTEN=...` on the public side, `CE_WS_DIAL=`/`CE_WS_PEERS=` on the NAT side)
and the standing link survives reboots. Override the path with `CE_WS_CONFIG`.


| Var | Meaning |
|---|---|
| `CE_WS_LISTEN` | Accept peer links here, e.g. `0.0.0.0:4820` (the public side) |
| `CE_WS_DIAL` | Comma-separated ws URLs to dial, e.g. `ws://relay.example:4820` (the NAT side) |
| `CE_WS_PEERS` | Comma-separated hex NodeIds this transport may register (REQUIRED) |
| `CE_TRANSPORT_NAME` | Registered transport name (default `ce-protocol-ws`) |
| `CE_LANE_SOCK` | Node lane socket (default `<data dir>/lane.sock`) |
| `CE_API_TOKEN` | Node api token (default: read `<data dir>/api.token`) |
| `CE_TRANSPORT_CAPS` | Optional hex capability chain granting the `transport` ability |

## Example: laptop behind NAT <-> public relay

On the relay (public side), with the laptop's node id allowed:

```bash
# /var/lib/ce/membrane-policy.toml:  [transport] allow = ["ce-protocol-ws"]
CE_WS_LISTEN=0.0.0.0:4820 CE_WS_PEERS=<laptop-node-id> ce-protocol-ws
```

On the laptop:

```bash
# ~/Library/Application Support/ce/membrane-policy.toml:  [transport] allow = ["ce-protocol-ws"]
CE_WS_DIAL=ws://relay.example:4820 CE_WS_PEERS=<relay-node-id> ce-protocol-ws
```

Any app's `request`/`send` between the two nodes now rides the ws link first, libp2p as
fallback — no app changes.

## Tests

`cargo test` runs the adapter over a real localhost WebSocket against fake node ends
(hello/Up/Down lifecycle, opaque frame transit, allow-list rejection). The node side of the
seam is proven in ce-node's `lane_transport` and `link_transport` integration tests.
