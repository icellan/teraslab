# TeraSlab Deployment Assumptions

This document captures the trust assumptions TeraSlab makes about its
operating environment. Operators MUST satisfy these before exposing a
deployment to traffic — TeraSlab will *not* try to compensate for an
untrusted network at runtime.

## 1. Trusted-overlay model for the TCP data port (default 3300)

The binary wire protocol on `listen_addr` (default `127.0.0.1:3300`) is
designed for a **trusted private network**. Until the mTLS wave lands the
data port has the following properties:

- **No per-connection authentication.** Any TCP peer that can reach the
  port can issue every opcode (read, write, mutate, admin).
- **No transport encryption.** Frames travel in cleartext; an on-path
  attacker can sniff and forge.
- **Cluster membership / topology / migration / replication frames** are
  authenticated by a shared `cluster_secret` (HMAC-SHA256) when configured.
  When the secret is missing, those frames are accepted unsigned.

This is intentional for single-node demos and bench rigs — they fail
open so the first-run experience does not require ceremony. For
production deployments TeraSlab assumes:

- The TCP data port is **only reachable from trusted peers** — your own
  Teranode node(s), a private subnet, a service mesh sidecar, or
  loopback only.
- A network-layer access control (security group, firewall, K8s
  NetworkPolicy, WireGuard / Tailscale, mTLS sidecar) gates the port.
- The `cluster_secret` is set on **every** node when running multi-node
  (RF > 1 OR `node_id > 0`). All nodes must use the *same* secret; the
  HMAC pre-flight rejects any peer that cannot reproduce it.

`validate_safe_defaults` enforces two safety rails by default:

1. Non-loopback binds (`0.0.0.0:*`, `192.168.*`, etc.) require
   `enable_remote_bind = true`. Without this explicit opt-in the daemon
   refuses to start when `listen_addr` resolves to anything other than
   loopback.
2. When `enable_admin_endpoints = true`, an `admin_token` is mandatory.
   No empty / missing token allowed; the HTTP middleware uses
   constant-time compare (`subtle`) against the bearer header.

### Hard mode: `--strict-auth`

The daemon supports a hard-mode toggle for production:

```bash
teraslab-server --config /etc/teraslab/config.toml --strict-auth
```

or in TOML:

```toml
strict_auth = true
```

With `strict_auth = true`, **multi-node configurations without a
`cluster_secret` refuse to start** with
`ConfigError::StrictAuthRequiresSecret`. The default (`strict_auth =
false`) downgrades the same condition to a prominent
`tracing::warn!(target = "teraslab::security", ...)` at boot, so single-
node demos work without ceremony but operators always see the missing-
secret state in their log aggregator.

`cluster_secret` length: when set, must be ≥ 16 bytes
(`ServerConfig::MIN_CLUSTER_SECRET_LEN`). Pre-fix a single-byte secret
passed validation, which made the HMAC trivially forgeable; the gate is
now enforced regardless of `strict_auth`.

## 2. HTTP observability port (default 9100)

The HTTP server on `http_listen_addr` (default `127.0.0.1:9100`) exposes:

- `/metrics` — Prometheus scrape target. Always public on the bound
  interface, even with admin endpoints disabled.
- `/health/live`, `/health/ready`, `/status` — Read-only health/readiness.
- `/admin/*` (mutating) — quiesce, drain, rebalance, log level.
  **Off by default** (`enable_admin_endpoints = false`).
- `/debug/*` — Record / index / redo introspection.
  Mutating debug endpoints are gated by the admin token; read-only ones
  (e.g. `GET /debug/log-level`, `/debug/freelist`) are public.

Recommendations:

- Bind to loopback or a private interface; never expose to the public
  internet. Same trust-overlay assumption as the TCP data port.
- When operating remotely, set:
  - `enable_remote_bind = true` (explicit opt-in for non-loopback binds);
  - `enable_admin_endpoints = true`;
  - `admin_token = "<≥16 random bytes, base64 / hex from `openssl rand`>"`.
    With both remote bind and admin endpoints on, the token must be at
    least 16 bytes (`MIN_REMOTE_ADMIN_TOKEN_LEN`) — a short token over
    the public internet is brute-forceable in milliseconds.
- Either way, run behind a reverse proxy / load balancer that terminates
  TLS and enforces per-route policy beyond what the daemon provides.

The CLI sends the bearer header via `--admin-token <s>` or
`TERASLAB_ADMIN_TOKEN=<s>` (env wins). Read-only endpoints work with or
without the header.

## 3. No encryption-at-rest for blobs (audit F-G9-009)

External blob storage (`blobstore_path`, default `./teraslab-blobstore`)
is intended for large transaction cold data that does not fit in a fixed
record. Blob files are written in plaintext; TeraSlab does NOT encrypt
them at rest.

If your deployment requires at-rest encryption, run on top of:

- **LUKS / dm-crypt** on the filesystem hosting `blobstore_path` and the
  device files;
- **AWS EBS encryption**, **GCE PD encryption**, **Azure disk
  encryption**, or equivalent;
- A POSIX FUSE encryption layer (gocryptfs, EncFS, etc.) — slower but
  portable.

The daemon does not validate that the underlying filesystem provides
encryption; that is the operator's responsibility. Encryption-at-rest
inside the daemon is tracked as a future feature; until then this is a
**deployment-level concern**, not a daemon-level one.

## 4. Process privilege and filesystem layout

- TeraSlab opens device files with `O_DIRECT` on Linux. On filesystems
  that disallow `O_DIRECT` (some tmpfs, some FUSE filesystems) the
  daemon falls back via `sync_fallback`. The fallback is correct but
  slower; production deployments should target an XFS or ext4 mount
  with `O_DIRECT` support.
- Default paths are **relative** to the daemon's working directory:
  - `device_paths = ["teraslab-data.dat"]`
  - `index_snapshot_path = "teraslab-index.snap"`
  - `blobstore_path = "./teraslab-blobstore"`
  - `redo_log_path = None` → derived as `<device_paths[0]>.redo`
  - `cluster_state_path = None` → derived as `<device_paths[0]>.cluster`

  Pre-fix `blobstore_path` defaulted to `/blobstore`, which is
  unwritable for a non-root process; new deployments hit blob-store I/O
  errors only on the first oversized record write — far from the
  startup banner. The default is now `./teraslab-blobstore`.

- The daemon does NOT call `setuid` / `setgid`; run it as an unprivileged
  user. systemd / container manifests should pin the user explicitly.

## 5. Graceful shutdown

The daemon installs a SIGINT + SIGTERM handler via the `ctrlc` crate.
Sending `kill -TERM <pid>` (the default for container orchestrators)
triggers the cleanup chain in this order:

1. Stop cluster coordinator (SWIM gossip + topology epoch persist).
2. Join background tasks (checkpoint, blob_gc, lag_monitor) with a 5s
   timeout each. Tasks that don't exit within the timeout are left to be
   reaped on process exit (log line emitted).
3. Snapshot the primary index.
4. Persist the allocator freelist.
5. Flush the replication-intent tracker.
6. Flush the redo log buffer.
7. `device.sync()`.
8. Shutdown the OTLP span pipeline (5s flush timeout).

Pre-fix the SIGINT/SIGTERM handler was a stub and the cleanup chain ran
only on natural exit (which never happened because the accept loop
polled an unrelated atomic). `kill -9` of course still bypasses
everything — that's expected behaviour and recovery on next startup
handles it.

## 6. Audit references

The trusted-overlay model is documented end-to-end in the audit report:

- `_review/02_findings_G10.md` — F-G10-007 / F-G10-010 / F-G10-011 (secret
  hygiene), F-G10-008 (no outbound DNS probe at boot), F-X-001 (this
  page).
- `_review/02_findings_G5.md` — wire-protocol auth gate.
- `_review/02_findings_G6.md` — admin / debug endpoint gating.
- `_review/02_findings_G9.md` — F-G9-009 (encryption-at-rest absence).
