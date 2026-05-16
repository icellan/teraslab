//! G10 config-validation regression tests.
//!
//! Each test below is the runtime witness for one F-G10-* fix. Failures
//! here regress a finding from `_review/02_findings_G10.md`.

use teraslab::config::{ConfigError, Secret, ServerConfig};

// ---------------------------------------------------------------------------
// F-G10-004: empty device_paths must be rejected at validation, not panic.
// ---------------------------------------------------------------------------

#[test]
fn validate_safe_defaults_rejects_empty_device_paths() {
    let cfg = ServerConfig {
        device_paths: vec![],
        ..ServerConfig::default()
    };
    let err = cfg
        .validate_safe_defaults()
        .expect_err("empty device_paths must be rejected with a typed error");
    assert!(
        matches!(err, ConfigError::NoDevicePaths),
        "expected NoDevicePaths, got {err:?}"
    );
}

#[test]
fn resolved_redo_log_path_does_not_panic_on_empty_device_paths() {
    // Even though validate_safe_defaults rejects this, the path resolver
    // is the line that previously indexed `[0]` and panicked. We assert
    // it returns the fallback default instead.
    let cfg = ServerConfig {
        device_paths: vec![],
        ..ServerConfig::default()
    };
    let path = cfg.resolved_redo_log_path();
    assert!(
        path.to_string_lossy().ends_with(".redo"),
        "fallback redo path must end in .redo, got {}",
        path.display(),
    );
}

// ---------------------------------------------------------------------------
// F-G10-005: range validation for sizes.
// ---------------------------------------------------------------------------

#[test]
fn validate_sizes_rejects_zero_device_alignment() {
    let cfg = ServerConfig {
        device_alignment: 0,
        ..ServerConfig::default()
    };
    let err = cfg
        .validate_sizes()
        .expect_err("device_alignment = 0 must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("device_alignment"), "msg = {msg}");
}

#[test]
fn validate_sizes_rejects_non_power_of_two_device_alignment() {
    let cfg = ServerConfig {
        device_alignment: 4097,
        ..ServerConfig::default()
    };
    let err = cfg.validate_sizes().unwrap_err();
    assert!(err.to_string().contains("device_alignment"));
}

#[test]
fn validate_sizes_rejects_non_power_of_two_lock_stripes() {
    let cfg = ServerConfig {
        lock_stripes: 1000,
        ..ServerConfig::default()
    };
    let err = cfg.validate_sizes().unwrap_err();
    assert!(err.to_string().contains("lock_stripes"));
}

#[test]
fn validate_sizes_rejects_zero_max_batch_size() {
    let cfg = ServerConfig {
        max_batch_size: 0,
        ..ServerConfig::default()
    };
    let err = cfg.validate_sizes().unwrap_err();
    assert!(err.to_string().contains("max_batch_size"));
}

#[test]
fn validate_sizes_rejects_zero_max_connections() {
    let cfg = ServerConfig {
        max_connections: 0,
        ..ServerConfig::default()
    };
    let err = cfg.validate_sizes().unwrap_err();
    assert!(err.to_string().contains("max_connections"));
}

#[test]
fn validate_sizes_accepts_defaults() {
    let cfg = ServerConfig::default();
    cfg.validate_sizes()
        .expect("default sizing config must pass validation");
}

// ---------------------------------------------------------------------------
// F-G10-006: blobstore_path default is a relative path that a non-root
// process can actually write.
// ---------------------------------------------------------------------------

#[test]
fn default_blobstore_path_is_relative() {
    let cfg = ServerConfig::default();
    let path = cfg.blobstore_path.to_string_lossy();
    assert!(
        path.starts_with("./") || !path.starts_with('/'),
        "default blobstore_path must be relative, got {path}",
    );
    assert!(
        !path.starts_with("/blobstore"),
        "pre-fix default `/blobstore` must NOT be the new default",
    );
}

// ---------------------------------------------------------------------------
// F-G10-007: Debug formatting of admin_token / cluster_secret must NOT
// leak the secret bytes.
// ---------------------------------------------------------------------------

#[test]
fn debug_format_of_secret_redacts_the_value() {
    let s = Secret::new("super-secret-cluster-key-1234");
    let rendered = format!("{s:?}");
    assert!(
        !rendered.contains("super-secret-cluster-key"),
        "Secret debug must not leak inner bytes, got {rendered}",
    );
    assert!(
        rendered.contains("redacted"),
        "Secret debug must mention redaction, got {rendered}",
    );
    assert!(
        rendered.contains("len="),
        "Secret debug must mention length, got {rendered}",
    );
}

#[test]
fn debug_format_of_server_config_redacts_admin_token() {
    let cfg = ServerConfig {
        admin_token: Some(Secret::new("supersecrettoken1234567890")),
        ..ServerConfig::default()
    };
    let rendered = format!("{cfg:?}");
    assert!(
        !rendered.contains("supersecrettoken"),
        "Debug must not leak admin_token, got {rendered}",
    );
}

#[test]
fn debug_format_of_server_config_redacts_cluster_secret() {
    let cfg = ServerConfig {
        cluster_secret: Some(Secret::new("supersecretclusterkey1234")),
        ..ServerConfig::default()
    };
    let rendered = format!("{cfg:?}");
    assert!(
        !rendered.contains("supersecretclusterkey"),
        "Debug must not leak cluster_secret, got {rendered}",
    );
}

// ---------------------------------------------------------------------------
// F-G10-010: admin_token strength when both admin endpoints and remote
// bind are on.
// ---------------------------------------------------------------------------

#[test]
fn weak_admin_token_with_remote_bind_is_rejected() {
    let toml = r#"
listen_addr = "192.168.1.10:3300"
http_listen_addr = "192.168.1.10:9100"
enable_remote_bind = true
enable_admin_endpoints = true
admin_token = "x"
"#;
    let cfg: ServerConfig = toml::from_str(toml).unwrap();
    let err = cfg.validate_safe_defaults().unwrap_err();
    match err {
        ConfigError::AdminTokenTooShort { actual, min } => {
            assert_eq!(actual, 1);
            assert_eq!(min, ServerConfig::MIN_REMOTE_ADMIN_TOKEN_LEN);
        }
        other => panic!("expected AdminTokenTooShort, got {other:?}"),
    }
}

#[test]
fn weak_admin_token_on_loopback_is_accepted() {
    // Loopback bind = trusted; weak tokens are operator-foot-shooting but
    // not a startup error.
    let cfg = ServerConfig {
        enable_admin_endpoints: true,
        admin_token: Some(Secret::new("x")),
        ..ServerConfig::default()
    };
    cfg.validate_safe_defaults()
        .expect("weak token on loopback is allowed (operator choice)");
}

// ---------------------------------------------------------------------------
// F-G10-011: cluster_secret strength when configured.
// ---------------------------------------------------------------------------

#[test]
fn short_cluster_secret_is_rejected() {
    let toml = r#"
node_id = 1
replication_factor = 2
cluster_secret = "short"
"#;
    let cfg: ServerConfig = toml::from_str(toml).unwrap();
    let err = cfg.validate_safe_defaults().unwrap_err();
    match err {
        ConfigError::ClusterSecretTooShort { actual, min } => {
            assert_eq!(actual, 5);
            assert_eq!(min, ServerConfig::MIN_CLUSTER_SECRET_LEN);
        }
        other => panic!("expected ClusterSecretTooShort, got {other:?}"),
    }
}

#[test]
fn long_cluster_secret_passes_validation() {
    let toml = r#"
node_id = 1
replication_factor = 2
cluster_secret = "this-is-a-long-enough-secret-from-openssl-rand"
"#;
    let cfg: ServerConfig = toml::from_str(toml).unwrap();
    cfg.validate_safe_defaults()
        .expect("a ≥16-byte cluster_secret must validate");
}

// ---------------------------------------------------------------------------
// F-G10-013: advertise_addr must be validated, not panic later.
// ---------------------------------------------------------------------------

#[test]
fn malformed_advertise_addr_is_a_typed_config_error() {
    let toml = r#"
advertise_addr = "definitely-not-a-socket-addr"
"#;
    let cfg: ServerConfig = toml::from_str(toml).unwrap();
    let err = cfg.validate_safe_defaults().unwrap_err();
    match err {
        ConfigError::InvalidAdvertiseAddr { addr, .. } => {
            assert_eq!(addr, "definitely-not-a-socket-addr");
        }
        other => panic!("expected InvalidAdvertiseAddr, got {other:?}"),
    }
}

#[test]
fn valid_advertise_addr_passes() {
    let toml = r#"
advertise_addr = "192.168.1.10:3300"
"#;
    let cfg: ServerConfig = toml::from_str(toml).unwrap();
    cfg.validate_safe_defaults()
        .expect("a valid advertise_addr must pass safe-defaults");
}

// ---------------------------------------------------------------------------
// F-X-001: strict_auth promotes multi-node-without-secret to a hard error.
// ---------------------------------------------------------------------------

#[test]
fn strict_auth_with_multi_node_and_no_secret_refuses_startup() {
    let cfg = ServerConfig {
        node_id: 1,
        replication_factor: 2,
        cluster_secret: None,
        strict_auth: true,
        ..ServerConfig::default()
    };
    let err = cfg.validate_safe_defaults().unwrap_err();
    assert!(
        matches!(err, ConfigError::StrictAuthRequiresSecret),
        "expected StrictAuthRequiresSecret, got {err:?}",
    );
}

#[test]
fn non_strict_with_multi_node_and_no_secret_is_accepted() {
    // The default trusted-overlay deployment model: warn at startup but
    // do not refuse. Validation must pass; the warn lives in the daemon's
    // main(), not in config-level validation.
    let cfg = ServerConfig {
        node_id: 1,
        replication_factor: 2,
        cluster_secret: None,
        strict_auth: false,
        ..ServerConfig::default()
    };
    cfg.validate_safe_defaults()
        .expect("non-strict mode must allow missing cluster_secret");
}

#[test]
fn strict_auth_with_single_node_no_secret_passes() {
    let cfg = ServerConfig {
        node_id: 0,
        replication_factor: 1,
        cluster_secret: None,
        strict_auth: true,
        ..ServerConfig::default()
    };
    cfg.validate_safe_defaults()
        .expect("strict_auth has no effect in true single-node mode");
}

#[test]
fn strict_auth_with_cluster_and_secret_passes() {
    let cfg = ServerConfig {
        node_id: 1,
        replication_factor: 2,
        cluster_secret: Some(Secret::new("openssl-rand-base64-24-bytes-ok")),
        strict_auth: true,
        ..ServerConfig::default()
    };
    cfg.validate_safe_defaults()
        .expect("strict_auth with a valid secret must validate");
}
