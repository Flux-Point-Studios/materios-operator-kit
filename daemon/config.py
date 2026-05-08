import os
from dataclasses import dataclass


@dataclass
class DaemonConfig:
    rpc_url: str = "ws://materios-rpc.materios.svc.cluster.local:9944"
    ogmios_url: str = "http://materios-ogmios.materios.svc.cluster.local:1337"
    poll_interval: int = 12  # seconds
    signer_uri: str = "//Alice"
    chain_id: str = ""  # Materios genesis hash, set via CHAIN_ID env
    retention_days: int = 365
    data_dir: str = "/data"
    blob_base_url: str = ""  # locator registry base URL
    blob_local_dir: str = "/data/materios-blobs"
    cert_store_dir: str = "/data/certs"
    state_file: str = "/data/daemon-state.json"
    health_port: int = 8080
    discord_webhook_url: str = ""
    max_blob_fetch_retries: int = 3
    blob_fetch_timeout: int = 30  # seconds
    max_chunk_size: int = 64 * 1024 * 1024  # 64 MB
    pending_alert_seconds: int = 3600  # alert after 1 hour pending
    tx_max_retries: int = 3
    checkpoint_batch_size: int = 100
    checkpoint_interval: int = 60  # minutes
    cardano_anchor_url: str = ""  # anchor worker endpoint for L1 checkpointing
    anchor_worker_token: str = ""  # x-internal-token for anchor worker auth
    checkpoint_enabled: bool = True  # Only primary daemon should run L1 checkpointing
    finality_confirmations: int = 12  # ~72s at 6s blocks
    cardano_network_id: str = ""  # Cardano network genesis hash for checkpoint manifest
    cert_schema_version: str = "1.0"  # Schema version included in cert array
    locator_registry_api_key: str = ""  # API key for blob gateway locator lookups
    blob_gateway_url: str = ""  # Blob gateway URL for posting batch metadata
    blob_gateway_api_key: str = ""  # API key for blob gateway batch metadata posts
    poll_interval_fast: int = 3  # seconds, when pending work exists
    poll_interval_idle: int = 12  # seconds, when idle
    max_leaf_wait_seconds: int = 90  # flush if oldest leaf pending > this
    notify_token: str = ""  # X-Internal-Token for push-notify auth
    # Content validation (cv_ prefix)
    content_validation_enabled: bool = False
    cv_min_duration: float = 3.0        # seconds — minimum plausible run
    cv_max_speed: float = 30.0          # m/s — max ~28 m/s at difficulty 20
    cv_max_crystal_rate: float = 0.15   # crystals per meter of track
    cv_max_event_rate: float = 0.25     # (near_miss + slides) per meter
    cv_score_margin: float = 1.1        # 10% tolerance over theoretical max
    # Schema registry
    schema_registry_path: str = ""  # SCHEMA_REGISTRY_PATH — empty = use default (schemas/registry.json)
    # Heartbeat
    heartbeat_url: str = ""  # HEARTBEAT_URL — empty = disabled
    heartbeat_interval: int = 30  # HEARTBEAT_INTERVAL, seconds
    # Per-block parallelization (task #120)
    #
    # Cap on concurrent `process_receipt` coroutines per block. The prep
    # phase (locator fetch + blob download + Merkle verify) is HTTP-bound
    # and parallelizes well; the submit phase is serialized by a chain-write
    # lock to keep nonces monotonic. 8 is a conservative ceiling that avoids
    # hammering the gateway/RPC for nodes processing dozens of receipts in
    # one block. Tune via MAX_CONCURRENT_RECEIPTS env var.
    max_concurrent_receipts: int = 8
    # Substrate WS transport hardening (task #41)
    #
    # `ws_recv_timeout`: socket-level recv timeout in seconds, passed to
    # websocket.create_connection(timeout=N). Without this the underlying
    # `socket.recv()` blocks forever when the chain RPC silently stops
    # responding — observed wedges of 11+ hours on the 2026-05-08 incident.
    # 45s is generous enough for `submit_extrinsic(wait_for_inclusion=True)`
    # on a congested mempool / 1-2 block reorg (worst case ~24s) while
    # ensuring the daemon notices a dead socket inside one poll cycle. Was
    # 30s in the first cut of this fix — bumped after pre-merge security
    # review flagged that 30s could trigger premature reconnect on a slow
    # extrinsic submit (which compounds with the non-idempotent retry
    # concern, since fixed by routing extrinsic submits through
    # `_call_no_retry`).
    ws_recv_timeout: int = 45
    # `ws_connected_freshness`: how many seconds since the last successful
    # RPC the `connected` property still reports True. Decoupled from
    # `ws_recv_timeout` so that a single long call doesn't immediately
    # mark the WS down. Keep this >= 3× poll_interval so quiet ticks
    # don't toggle the metric. 90s ≈ 7.5 poll intervals at the default.
    ws_connected_freshness: int = 90

    @classmethod
    def from_env(cls) -> "DaemonConfig":
        return cls(
            rpc_url=os.environ.get("MATERIOS_RPC_URL", cls.rpc_url),
            ogmios_url=os.environ.get("OGMIOS_URL", cls.ogmios_url),
            poll_interval=int(os.environ.get("POLL_INTERVAL", cls.poll_interval)),
            signer_uri=os.environ.get("SIGNER_URI", cls.signer_uri),
            chain_id=os.environ.get("CHAIN_ID", cls.chain_id),
            retention_days=int(os.environ.get("RETENTION_DAYS", cls.retention_days)),
            data_dir=os.environ.get("DATA_DIR", cls.data_dir),
            blob_base_url=os.environ.get("LOCATOR_REGISTRY_URL", cls.blob_base_url),
            blob_local_dir=os.environ.get("BLOB_LOCAL_DIR", cls.blob_local_dir),
            cert_store_dir=os.environ.get("CERT_STORE_DIR", cls.cert_store_dir),
            state_file=os.environ.get("STATE_FILE", cls.state_file),
            health_port=int(os.environ.get("HEALTH_PORT", cls.health_port)),
            discord_webhook_url=os.environ.get("DISCORD_WEBHOOK_URL", cls.discord_webhook_url),
            max_blob_fetch_retries=int(os.environ.get("MAX_BLOB_FETCH_RETRIES", cls.max_blob_fetch_retries)),
            blob_fetch_timeout=int(os.environ.get("BLOB_FETCH_TIMEOUT", cls.blob_fetch_timeout)),
            max_chunk_size=int(os.environ.get("MAX_CHUNK_SIZE", cls.max_chunk_size)),
            pending_alert_seconds=int(os.environ.get("PENDING_ALERT_SECONDS", cls.pending_alert_seconds)),
            tx_max_retries=int(os.environ.get("TX_MAX_RETRIES", cls.tx_max_retries)),
            checkpoint_batch_size=int(os.environ.get("CHECKPOINT_BATCH_SIZE", cls.checkpoint_batch_size)),
            checkpoint_interval=int(os.environ.get("CHECKPOINT_INTERVAL", cls.checkpoint_interval)),
            cardano_anchor_url=os.environ.get("CARDANO_ANCHOR_URL", cls.cardano_anchor_url),
            anchor_worker_token=os.environ.get("ANCHOR_WORKER_TOKEN", cls.anchor_worker_token),
            checkpoint_enabled=os.environ.get("CHECKPOINT_ENABLED", "true").lower() in ("true", "1", "yes"),
            finality_confirmations=int(os.environ.get("FINALITY_CONFIRMATIONS", cls.finality_confirmations)),
            cardano_network_id=os.environ.get("CARDANO_NETWORK_ID", cls.cardano_network_id),
            cert_schema_version=os.environ.get("CERT_SCHEMA_VERSION", cls.cert_schema_version),
            locator_registry_api_key=os.environ.get("LOCATOR_REGISTRY_API_KEY", cls.locator_registry_api_key),
            blob_gateway_url=os.environ.get("BLOB_GATEWAY_URL", cls.blob_gateway_url),
            blob_gateway_api_key=os.environ.get("BLOB_GATEWAY_API_KEY", cls.blob_gateway_api_key),
            poll_interval_fast=int(os.environ.get("POLL_INTERVAL_FAST", cls.poll_interval_fast)),
            poll_interval_idle=int(os.environ.get("POLL_INTERVAL_IDLE", cls.poll_interval_idle)),
            max_leaf_wait_seconds=int(os.environ.get("MAX_LEAF_WAIT_SECONDS", cls.max_leaf_wait_seconds)),
            notify_token=os.environ.get("NOTIFY_TOKEN", cls.notify_token),
            content_validation_enabled=os.environ.get("CONTENT_VALIDATION_ENABLED", "false").lower() in ("true", "1", "yes"),
            cv_min_duration=float(os.environ.get("CV_MIN_DURATION", cls.cv_min_duration)),
            cv_max_speed=float(os.environ.get("CV_MAX_SPEED", cls.cv_max_speed)),
            cv_max_crystal_rate=float(os.environ.get("CV_MAX_CRYSTAL_RATE", cls.cv_max_crystal_rate)),
            cv_max_event_rate=float(os.environ.get("CV_MAX_EVENT_RATE", cls.cv_max_event_rate)),
            cv_score_margin=float(os.environ.get("CV_SCORE_MARGIN", cls.cv_score_margin)),
            schema_registry_path=os.environ.get("SCHEMA_REGISTRY_PATH", cls.schema_registry_path),
            heartbeat_url=os.environ.get("HEARTBEAT_URL", cls.heartbeat_url),
            heartbeat_interval=int(os.environ.get("HEARTBEAT_INTERVAL", cls.heartbeat_interval)),
            max_concurrent_receipts=max(
                1, int(os.environ.get("MAX_CONCURRENT_RECEIPTS", cls.max_concurrent_receipts))
            ),
            ws_recv_timeout=max(
                5, int(os.environ.get("WS_RECV_TIMEOUT", cls.ws_recv_timeout))
            ),
            ws_connected_freshness=max(
                30, int(os.environ.get("WS_CONNECTED_FRESHNESS", cls.ws_connected_freshness))
            ),
        )
