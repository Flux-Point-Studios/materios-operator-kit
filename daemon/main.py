import asyncio
import logging
import signal
import sys
import os

from daemon.config import DaemonConfig
from daemon.cert_daemon import CertDaemon
from daemon.health_server import start_health_server, set_notify_token

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def probe_ogmios(ogmios_url: str, timeout: float = 5.0) -> tuple[bool, str]:
    """Startup-time reachability probe for the configured Ogmios endpoint.

    Returns (ok, detail) where:
      ok=True  -> /health returned HTTP 200 with a `currentEpoch` (or a usable
                  `lastKnownTip.slot`). detail is a human-readable summary
                  ("epoch=285 network=preprod synced=1.0").
      ok=False -> any failure (NXDOMAIN, refused, timeout, non-200, malformed).
                  detail is the underlying error string.

    Used at boot to surface a clear ERROR (not a per-cycle WARNING) when the
    operator's OGMIOS_URL is misconfigured. Added in task-185 after stale k8s
    DNS defaults (`materios-ogmios.materios.svc.cluster.local`) silently
    polluted logs across all 4 daemons.
    """
    import requests as _requests
    try:
        resp = _requests.get(
            f"{ogmios_url.rstrip('/')}/health",
            headers={"Accept": "application/json"},
            timeout=timeout,
        )
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
        data = resp.json()
        epoch = data.get("currentEpoch")
        network = data.get("network", "unknown")
        sync = data.get("networkSynchronization", "?")
        if epoch is None:
            tip = data.get("lastKnownTip", {})
            if not tip.get("slot"):
                return False, "no currentEpoch and no lastKnownTip.slot"
        return True, f"epoch={epoch} network={network} synced={sync}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def main():
    config = DaemonConfig.from_env()

    # Startup-time Ogmios probe — log the configured URL and whether it
    # resolved. Failure is logged at ERROR (not the per-cycle WARNING we
    # used to spam) so operators see it once at boot and know to set
    # OGMIOS_URL. The daemon does NOT exit on probe failure — Cardano epoch
    # in cert metadata degrades to 0 but attestation still proceeds.
    logger.info(f"Configured OGMIOS_URL: {config.ogmios_url}")
    ok, detail = probe_ogmios(config.ogmios_url)
    if ok:
        logger.info(f"Ogmios reachable at boot: {detail}")
    else:
        logger.error(
            f"Ogmios UNREACHABLE at boot ({config.ogmios_url}): {detail}. "
            f"Cardano epoch will be 0 in cert metadata until reachable. "
            f"Set OGMIOS_URL to a working Ogmios HTTP endpoint "
            f"(e.g. http://192.168.0.133:1337 on the LAN, or "
            f"https://ogmios.saturnswap.io public)."
        )

    # Ensure data directories exist
    for d in [config.data_dir, config.cert_store_dir, config.blob_local_dir]:
        os.makedirs(d, exist_ok=True)

    # Configure push-notify auth token
    if config.notify_token:
        set_notify_token(config.notify_token)

    # Start health server
    health_srv = start_health_server(config.health_port)

    # Create daemon
    daemon = CertDaemon(config)

    # Start heartbeat sender (if configured)
    if config.heartbeat_url:
        from daemon.heartbeat import start_heartbeat_sender
        from daemon.health_server import _metrics, _metrics_lock
        start_heartbeat_sender(config, _metrics, _metrics_lock, daemon.client.keypair)

    # Signal handling
    loop = asyncio.new_event_loop()

    def shutdown(sig, frame):
        logger.info(f"Received {signal.Signals(sig).name}, shutting down...")
        daemon.stop()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        loop.run_until_complete(daemon.run())
    except KeyboardInterrupt:
        daemon.stop()
    finally:
        health_srv.shutdown()
        loop.close()
        logger.info("Daemon exited cleanly")


if __name__ == "__main__":
    main()
