#!/usr/bin/env python3
"""Print what a runtime's metadata makes the settle attestor and slash watcher
do, for the enactment preflight.

Usage: settlement-runtime.py METADATA
METADATA holds the 0x-hex RuntimeMetadataPrefixed (state_getMetadata, or
Runtime::metadata_at_version(14) from a native build), optionally gzipped.
Exits 1, with the reason on stderr, for a runtime the daemons refuse.
"""
import gzip
from pathlib import Path
import sys

from scalecodec.base import RuntimeConfigurationObject, ScaleBytes
from scalecodec.type_registry import load_type_registry_preset

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from daemon.substrate_client import settlement_contract  # noqa: E402

path = Path(sys.argv[1])
raw = path.read_bytes()
text = (gzip.decompress(raw) if path.suffix == ".gz" else raw).decode().strip()
try:
    config = RuntimeConfigurationObject()
    config.update_type_registry(load_type_registry_preset("core"))
    metadata = config.create_scale_object("MetadataVersioned", data=ScaleBytes(text))
    metadata.decode()
    contract = settlement_contract(metadata)
except Exception as error:  # noqa: BLE001
    sys.exit(f"refused: {type(error).__name__}: {error}")
print(" ".join(f"{key}={contract[key]}" for key in ("protocol", "supersede", "fingerprint", "ttl", "min_finality_depth")))
