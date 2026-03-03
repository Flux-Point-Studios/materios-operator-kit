import json
import logging
import os
from typing import Optional
import aiohttp

from daemon.models import BlobManifest, ChunkInfo
from daemon.config import DaemonConfig

logger = logging.getLogger(__name__)


class LocatorRegistry:
    def __init__(self, config: DaemonConfig):
        self.config = config

    async def resolve(self, receipt_id: str) -> Optional[BlobManifest]:
        """Resolve a receipt_id to its blob manifest. Try HTTP first, then local filesystem."""
        manifest = await self._resolve_http(receipt_id)
        if manifest:
            return manifest
        return self._resolve_local(receipt_id)

    async def _resolve_http(self, receipt_id: str) -> Optional[BlobManifest]:
        if not self.config.blob_base_url:
            return None
        url = f"{self.config.blob_base_url}/locators/{receipt_id}"
        headers = {}
        api_key = os.environ.get("LOCATOR_REGISTRY_API_KEY", "") or getattr(self.config, "locator_registry_api_key", "")
        if api_key:
            headers["x-api-key"] = api_key
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10), headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self._parse_manifest(receipt_id, data)
                    elif resp.status == 404:
                        return None
                    else:
                        logger.warning(f"Locator registry returned {resp.status} for {receipt_id}")
                        return None
        except Exception as e:
            logger.warning(f"HTTP locator lookup failed for {receipt_id}: {e}")
            return None

    def _resolve_local(self, receipt_id: str) -> Optional[BlobManifest]:
        clean_id = receipt_id.removeprefix("0x")
        manifest_dir = os.path.join(self.config.blob_local_dir, clean_id)
        manifest_path = os.path.join(manifest_dir, "manifest.json")
        if not os.path.exists(manifest_path):
            return None
        try:
            with open(manifest_path) as f:
                data = json.load(f)
            return self._parse_manifest(receipt_id, data, manifest_dir=manifest_dir)
        except Exception as e:
            logger.warning(f"Failed to read local manifest for {receipt_id}: {e}")
            return None

    def _parse_manifest(self, receipt_id: str, data: dict, manifest_dir: str = "") -> BlobManifest:
        chunks = []
        for i, chunk_data in enumerate(data.get("chunks", [])):
            url = chunk_data.get("url", "")
            if not url and "path" in chunk_data:
                # Support SDK-generated manifests that use relative 'path' instead of 'url'
                rel_path = chunk_data["path"]
                abs_path = os.path.join(manifest_dir, rel_path) if manifest_dir else rel_path
                url = f"file://{abs_path}"
            chunks.append(ChunkInfo(
                index=i,
                url=url,
                sha256_hash=bytes.fromhex(chunk_data["sha256"]),
                size=chunk_data.get("size", 0),
            ))
        return BlobManifest(
            receipt_id=receipt_id,
            chunks=chunks,
            total_size=data.get("total_size", sum(c.size for c in chunks)),
        )
