"""
Content validation gate for blob payloads.

Parses the blob data and runs game-specific plausibility checks.
If validation fails, the receipt is not certified — the score is
visible on-chain but ignored by leaderboard indexers.

Validators are keyed by schema version (the "v" field in the payload).
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Optional

from daemon.config import DaemonConfig

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    valid: bool = True
    errors: list[str] = field(default_factory=list)
    schema_version: Optional[int] = None
    payload: Optional[dict] = None


class ContentValidator:
    def __init__(self, config: DaemonConfig):
        self.config = config

    def validate(self, chunk_data_list: list[bytes], receipt_id: str) -> ValidationResult:
        """Validate blob content after integrity checks pass.

        Args:
            chunk_data_list: Raw bytes of each verified chunk (in order).
            receipt_id: For logging context.

        Returns:
            ValidationResult with valid=True if content is plausible.
        """
        result = ValidationResult()

        raw = b"".join(chunk_data_list)

        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            result.valid = False
            result.errors.append(f"Blob is not valid JSON: {e}")
            return result

        result.payload = payload

        version = payload.get("v")
        result.schema_version = version

        if version == 1:
            return self._validate_v1(payload, result, receipt_id)
        elif version is None:
            result.valid = False
            result.errors.append("Missing schema version field 'v'")
            return result
        else:
            logger.info(f"Unknown schema v={version} for {receipt_id[:16]}..., skipping content validation")
            return result

    def _validate_v1(self, p: dict, result: ValidationResult, receipt_id: str) -> ValidationResult:
        """Clay Monster Dash run_complete schema v1."""

        required = ["score", "dist", "crystals", "combo", "near_miss", "slides", "diff", "dur", "player"]
        for f in required:
            if f not in p:
                result.valid = False
                result.errors.append(f"Missing required field: {f}")
                return result

        try:
            score = int(p["score"])
            dist = float(p["dist"])
            crystals = int(p["crystals"])
            combo = int(p["combo"])
            near_miss = int(p["near_miss"])
            slides = int(p["slides"])
            diff = int(p["diff"])
            dur = float(p["dur"])
        except (ValueError, TypeError) as e:
            result.valid = False
            result.errors.append(f"Invalid field type: {e}")
            return result

        # Non-negative checks
        for fname, val in [("score", score), ("dist", dist), ("crystals", crystals),
                           ("near_miss", near_miss), ("slides", slides)]:
            if val < 0:
                result.valid = False
                result.errors.append(f"{fname} cannot be negative: {val}")
                return result

        # Minimum run duration
        if dur < self.config.cv_min_duration:
            result.valid = False
            result.errors.append(f"Run too short: {dur:.1f}s < {self.config.cv_min_duration}s")
            return result

        # Max speed: dist / dur
        speed = dist / dur
        if speed > self.config.cv_max_speed:
            result.valid = False
            result.errors.append(f"Speed implausible: {speed:.1f} m/s > {self.config.cv_max_speed} m/s")
            return result

        # Crystal density: crystals / dist
        if dist > 0:
            crystal_rate = crystals / dist
            if crystal_rate > self.config.cv_max_crystal_rate:
                result.valid = False
                result.errors.append(
                    f"Crystal rate implausible: {crystal_rate:.3f}/m > {self.config.cv_max_crystal_rate}/m"
                )
                return result

        # Obstacle event density: (near_miss + slides) / dist
        if dist > 0:
            event_rate = (near_miss + slides) / dist
            if event_rate > self.config.cv_max_event_rate:
                result.valid = False
                result.errors.append(
                    f"Event rate implausible: {event_rate:.3f}/m > {self.config.cv_max_event_rate}/m"
                )
                return result

        # Combo tier bounds
        if combo < 0 or combo > 10:
            result.valid = False
            result.errors.append(f"Combo out of range: {combo} not in [0, 10]")
            return result

        # Difficulty bounds
        if diff < 1 or diff > 20:
            result.valid = False
            result.errors.append(f"Difficulty out of range: {diff} not in [1, 20]")
            return result

        # Score cap with margin
        max_score = (dist * 10) + (crystals * 100) + ((near_miss * 50 + slides * 25) * 3.5)
        if score > max_score * self.config.cv_score_margin:
            result.valid = False
            result.errors.append(
                f"Score implausible: {score} > theoretical max {max_score:.0f} "
                f"(with {self.config.cv_score_margin}x margin)"
            )
            return result

        logger.info(
            f"Content valid for {receipt_id[:16]}...: "
            f"score={score} dist={dist:.0f}m dur={dur:.0f}s diff={diff} "
            f"speed={speed:.1f}m/s crystals={crystals}"
        )
        return result
