"""scripts/settlement-runtime.py: what a runtime's metadata makes the daemons sign,
checked before that runtime is enacted."""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
METADATA = ROOT / "tests/fixtures/custody/materios"


def check(path):
    return subprocess.run([sys.executable, "-B", str(ROOT / "scripts/settlement-runtime.py"), str(path)],
                          capture_output=True, text=True, cwd=ROOT, timeout=120)


def test_spec_242_signs_frv2_and_offers_supersede():
    result = check(METADATA / "metadata_spec242.hex.gz")
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [
        "protocol=FRV2", "supersede=True",
        "fingerprint=693ba3e8bc9106fb036f96bc73b3c7797d314c36ef95c0564c715ce58113eeb7",
        "ttl=2400", "min_finality_depth=15",
    ]


def test_spec_238_signs_frau_without_supersede():
    result = check(METADATA / "metadata_spec238.hex.gz")
    assert result.returncode == 0, result.stderr
    assert result.stdout.split()[:2] == ["protocol=FRAU", "supersede=False"]


def test_a_refused_runtime_exits_non_zero_and_says_why(tmp_path):
    bad = tmp_path / "metadata.hex"
    bad.write_text("0x6d657461")
    result = check(bad)
    assert result.returncode == 1
    assert "refused" in result.stderr
