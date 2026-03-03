#!/usr/bin/env python3
"""Generate an sr25519 keypair for Materios committee membership.

Usage:
    pip install substrate-interface mnemonic
    python generate_key.py

Outputs:
    - Mnemonic (24 words) — your SIGNER_URI, keep this secret
    - SS58 Address — send this to the FPS team
    - Public Key hex — send this to the FPS team
"""

import sys

try:
    from substrateinterface import Keypair
    from mnemonic import Mnemonic
except ImportError:
    print("Missing dependencies. Install them with:")
    print()
    print("  pip install substrate-interface mnemonic")
    print()
    sys.exit(1)


def main():
    m = Mnemonic("english")
    mnemonic = m.generate(strength=256)  # 24 words
    keypair = Keypair.create_from_mnemonic(mnemonic)

    print()
    print("=" * 64)
    print("  Materios Committee Key Generator")
    print("=" * 64)
    print()
    print("  Mnemonic (24 words) — KEEP THIS SECRET:")
    print()
    print(f"    {mnemonic}")
    print()
    print("  SS58 Address (send to FPS team):")
    print(f"    {keypair.ss58_address}")
    print()
    print("  Public Key hex (send to FPS team):")
    print(f"    0x{keypair.public_key.hex()}")
    print()
    print("=" * 64)
    print("  IMPORTANT:")
    print("  - Save the mnemonic in a password manager")
    print("  - NEVER share the mnemonic with anyone")
    print("  - The SS58 address and public key are safe to share")
    print("  - Use the mnemonic as SIGNER_URI in docker-compose.yml")
    print("=" * 64)
    print()


if __name__ == "__main__":
    main()
