import os
import sys

# Make the repo root importable so `import daemon.cert_daemon` works when
# pytest is invoked from the repo root.
HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
