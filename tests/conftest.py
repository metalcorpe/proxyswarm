"""Shared test fixtures and path setup.

`examples/` is a standalone script directory, not an installed package, so add
it to sys.path to let the use-case tests import `malware_bazaar` directly.
"""

import sys
from pathlib import Path

EXAMPLES_DIR = str(Path(__file__).resolve().parent.parent / "examples")
if EXAMPLES_DIR not in sys.path:
    sys.path.insert(0, EXAMPLES_DIR)
