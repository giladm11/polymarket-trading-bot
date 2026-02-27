#!/usr/bin/env python3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from lowball.crypto_target_lowball import run_strategy

if __name__ == "__main__":
    run_strategy("XRP")
