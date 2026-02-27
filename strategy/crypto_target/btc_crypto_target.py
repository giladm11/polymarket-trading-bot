#!/usr/bin/env python3
import sys
from pathlib import Path

# Add this folder to path so we can import the sibling module
sys.path.insert(0, str(Path(__file__).parent))

from crypto_target_lowball import run_strategy

if __name__ == "__main__":
    run_strategy("BTC")
