#!/usr/bin/env python3
import sys
from pathlib import Path

# Add strategy dir to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lowball.base_lowball_forever import run_strategy

if __name__ == "__main__":
    run_strategy("BTC")
