#!/usr/bin/env python3
"""
BTC 5 Minute Forever Strategy

1. Finds the next 5 minutes BTC up/down market.
2. Places 2 limit orders (Buy UP, Buy DOWN).
3. Claims rewards for previous market.
4. Runs forever.
"""

import os
import sys
import time
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from web3 import Web3

from src.bot import TradingBot
from src.gamma_client import GammaClient
from src.config import Config

# Configuration
ORDER_AMOUNT_USD = 5
ORDER_PRICE = 0.45
MARKET_DURATION = 5  # minutes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BTC_5Min_Strategy")


async def main():
    logger.info("Starting BTC 5 Minute Forever Strategy...")
    
    # Load env
    load_dotenv()
    private_key = os.environ.get("POLY_PRIVATE_KEY")
    if not private_key:
        logger.error("POLY_PRIVATE_KEY not set in .env")
        sys.exit(1)

    # Initialize Bot
    config = Config.from_env()
    bot = TradingBot(config=config, private_key=private_key)
    
    if not bot.config.use_gasless:
        logger.warning("Gasless mode not enabled! Redemption transactions will require MATIC.")
        # We enforce gasless for this strategy as we use RelayerClient for redeem
        logger.error("This strategy requires gasless mode (Builder API).")
        sys.exit(1)

    # Deploy safe if needed
    logger.info("Checking Safe deployment...")
    await bot.deploy_safe_if_needed()
    
    # Initialize Gamma Client (5 min markets)
    gamma = GammaClient(duration_minutes=MARKET_DURATION)
    
    last_traded_slug = None

    while True:
        try:
            # 1. Find the next market
            logger.info("Looking for next BTC 5min market...")
            market = gamma.get_next_market("BTC")
            
            if not market:
                logger.info("No market found. Retrying in 10s...")
                await asyncio.sleep(10)
                continue
                
            slug = market.get("slug")
            condition_id = market.get("conditionId")
            end_date_iso = market.get("endDate")
            
            # Parse End Date
            try:
                # ISO format: 2023-10-01T12:00:00Z
                if end_date_iso.endswith("Z"):
                    end_date_iso = end_date_iso[:-1] + "+00:00"
                end_ts = datetime.fromisoformat(end_date_iso).timestamp()
            except Exception:
                # Fallback: calculate from slug if possible or just use current + 5m
                end_ts = time.time() + 300

            if slug == last_traded_slug:
                logger.info(f"Already traded {slug}. Waiting...")
                await asyncio.sleep(10)
                continue
            
            logger.info(f"Found market: {slug}")
            logger.info(f"Condition ID: {condition_id}")
            
            # 3. Check Neg Risk
            if market.get("negRisk"):
                logger.warning("Market is Negative Risk. Redemption not fully supported.")
            
            # 4. Place Orders
            token_ids = gamma.parse_token_ids(market)
            size = ORDER_AMOUNT_USD / ORDER_PRICE
            size = round(size, 0)
            
            logger.info(f"Placing orders: Size={size}, Price={ORDER_PRICE}")
            
            res_up = await bot.place_order(token_ids["up"], ORDER_PRICE, size, "BUY")
            logger.info(f"UP Order: {res_up.success} {res_up.message}")
            
            res_down = await bot.place_order(token_ids["down"], ORDER_PRICE, size, "BUY")
            logger.info(f"DOWN Order: {res_down.success} {res_down.message}")
            
            last_traded_slug = slug
            
            # 5. Wait for next cycle
            logger.info(f"Sleeping for {MARKET_DURATION} minutes...")
            await asyncio.sleep(MARKET_DURATION * 60)
            
        except Exception as e:
            logger.error(f"Error in loop: {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopping...")
