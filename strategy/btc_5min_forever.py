#!/usr/bin/env python3
"""
BTC 5 Minute Forever Strategy (Refactored)

Uses BaseStrategy for robust order management.
1. Finds next BTC 5-min market.
2. Places UP/DOWN orders.
3. Monitors fills -> places auto-sell at 0.99.
4. Cancels unfilled at end of cycle.
"""

import sys
import asyncio
import logging
import time
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

from src.bot import TradingBot, OrderResult, OrderSide
from src.gamma_client import GammaClient
from src.config import Config
from examples.strategy_example import BaseStrategy, Position, OrderInfo, StrategyStatus

# Configuration
ORDER_AMOUNT_USD = 5
ORDER_PRICE = 0.45
SELL_PRICE = 0.99
MARKET_DURATION = 5  # minutes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BTC_5Min_Strategy")


class Btc5MinStrategy(BaseStrategy):
    """
    BTC 5-Minute Forever Strategy.
    """
    def __init__(self, bot: TradingBot, params: Optional[Dict[str, Any]] = None):
        super().__init__(bot, params or {}, name="Btc5MinForever")
        
        self.gamma = GammaClient(duration_minutes=MARKET_DURATION)
        self.current_market: Optional[Dict[str, Any]] = None
        self.token_ids: Dict[str, str] = {}
        self.cycle_end_time = 0.0
        self.cycle_active = False
        self.last_slug = None

    async def run(self, token_ids: List[str] = None, duration: int = None):
        """
        Override run loop to support dynamic market discovery.
        We don't need fixed token_ids.
        """
        await self.initialize()
        start_time = time.time()
        
        try:
            while self.status == StrategyStatus.RUNNING:
                await self.sync_orders()

                if duration and (time.time() - start_time) > duration:
                    break
                
                # Main Strategy Logic
                await self.on_tick({})
                
                # Poll orders manually since we might not rely on socket updates yet
                # (BaseStrategy usually needs a socket listener or manual polling)
                # We will manually sync orders here
                await self.sync_orders()

                await asyncio.sleep(self.check_interval)
        finally:
            await self.cleanup()

    async def on_tick(self, _: Dict[str, Any]) -> None:
        """
        Called periodically.
        Manages the market cycle state.
        """
        now = time.time()
        
        # 1. If no active cycle, find next market
        if not self.cycle_active:
            await self.find_and_enter_market()
        
        # 2. If cycle is active, check if it's over
        elif self.cycle_active and now >= self.cycle_end_time:
            logger.info("Cycle ended.")
            # await self.cancel_all_orders() # Disabled as per user request
            self.cycle_active = False
            self.current_market = None
            self.token_ids = {}
            # Wait a small buffer before searching again
            await asyncio.sleep(5) 

    async def find_and_enter_market(self):
        """Find next market and place initial orders."""
        logger.info("Looking for next BTC 5min market...")
        market = self.gamma.get_next_market("BTC")
        
        if not market:
            logger.info("No market found. Retrying next tick...")
            return

        slug = market.get("slug")
        if slug == self.last_slug:
            # Already played this one
            return
            
        logger.info(f"Found market: {slug}")
        self.last_slug = slug
        self.current_market = market
        
        # Calculate End Time
        end_date_iso = market.get("endDate")
        try:
            if end_date_iso.endswith("Z"):
                end_date_iso = end_date_iso[:-1] + "+00:00"
            self.cycle_end_time = datetime.fromisoformat(end_date_iso).timestamp()
        except:
            self.cycle_end_time = time.time() + (MARKET_DURATION * 60)

        # Parse Tokens
        self.token_ids = self.gamma.parse_token_ids(market)
        
        # Calculate Size
        size = round(ORDER_AMOUNT_USD / ORDER_PRICE, 0)
        
        # Place Orders
        logger.info(f"Placing initial orders. Size={size} @ {ORDER_PRICE}")
        
        # Buy UP
        await self.place_order(
            token_id=self.token_ids["up"],
            price=ORDER_PRICE,
            size=size,
            side="BUY"
        )
        # Buy DOWN
        await self.place_order(
            token_id=self.token_ids["down"],
            price=ORDER_PRICE,
            size=size,
            side="BUY"
        )
        
        self.cycle_active = True
        logger.info(f"Cycle started. Ends at {datetime.fromtimestamp(self.cycle_end_time)}")

    async def on_order_update(self, order: OrderInfo) -> None:
        """
        Handle order updates.
        If a BUY order fills, place a SELL order.
        """
        if order.status == 'filled' or order.status == 'MATCHED':  # 'MATCHED' is API status
            if order.side == 'BUY':
                logger.info(f"Order {order.order_id} ({order.side}) filled! Placing SELL at {SELL_PRICE}")
                
                # Check if we already have a sell for this Token ID to avoid duplicates?
                # For simplicity, we just place the sell order.
                # BaseStrategy doesn't track "pending sell for this position" specifically,
                # but we can check open orders if needed. 
                
                await self.place_order(
                    token_id=order.token_id,
                    price=SELL_PRICE,
                    size=order.size,
                    side="SELL"
                )
                
                # Add position tracking (optional, but good practice)
                self.add_position(Position(
                    token_id=order.token_id,
                    side='BUY',
                    size=order.size,
                    entry_price=order.price
                ))

            elif order.side == 'SELL':
                logger.info(f"Sell order {order.order_id} filled. Profit secured.")
                self.close_position(order.token_id, 'BUY')


async def main():
    load_dotenv()
    private_key = os.environ.get("POLY_PRIVATE_KEY")
    if not private_key:
        logger.error("POLY_PRIVATE_KEY not set")
        sys.exit(1)

    config = Config.from_env()
    bot = TradingBot(config=config, private_key=private_key)
    
    if not bot.config.use_gasless:
         logger.warning("Gasless mode not enabled")

    # Initialize strategy
    # check_interval: 1 second for fast monitoring of cycle end
    strategy = Btc5MinStrategy(bot, params={"check_interval": 1})
    
    logger.info("Starting BTC 5Min Strategy (Refactored)...")
    
    # We pass an empty list of tokens because we dynamically find markets.
    # The run loop will call on_tick periodically.
    await strategy.run(token_ids=[], duration=None)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopping...")
