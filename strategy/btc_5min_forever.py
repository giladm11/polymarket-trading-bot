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
from datetime import datetime, timezone
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

        # Track which cycle end times we've already placed orders for (by end timestamp)
        # On startup, pre-populate with the current window so we skip it.
        now = datetime.now(timezone.utc)
        current_minute = (now.minute // MARKET_DURATION) * MARKET_DURATION
        current_window_start = now.replace(minute=current_minute, second=0, microsecond=0)
        current_window_end = current_window_start.timestamp() + (MARKET_DURATION * 60)
        self.entered_cycles = {current_window_end}  # set of end timestamps already traded
        self._entering = False  # guard against concurrent order placement
        logger.info(f"Bot started. Skipping current cycle, will enter next one after {datetime.fromtimestamp(current_window_end)}")

    async def run(self, token_ids: List[str] = None, duration: int = None):
        """
        Override run loop to support dynamic market discovery.
        We don't need fixed token_ids.
        """
        await self.initialize()
        start_time = time.time()
        
        try:
            while self.status == StrategyStatus.RUNNING:
                if duration and (time.time() - start_time) > duration:
                    break
                
                # Sync order statuses first (triggers on_order_update on changes)
                await self.sync_orders()

                # Main Strategy Logic
                await self.on_tick({})

                await asyncio.sleep(self.check_interval)
        finally:
            await self.cleanup()

    async def on_tick(self, _: Dict[str, Any]) -> None:
        """
        Called every second.
        - If cycle ended, clean up state.
        - Always try to pre-place orders for the next upcoming market.
        """
        now = time.time()
        
        # 1. Check if current cycle has ended
        if self.cycle_active and now >= self.cycle_end_time:
            logger.info("Cycle ended.")
            self.cycle_active = False
            self.current_market = None
            self.token_ids = {}
            # Clean up unfilled orders from tracking (don't cancel on-chain)
            for order_id in list(self.orders.keys()):
                if self.orders[order_id].status not in ('filled', 'MATCHED', 'cancelled'):
                    logger.info(f"Dropping unfulfilled order {order_id} from tracking.")
                    del self.orders[order_id]

        # 2. Always try to place orders for the next upcoming market
        await self.try_enter_next_market()

    async def try_enter_next_market(self):
        """
        Look for the next upcoming market and place orders if we haven't yet.
        This runs every tick so we pre-place orders before the cycle starts.
        """
        # Guard: prevent re-entry while orders are being placed
        if self._entering:
            return

        market = self.gamma.get_next_market("BTC")

        if not market:
            return

        if not market.get("acceptingOrders", False):
            return

        # Calculate end time to use as unique cycle key
        end_date_iso = market.get("endDate", "")
        try:
            if end_date_iso.endswith("Z"):
                end_date_iso = end_date_iso[:-1] + "+00:00"
            cycle_end = datetime.fromisoformat(end_date_iso).timestamp()
        except:
            cycle_end = time.time() + (MARKET_DURATION * 60)

        # Skip if we've already entered this cycle
        if cycle_end in self.entered_cycles:
            return

        # Mark as entered immediately — before any awaits — to block concurrent ticks
        self.entered_cycles.add(cycle_end)
        self._entering = True

        try:
            slug = market.get("slug")
            logger.info(f"Pre-placing orders for next market: {slug} (ends {datetime.fromtimestamp(cycle_end)})")

            # Parse Tokens
            token_ids = self.gamma.parse_token_ids(market)
            size = round(ORDER_AMOUNT_USD / ORDER_PRICE, 0)
            logger.info(f"Placing orders: Size={size} @ {ORDER_PRICE}")

            # Buy UP
            await self.place_order(
                token_id=token_ids["up"],
                price=ORDER_PRICE,
                size=size,
                side="BUY"
            )
            # Buy DOWN
            await self.place_order(
                token_id=token_ids["down"],
                price=ORDER_PRICE,
                size=size,
                side="BUY"
            )

            # Update cycle tracking
            self.cycle_end_time = cycle_end
            self.cycle_active = True
            self.current_market = market
            self.token_ids = token_ids
            logger.info(f"Orders placed. Cycle ends at {datetime.fromtimestamp(cycle_end)}")
        finally:
            self._entering = False

    async def on_order_update(self, order: OrderInfo) -> None:
        """
        Handle order updates.
        If a BUY order fills, place a SELL order using the actual token balance.
        """
        if order.status in ('filled', 'MATCHED'):
            if order.side == 'BUY':
                logger.info(f"Order {order.order_id} (BUY) filled! Fetching actual token balance...")

                # Retry balance fetch — there's a propagation delay between fill and balance update
                actual_balance = 0.0
                for attempt in range(6):
                    actual_balance = await self.bot.get_token_balance(order.token_id)
                    if actual_balance > 0:
                        break
                    logger.info(f"Balance not yet updated (attempt {attempt + 1}/6), retrying in 2s...")
                    await asyncio.sleep(2)

                if actual_balance <= 0:
                    logger.warning(f"Token balance still 0 after retries for {order.token_id}, skipping sell.")
                    return

                # Round down to 2dp to stay within balance
                sell_size = round(actual_balance, 2)
                if sell_size > actual_balance:
                    sell_size = sell_size - 0.01

                logger.info(f"Placing SELL {sell_size} @ {SELL_PRICE} (balance: {actual_balance})")

                await self.place_order(
                    token_id=order.token_id,
                    price=SELL_PRICE,
                    size=sell_size,
                    side="SELL"
                )

                self.add_position(Position(
                    token_id=order.token_id,
                    side='BUY',
                    size=sell_size,
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
