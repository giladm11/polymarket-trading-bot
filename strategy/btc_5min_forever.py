#!/usr/bin/env python3
"""
BTC 5 Minute Forever Strategy (Refactored)

Uses BaseStrategy for robust order management.
1. Finds next BTC 5-min market.
2. Places UP/DOWN orders.
3. Monitors fills -> places auto-sell at 0.99.
4. Cancels unfilled at end of cycle.

Environment variables:
  POLY_PRIVATE_KEY       - required
  ORDER_AMOUNT_USD       - USD per side (default: 5)
  DRY_RUN                - set to "true" to log orders without submitting (default: false)
  TELEGRAM_BOT_TOKEN     - Telegram bot token (optional)
  TELEGRAM_CHAT_ID       - Telegram group/channel chat ID (optional)
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
ORDER_PRICE = 0.45
SELL_PRICE = 0.99
MARKET_DURATION = 5        # minutes
SELL_DELAY_SECONDS = 5     # wait after fill before placing sell

# Persistent config file (stores runtime-adjustable settings like order size)
CONFIG_FILE = Path(__file__).parent / "bot_config.json"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BTC_5Min_Strategy")


# ---------------------------------------------------------------------------
# Telegram Notifier
# ---------------------------------------------------------------------------

class TelegramNotifier:
    """Sends messages to a Telegram group via Bot API, with command polling."""

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = str(chat_id)
        self._base_url = f"https://api.telegram.org/bot{token}"
        self._last_update_id = 0  # for getUpdates offset

    # ------------------------------------------------------------------
    # Sending
    # ------------------------------------------------------------------

    async def send(self, text: str, chat_id: str = None) -> bool:
        """Send a message asynchronously."""
        import urllib.request
        import json

        url = f"{self._base_url}/sendMessage"
        payload = json.dumps({
            "chat_id": chat_id or self.chat_id,
            "text": text,
            "parse_mode": "HTML",
        }).encode("utf-8")

        try:
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            await asyncio.to_thread(urllib.request.urlopen, req, timeout=10)
            return True
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")
            return False

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------

    async def get_updates(self) -> list:
        """Fetch new updates from Telegram."""
        import urllib.request
        import json

        # URL-encode the allowed_updates array to avoid f-string quote issues
        url = (
            self._base_url
            + "/getUpdates"
            + "?offset=" + str(self._last_update_id + 1)
            + "&timeout=1"
            + "&allowed_updates=%5B%22message%22%5D"
        )
        try:
            resp = await asyncio.to_thread(urllib.request.urlopen, url, timeout=5)
            data = json.loads(resp.read().decode())
            updates = data.get("result", [])
            if updates:
                self._last_update_id = updates[-1]["update_id"]
            return updates
        except Exception as e:
            logger.debug(f"Telegram getUpdates failed: {e}")
            return []


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

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

        # Telegram
        tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "")
        self.telegram: Optional[TelegramNotifier] = (
            TelegramNotifier(tg_token, tg_chat) if tg_token and tg_chat else None
        )
        if self.telegram:
            logger.info("Telegram notifications enabled.")
        else:
            logger.info("Telegram notifications disabled (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set).")

        # Order amount — runtime-adjustable via /setsize, persisted to bot_config.json
        self.order_amount_usd: float = self._load_config_value("order_amount_usd", float(os.environ.get("ORDER_AMOUNT_USD", "5")))

        # Balance report interval in seconds — runtime-adjustable via /setinterval
        self.balance_report_interval: float = self._load_config_value("balance_report_interval", 3600.0)

        # Dry-run mode — logs orders without submitting them
        self.dry_run: bool = os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes")
        if self.dry_run:
            logger.warning("*** DRY RUN MODE ENABLED — orders will NOT be submitted ***")

        # Hourly balance reporting
        self._last_balance_report = 0.0

        # Track cycles where we've already sent a low-balance Telegram alert
        self._low_balance_notified: set = set()

        # Track which cycle end times we've already placed orders for (by end timestamp)
        # On startup, pre-populate with the current window so we skip it.
        now = datetime.now(timezone.utc)
        current_minute = (now.minute // MARKET_DURATION) * MARKET_DURATION
        current_window_start = now.replace(minute=current_minute, second=0, microsecond=0)
        current_window_end = current_window_start.timestamp() + (MARKET_DURATION * 60)
        self.entered_cycles = {current_window_end}  # set of end timestamps already traded
        self._entering = False  # guard against concurrent order placement
        logger.info(f"Bot started. Skipping current cycle, will enter next one after {datetime.fromtimestamp(current_window_end)}")

    # ------------------------------------------------------------------
    # Order placement (dry-run aware)
    # ------------------------------------------------------------------

    async def place_order(self, token_id: str, price: float, size: float, side: str):
        """Place an order, or simulate it in dry-run mode."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would place {side} {size} @ {price} (token: {token_id[:16]}...)")
            # Return a fake successful OrderInfo so the rest of the strategy works normally
            from examples.strategy_example import OrderInfo
            fake = OrderInfo(
                order_id=f"dryrun_{int(time.time() * 1000)}",
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                status="pending",
            )
            self.orders[fake.order_id] = fake
            return fake
        return await super().place_order(token_id=token_id, price=price, size=size, side=side)

    # ------------------------------------------------------------------
    # Persistent config helpers
    # ------------------------------------------------------------------

    def _load_config_value(self, key: str, default):
        """Load a single value from bot_config.json, falling back to default."""
        import json
        try:
            if CONFIG_FILE.exists():
                data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
                if key in data:
                    value = type(default)(data[key])
                    logger.info(f"Loaded {key}={value} from {CONFIG_FILE.name}")
                    return value
        except Exception as e:
            logger.warning(f"Could not read {CONFIG_FILE.name} ({key}): {e}")
        return default

    def _save_config(self) -> None:
        """Persist all runtime-adjustable settings to bot_config.json."""
        import json
        try:
            data = {}
            if CONFIG_FILE.exists():
                try:
                    data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
                except Exception:
                    pass
            data["order_amount_usd"] = self.order_amount_usd
            data["balance_report_interval"] = self.balance_report_interval
            CONFIG_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
            logger.info(f"Config saved to {CONFIG_FILE.name}")
        except Exception as e:
            logger.warning(f"Could not save {CONFIG_FILE.name}: {e}")

    # kept for backwards compatibility
    def _load_order_amount(self) -> float:
        return self._load_config_value("order_amount_usd", float(os.environ.get("ORDER_AMOUNT_USD", "5")))

    def _save_order_amount(self) -> None:
        self._save_config()

    # ------------------------------------------------------------------
    # Telegram helpers
    # ------------------------------------------------------------------

    async def _notify(self, text: str) -> None:
        if self.telegram:
            await self.telegram.send(text)

    async def _report_balance(self, context: str = "") -> None:
        """Fetch balance and send it to Telegram."""
        balance = await self.bot.get_balance()
        prefix = f"[{context}] " if context else ""
        dry = " [DRY RUN]" if self.dry_run else ""
        msg = f"\U0001f4b0 {prefix}Current balance: <b>${balance:.2f} USDC</b>{dry}"
        logger.info(msg)
        await self._notify(msg)
        self._last_balance_report = time.time()

    # ------------------------------------------------------------------
    # Telegram command handler (runs concurrently with strategy loop)
    # ------------------------------------------------------------------

    async def _handle_telegram_commands(self) -> None:
        """Poll Telegram for commands and respond to them."""
        if not self.telegram:
            return

        dry_tag = " <i>[DRY RUN]</i>" if self.dry_run else ""
        HELP = (
            "\U0001f916 <b>Available commands:</b>\n"
            "/balance \u2014 current USDC balance\n"
            "/size \u2014 current order size (USD per side)\n"
            "/setsize &lt;amount&gt; \u2014 set order size (e.g. /setsize 10)\n"
            "/interval \u2014 current balance report interval\n"
            "/setinterval &lt;minutes&gt; \u2014 set report interval (e.g. /setinterval 30)"
        )

        while self.status == StrategyStatus.RUNNING:
            try:
                updates = await self.telegram.get_updates()
                for update in updates:
                    msg = update.get("message") or update.get("edited_message")
                    if not msg:
                        continue

                    text = (msg.get("text") or "").strip()
                    chat_id = str(msg["chat"]["id"])

                    if not text.startswith("/"):
                        continue

                    # Strip bot username suffix (e.g. /balance@MyBot -> /balance)
                    command = text.split()[0].lower().split("@")[0]
                    args = text.split()[1:]

                    if command == "/balance":
                        balance = await self.bot.get_balance()
                        reply = f"\U0001f4b0 Current balance: <b>${balance:.2f} USDC</b>{dry_tag}"

                    elif command == "/size":
                        reply = (
                            f"\U0001f4d0 Current order size: <b>${self.order_amount_usd:.2f} USD</b> per side\n"
                            f"(Total per cycle: <b>${self.order_amount_usd * 2:.2f} USD</b>){dry_tag}"
                        )

                    elif command == "/setsize":
                        if not args:
                            reply = "\u274c Usage: /setsize &lt;amount&gt;  (e.g. /setsize 10)"
                        else:
                            try:
                                new_size = float(args[0])
                                if new_size <= 0:
                                    raise ValueError("must be positive")
                                old_size = self.order_amount_usd
                                self.order_amount_usd = new_size
                                self._save_config()
                                logger.info(f"Order size changed via Telegram: ${old_size} -> ${new_size}")
                                reply = (
                                    f"\u2705 Order size updated: "
                                    f"<b>${old_size:.2f}</b> \u2192 <b>${new_size:.2f} USD</b> per side{dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"\u274c Invalid amount: {e}"

                    elif command == "/interval":
                        mins = self.balance_report_interval / 60
                        reply = (
                            f"\u23f0 Balance report interval: "
                            f"<b>{mins:.0f} min</b> ({self.balance_report_interval:.0f}s){dry_tag}"
                        )

                    elif command == "/setinterval":
                        if not args:
                            reply = "\u274c Usage: /setinterval &lt;minutes&gt;  (e.g. /setinterval 30)"
                        else:
                            try:
                                new_mins = float(args[0])
                                if new_mins <= 0:
                                    raise ValueError("must be positive")
                                old_mins = self.balance_report_interval / 60
                                self.balance_report_interval = new_mins * 60
                                self._save_config()
                                logger.info(f"Balance report interval changed via Telegram: {old_mins:.0f}min -> {new_mins:.0f}min")
                                reply = (
                                    f"\u2705 Report interval updated: "
                                    f"<b>{old_mins:.0f} min</b> \u2192 <b>{new_mins:.0f} min</b>{dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"\u274c Invalid value: {e}"

                    elif command in ("/help", "/start"):
                        reply = HELP

                    else:
                        reply = HELP

                    await self.telegram.send(reply, chat_id=chat_id)

            except Exception as e:
                logger.debug(f"Command handler error: {e}")

            await asyncio.sleep(2)  # poll every 2 seconds

    # ------------------------------------------------------------------
    # Lifecycle overrides
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        await super().initialize()
        dry = " [DRY RUN]" if self.dry_run else ""
        await self._report_balance(f"Bot started{dry}")

    async def cleanup(self) -> None:
        dry = " [DRY RUN]" if self.dry_run else ""
        await self._report_balance(f"Bot stopped{dry}")
        await super().cleanup()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self, token_ids: List[str] = None, duration: int = None):
        """
        Override run loop to support dynamic market discovery.
        Runs strategy loop and Telegram command poller concurrently.
        """
        await self.initialize()

        async def _strategy_loop():
            start_time = time.time()
            try:
                while self.status == StrategyStatus.RUNNING:
                    if duration and (time.time() - start_time) > duration:
                        break

                    # Sync order statuses first (triggers on_order_update on changes)
                    await self.sync_orders()

                    # Main Strategy Logic
                    await self.on_tick({})

                    # Periodic balance report
                    if time.time() - self._last_balance_report >= self.balance_report_interval:
                        interval_mins = self.balance_report_interval / 60
                        await self._report_balance(f"Update (every {interval_mins:.0f}min)")

                    await asyncio.sleep(self.check_interval)
            finally:
                await self.cleanup()

        # Run strategy loop and Telegram command poller concurrently
        await asyncio.gather(
            _strategy_loop(),
            self._handle_telegram_commands(),
        )

    # ------------------------------------------------------------------
    # Tick logic
    # ------------------------------------------------------------------

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
        except Exception:
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

            # Read order amount from instance variable (runtime-adjustable via /setsize)
            order_amount_usd = self.order_amount_usd
            size = round(order_amount_usd / ORDER_PRICE, 0)

            # Recalculate actual cost after rounding (size * price, two sides)
            actual_cost_per_side = round(size * ORDER_PRICE, 2)
            total_cost = actual_cost_per_side * 2

            # Pre-flight balance check (skip in dry-run — no real funds needed)
            if not self.dry_run:
                balance = await self.bot.get_balance()
                if balance < total_cost:
                    warn_msg = (
                        f"Insufficient balance: ${balance:.2f} available, "
                        f"${total_cost:.2f} required ({actual_cost_per_side:.2f} x 2 sides). "
                        f"Skipping cycle."
                    )
                    logger.warning(warn_msg)
                    # Send Telegram alert once per cycle
                    if cycle_end not in self._low_balance_notified:
                        self._low_balance_notified.add(cycle_end)
                        await self._notify(f"\u26a0\ufe0f {warn_msg}")
                    # Un-mark so we retry on the next tick
                    self.entered_cycles.discard(cycle_end)
                    return

                logger.info(
                    f"Balance OK: ${balance:.2f} available, "
                    f"${total_cost:.2f} required. "
                    f"Placing orders: Size={size} @ {ORDER_PRICE} (${actual_cost_per_side:.2f} each side)"
                )
            else:
                logger.info(
                    f"[DRY RUN] Simulating orders: Size={size} @ {ORDER_PRICE} "
                    f"(${actual_cost_per_side:.2f} each side, ${total_cost:.2f} total)"
                )

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

    # ------------------------------------------------------------------
    # Order updates
    # ------------------------------------------------------------------

    async def on_order_update(self, order: OrderInfo) -> None:
        """
        Handle order updates.
        If a BUY order fills, wait 5 seconds then place a SELL order
        using the actual token balance.
        """
        if order.status in ('filled', 'MATCHED'):
            if order.side == 'BUY':
                logger.info(f"Order {order.order_id} (BUY) filled! Waiting {SELL_DELAY_SECONDS}s before placing sell...")
                await asyncio.sleep(SELL_DELAY_SECONDS)

                logger.info(f"Fetching actual token balance for {order.token_id}...")

                if self.dry_run:
                    # Simulate a balance equal to what we bought
                    actual_balance = order.size
                    logger.info(f"[DRY RUN] Simulated token balance: {actual_balance}")
                else:
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
