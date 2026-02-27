#!/usr/bin/env python3
"""
BTC Lowball Grid Strategy (Forever)

Runs continuously, cycling through every BTC 5-min market.

Logic per cycle:
  1. Place 4 BUY orders on BOTH up and down tokens at prices:
       0.15, 0.10, 0.05, 0.02
     Orders are posted at the START of the cycle (as soon as the market
     accepts orders) but are CANCELLED 3.5 minutes before the cycle ends
     (i.e. at cycle_end - 210 seconds) if still unfilled.

  2. When any BUY order is filled, place a SELL at exactly 4× the buy price
     for the same token amount:
       0.15 buy → 0.60 sell
       0.10 buy → 0.40 sell
       0.05 buy → 0.20 sell
       0.02 buy → 0.08 sell

  3. Telegram bot for runtime configuration and fill notifications.

Environment variables:
  POLY_PRIVATE_KEY          - required
  ORDER_AMOUNT_USD          - USD per price level per side (default: 1)
  DRY_RUN                   - "true" to log orders without submitting (default: false)
  TELEGRAM_BOT_TOKEN        - Telegram bot token (optional)
  TELEGRAM_CHAT_ID          - Telegram group/channel chat ID (optional)
"""

import sys
import asyncio
import logging
import time
import os
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
# Add project root directory to path (three levels up from strategy/lowball/script.py)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from dotenv import load_dotenv

from src.bot import TradingBot
from src.gamma_client import GammaClient
from src.config import Config
from examples.strategy_example import BaseStrategy, Position, OrderInfo, StrategyStatus

# ---------------------------------------------------------------------------
# Strategy constants
# ---------------------------------------------------------------------------

# Buy prices and their fractioned sell targets
BUY_PRICES = [0.05]
SELL_TARGETS = [0.20, 0.25, 0.30, 0.35]

MARKET_DURATION = 5            # minutes per cycle
# Orders are cancelled this many seconds before cycle end if still open.
# NOTE: Gamma's endDate is systematically ~60s ahead of the real market close,
# so we subtract 60s from the intended 3.5 min (210s) window to compensate:
#   150s here  +  60s API offset  =  210s = 3.5 min before actual cycle end.
ORDER_CANCEL_BEFORE_END = 210  # 210s - 60s offset compensation

SELL_DELAY_SECONDS = 3         # wait after fill before placing sell
SELL_ORDER_RETRY_ATTEMPTS = 7

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Base_Lowball_Strategy")


# ---------------------------------------------------------------------------
# Telegram Notifier (identical pattern to btc_5min_forever.py)
# ---------------------------------------------------------------------------

class TelegramNotifier:
    """Sends messages to a Telegram group via Bot API, with command polling."""

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = str(chat_id)
        self._base_url = f"https://api.telegram.org/bot{token}"
        self._last_update_id = 0

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

    async def get_updates(self) -> list:
        """Fetch new updates from Telegram."""
        import urllib.request
        import json

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

class BaseLowballStrategy(BaseStrategy):
    """
    Base Lowball Grid — Forever Strategy.

    Places 4 buy orders at low prices (0.15 / 0.10 / 0.05 / 0.02) on both
    UP and DOWN tokens each cycle.  Fills generate a 4× sell order.
    Unfilled buys are cancelled at cycle_end − 3.5 min.
    """

    def __init__(self, bot: TradingBot, ticker: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(bot, params or {}, name=f"{ticker}Lowball")
        self.ticker = ticker.upper()
        self.config_file = Path(__file__).parent / f"bot_config_lowball_{self.ticker.lower()}.json"

        self.gamma = GammaClient(duration_minutes=MARKET_DURATION)
        self.current_market: Optional[Dict[str, Any]] = None
        self.token_ids: Dict[str, str] = {}
        self.cycle_end_time: float = 0.0
        self.cycle_active: bool = False

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

        # Order amount per price level per side — runtime-adjustable via /setsize
        self.order_amount_usd: float = self._load_config_value(
            "lowball_order_amount_usd",
            float(os.environ.get("ORDER_AMOUNT_USD", "2"))
        )

        # Sell targets
        loaded_targets = self._load_config_value("sell_targets", SELL_TARGETS)
        self.sell_targets = [float(t) for t in loaded_targets]

        # Balance report interval
        self.balance_report_interval: float = self._load_config_value(
            "balance_report_interval", 3600.0 * 60
        )

        # Dry-run mode
        self.dry_run: bool = os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes")
        if self.dry_run:
            logger.warning("*** DRY RUN MODE ENABLED — orders will NOT be submitted ***")

        # Hourly balance reporting
        self._last_balance_report = 0.0

        # Track cycles where we've already sent a low-balance alert
        self._low_balance_notified: set = set()

        # Map order_id → buy_price  (so we know what sell price to use on fill)
        self._order_buy_price: Dict[str, float] = {}

        # Track active buy order IDs per cycle (for cancellation)
        self._active_buy_order_ids: List[str] = []

        # Whether we've already cancelled the resting buys this cycle
        self._buys_cancelled: bool = False

        # Guard against concurrent order placement
        self._entering: bool = False

        # Track which cycle end timestamps we've already entered
        now = datetime.now(timezone.utc)
        current_minute = (now.minute // MARKET_DURATION) * MARKET_DURATION
        current_window_start = now.replace(minute=current_minute, second=0, microsecond=0)
        current_window_end = current_window_start.timestamp() + (MARKET_DURATION * 60)
        self.entered_cycles: set = {current_window_end}
        logger.info(
            f"Bot started. Skipping current cycle, will enter next one after "
            f"{datetime.fromtimestamp(current_window_end)}"
        )

    # ------------------------------------------------------------------
    # Persistent config helpers
    # ------------------------------------------------------------------

    def _load_config_value(self, key: str, default):
        """Load a single value from config file, falling back to default."""
        import json
        try:
            if self.config_file.exists():
                data = json.loads(self.config_file.read_text(encoding="utf-8"))
                if key in data:
                    value = type(default)(data[key])
                    logger.info(f"Loaded {key}={value} from {self.config_file.name}")
                    return value
        except Exception as e:
            logger.warning(f"Could not read {self.config_file.name} ({key}): {e}")
        return default

    def _save_config(self) -> None:
        """Persist all runtime-adjustable settings."""
        import json
        try:
            data = {}
            if self.config_file.exists():
                try:
                    data = json.loads(self.config_file.read_text(encoding="utf-8"))
                except Exception:
                    pass
            data["lowball_order_amount_usd"] = self.order_amount_usd
            data["sell_targets"] = self.sell_targets
            data["balance_report_interval"] = self.balance_report_interval
            self.config_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
            logger.info(f"Config saved to {self.config_file.name}")
        except Exception as e:
            logger.warning(f"Could not save {self.config_file.name}: {e}")

    # ------------------------------------------------------------------
    # Order placement (dry-run aware)
    # ------------------------------------------------------------------

    async def place_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        order_type: str = "GTC",
        expiration: int = 0,
        send_error_to_telegram: bool = True,
    ):
        """Place an order, or simulate it in dry-run mode."""
        if self.dry_run:
            logger.info(
                f"[DRY RUN] Would place {order_type} {side} {size} @ {price} "
                f"(token: {token_id[:16]}..., expiration: {expiration or 'none'})"
            )
            fake = OrderInfo(
                order_id=f"dryrun_{int(time.time() * 1000)}_{price}",
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                status="pending",
            )
            self.orders[fake.order_id] = fake
            await asyncio.sleep(0.05)   # small delay to avoid identical timestamps
            return fake
        result = await self.bot.place_order(
            token_id=token_id,
            price=price,
            size=size,
            side=side,
            order_type=order_type,
            expiration=expiration,
        )
        if not result or not result.success or not result.order_id:
            import html
            error_msg = result.message if result else 'no result'
            log_msg = f"Order placement failed: {error_msg} (side: {side}, size: {size}, price: {price})"
            logger.warning(log_msg)
            safe_error_msg = html.escape(str(error_msg))
            if send_error_to_telegram:
                await self._notify(f"\u274c <b>ORDER FAILED:</b> {safe_error_msg}\nSide: {side}\nSize: {size}\nPrice: {price}")
            return None

        order_info = OrderInfo(
            order_id=result.order_id,
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            status="pending",
        )
        self.orders[order_info.order_id] = order_info
        return order_info

    # ------------------------------------------------------------------
    # Telegram helpers
    # ------------------------------------------------------------------

    async def _notify(self, text: str) -> None:
        if self.telegram:
            await self.telegram.send(f"<b>[{self.name}]</b> {text}")

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
    # Telegram command handler
    # ------------------------------------------------------------------

    async def _handle_telegram_commands(self) -> None:
        """Poll Telegram for commands and respond to them."""
        if not self.telegram:
            return

        dry_tag = " <i>[DRY RUN]</i>" if self.dry_run else ""
        num_levels = len(BUY_PRICES)
        prices_str = " / ".join(str(p) for p in BUY_PRICES)
        HELP = (
            f"\U0001f916 <b>[{self.name}] Available commands:</b>\n"
            "/balance — current USDC balance\n"
            "/size — current order size per price level\n"
            f"/setsize &lt;amount&gt; — set order size per level (e.g. /setsize 2)\n"
            f"  ({num_levels} levels × 2 sides = {num_levels * 2} orders, prices: {prices_str})\n"
            "/targets — current sell targets\n"
            "/settargets &lt;t1&gt; &lt;t2&gt; ... — set targets (e.g. /settargets 0.20 0.25 0.30)\n"
            "/interval — current balance report interval\n"
            "/setinterval &lt;minutes&gt; — set report interval (e.g. /setinterval 30)\n"
            "/levels — show current price levels & sell targets"
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

                    command = text.split()[0].lower().split("@")[0]
                    args = text.split()[1:]

                    if command == "/balance":
                        balance = await self.bot.get_balance()
                        reply = f"<b>[{self.name}]</b> \U0001f4b0 Current balance: <b>${balance:.2f} USDC</b>{dry_tag}"

                    elif command == "/size":
                        num = len(BUY_PRICES)
                        total = self.order_amount_usd * num * 2
                        reply = (
                            f"<b>[{self.name}]</b> \U0001f4d0 Current order size: <b>${self.order_amount_usd:.2f} USD</b> per level\n"
                            f"({num} price levels × 2 sides = <b>${total:.2f} USD</b> total per cycle){dry_tag}"
                        )

                    elif command == "/setsize":
                        if not args:
                            reply = "\u274c Usage: /setsize &lt;amount&gt;  (e.g. /setsize 2)"
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
                                    f"<b>[{self.name}]</b> \u2705 Order size updated: "
                                    f"<b>${old_size:.2f}</b> \u2192 <b>${new_size:.2f} USD</b> per level{dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"\u274c Invalid amount: {e}"

                    elif command == "/interval":
                        mins = self.balance_report_interval / 60
                        reply = (
                            f"<b>[{self.name}]</b> \u23f0 Balance report interval: "
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
                                logger.info(f"Balance report interval changed: {old_mins:.0f}min -> {new_mins:.0f}min")
                                reply = (
                                    f"<b>[{self.name}]</b> \u2705 Report interval updated: "
                                    f"<b>{old_mins:.0f} min</b> \u2192 <b>{new_mins:.0f} min</b>{dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"\u274c Invalid value: {e}"

                    elif command == "/targets":
                        targets_str = " / ".join(str(t) for t in self.sell_targets)
                        reply = (
                            f"<b>[{self.name}]</b> \U0001f3af Current sell targets:\n"
                            f"<b>{targets_str}</b>\n"
                            f"({len(self.sell_targets)} targets, {100.0/len(self.sell_targets):.1f}% of matched size each){dry_tag}"
                        )

                    elif command == "/settargets":
                        if not args:
                            reply = "\u274c Usage: /settargets &lt;t1&gt; &lt;t2&gt; ...  (e.g. /settargets 0.20 0.25 0.30)"
                        else:
                            try:
                                new_targets = [float(x) for x in args]
                                if any(t <= 0 or t >= 1 for t in new_targets):
                                    raise ValueError("targets must be between 0 and 1")
                                old_str = " / ".join(str(t) for t in self.sell_targets)
                                self.sell_targets = new_targets
                                self._save_config()
                                new_str = " / ".join(str(t) for t in self.sell_targets)
                                logger.info(f"Sell targets changed via Telegram: {old_str} -> {new_str}")
                                reply = (
                                    f"<b>[{self.name}]</b> \u2705 Sell targets updated:\n"
                                    f"<b>{old_str}</b> \u2192 <b>{new_str}</b>\n"
                                    f"({len(self.sell_targets)} targets, {100.0/len(self.sell_targets):.1f}% of matched size each){dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"\u274c Invalid targets: {e}"

                    elif command == "/levels":
                        targets_str = " / ".join(str(t) for t in self.sell_targets)
                        lines = [f"<b>[{self.name}]</b> \U0001f4ca <b>Price levels:</b>"]
                        for bp in BUY_PRICES:
                            cost = round(self.order_amount_usd / bp, 2)
                            lines.append(f"  Buy @ <b>{bp}</b> → Sell targets: <b>{targets_str}</b> (size ≈ {cost} shares)")
                        reply = "\n".join(lines) + dry_tag

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
        await self._report_balance(f"Lowball bot started{dry}")

    async def cleanup(self) -> None:
        dry = " [DRY RUN]" if self.dry_run else ""
        await self._report_balance(f"Lowball bot stopped{dry}")
        await super().cleanup()

    # ------------------------------------------------------------------
    # Main run loop
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

                    # Main tick
                    await self.on_tick({})

                    # Periodic balance report
                    if time.time() - self._last_balance_report >= self.balance_report_interval:
                        interval_mins = self.balance_report_interval / 60
                        await self._report_balance(f"Update (every {interval_mins:.0f}min)")

                    await asyncio.sleep(self.check_interval)
            finally:
                await self.cleanup()

        await asyncio.gather(
            _strategy_loop(),
            self._handle_telegram_commands(),
        )

    # ------------------------------------------------------------------
    # Tick logic
    # ------------------------------------------------------------------

    async def on_tick(self, _: Dict[str, Any]) -> None:
        """
        Called every second (check_interval=1).

        Responsibilities:
          - Detect cycle end → reset state
          - Cancel resting buy orders at cycle_end − ORDER_CANCEL_BEFORE_END
          - Pre-place orders for the next upcoming market
        """
        now = time.time()

        # 1. Check if current cycle has ended
        if self.cycle_active and now >= self.cycle_end_time:
            logger.info("Cycle ended. Resetting state.")
            self.cycle_active = False
            self.current_market = None
            self.token_ids = {}
            self._active_buy_order_ids.clear()
            self._buys_cancelled = False
            # Drop unfilled orders from tracking
            for order_id in list(self.orders.keys()):
                if self.orders[order_id].status not in ('filled', 'MATCHED', 'cancelled'):
                    logger.info(f"Dropping unfulfilled order {order_id} from tracking.")
                    del self.orders[order_id]
            # Clean up order→price map to avoid unbounded growth
            self._order_buy_price.clear()

        # 2. Cancel resting buys 3.5 min before cycle end
        # if (
        #     self.cycle_active
        #     and not self._buys_cancelled
        #     and now >= self.cycle_end_time - ORDER_CANCEL_BEFORE_END
        # ):
        #     await self._cancel_resting_buys()

        # 3. Try to enter the next market (pre-place orders)
        await self._try_enter_next_market()

    # async def _cancel_resting_buys(self) -> None:
    #     """
    #     Cancel all still-open BUY orders for the current cycle.
    #     Called at cycle_end − 3.5 min.
    #     """
    #     self._buys_cancelled = True
    #     cancel_time = datetime.fromtimestamp(self.cycle_end_time - ORDER_CANCEL_BEFORE_END)
    #     logger.info(
    #         f"Cancelling resting buy orders at {cancel_time} "
    #         f"(3.5 min before cycle end {datetime.fromtimestamp(self.cycle_end_time)})"
    #     )

    #     for order_id in list(self._active_buy_order_ids):
    #         order = self.orders.get(order_id)
    #         if order is None:
    #             continue
    #         if order.status in ('filled', 'MATCHED', 'cancelled'):
    #             continue  # already done

    #         if self.dry_run:
    #             logger.info(f"[DRY RUN] Would cancel buy order {order_id} @ {order.price}")
    #             order.status = "cancelled"
    #         else:
    #             try:
    #                 await self.bot.cancel_order(order_id)
    #                 logger.info(f"Cancelled buy order {order_id} @ {order.price}")
    #                 order.status = "cancelled"
    #             except Exception as e:
    #                 logger.warning(f"Failed to cancel order {order_id}: {e}")

    # ------------------------------------------------------------------
    # Market entry
    # ------------------------------------------------------------------

    async def _try_enter_next_market(self) -> None:
        """
        Look for the next upcoming token 5-min market and place grid orders
        if we haven't already entered that cycle.
        """
        if self._entering:
            return

        market = self.gamma.get_next_market(self.ticker)
        if not market:
            return
        if not market.get("acceptingOrders", False):
            return

        # Compute unique cycle key from market end time
        end_date_iso = market.get("endDate", "")
        try:
            if end_date_iso.endswith("Z"):
                end_date_iso = end_date_iso[:-1] + "+00:00"
            cycle_end = datetime.fromisoformat(end_date_iso).timestamp()
        except Exception:
            cycle_end = time.time() + (MARKET_DURATION * 60)

        if cycle_end in self.entered_cycles:
            return

        # Mark immediately to block concurrent ticks
        self.entered_cycles.add(cycle_end)
        self._entering = True

        try:
            slug = market.get("slug")
            logger.info(
                f"Pre-placing lowball grid orders for next market: {slug} "
                f"(ends {datetime.fromtimestamp(cycle_end)})"
            )

            token_ids = self.gamma.parse_token_ids(market)
            order_amount_usd = self.order_amount_usd

            # GTD expiration: orders expire 3.5 min before cycle end
            gtd_expiration = int(cycle_end) - ORDER_CANCEL_BEFORE_END

            # Total cost = order_amount_usd per price level × 4 levels × 2 sides
            total_cost = sum(order_amount_usd for _ in BUY_PRICES) * 2

            # Pre-flight balance check
            if not self.dry_run:
                balance = await self.bot.get_balance()
                if balance < total_cost:
                    warn_msg = (
                        f"Insufficient balance: ${balance:.2f} available, "
                        f"${total_cost:.2f} required "
                        f"(${order_amount_usd:.2f} × {len(BUY_PRICES)} levels × 2 sides). "
                        f"Skipping cycle."
                    )
                    logger.warning(warn_msg)
                    if cycle_end not in self._low_balance_notified:
                        self._low_balance_notified.add(cycle_end)
                        await self._notify(f"\u26a0\ufe0f {warn_msg}")
                    self.entered_cycles.discard(cycle_end)
                    return

                logger.info(
                    f"Balance OK: ${balance:.2f} available, ${total_cost:.2f} required. "
                    f"Placing {len(BUY_PRICES) * 2} buy orders ({len(BUY_PRICES)} levels × 2 sides)."
                )
            else:
                logger.info(
                    f"[DRY RUN] Simulating {len(BUY_PRICES) * 2} buy orders "
                    f"(${order_amount_usd:.2f} per level × {len(BUY_PRICES)} levels × 2 sides = ${total_cost:.2f})"
                )

            # Place buy orders on both UP and DOWN tokens for each price level
            new_order_ids: List[str] = []
            for side_name, token_id in [("UP", token_ids["up"]), ("DOWN", token_ids["down"])]:
                for buy_price in BUY_PRICES:
                    size = round(order_amount_usd / buy_price, 2)
                    logger.info(
                        f"Placing BUY {side_name} {size:.2f} shares @ {buy_price} "
                        f"(cost ≈ ${size * buy_price:.2f})"
                    )
                    order = await self.place_order(
                        token_id=token_id,
                        price=buy_price,
                        size=size,
                        side="BUY",
                        order_type="GTD",
                        expiration=gtd_expiration,
                    )
                    if order:
                        self._order_buy_price[order.order_id] = buy_price
                        new_order_ids.append(order.order_id)

            self._active_buy_order_ids = new_order_ids
            self._buys_cancelled = False
            self.cycle_end_time = cycle_end
            self.cycle_active = True
            self.current_market = market
            self.token_ids = token_ids

            cancel_at = datetime.fromtimestamp(cycle_end - ORDER_CANCEL_BEFORE_END)
            logger.info(
                f"Grid orders placed. Orders will be cancelled at {cancel_at} "
                f"(3.5 min before cycle end {datetime.fromtimestamp(cycle_end)})."
            )



        finally:
            self._entering = False

    # ------------------------------------------------------------------
    # Order updates
    # ------------------------------------------------------------------

    async def _handle_buy_fill(self, order: OrderInfo, buy_price: float) -> None:
        """
        Handle post-fill logic for BUY orders in the background:
        - Notify Telegram
        - Wait 3 seconds
        - Place 4 matching SELL orders
        - Update position tracking
        """
        # Notify Telegram immediately on fill
        targets_str = " / ".join(str(t) for t in self.sell_targets)
        now_str = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
        await self._notify(
            f"\U0001f7e1 <b>BUY FILLED at {now_str}</b>\n"
            f"Token: {order.token_id[:20]}...\n"
            f"Buy price: <b>{buy_price}</b>\n"
            f"Sell targets: <b>{targets_str}</b>\n"
            f"Size: {order.size_matched:.2f} shares"
        )

        # Wait before placing sell order (non-blocking sleep)
        await asyncio.sleep(SELL_DELAY_SECONDS)


        total_sell_size = math.floor(order.size_matched * 100) / 100.0

        chunk_size_raw = order.size_matched / len(self.sell_targets)
        chunk_size = math.floor(chunk_size_raw * 100) / 100.0


        if total_sell_size < 5:
            logger.info(f"Total sell size {total_sell_size:.2f} is less than 5, skipping all SELL orders")
        else:
            sell_order_sizes = [chunk_size] * len(self.sell_targets)

            while sell_order_sizes[-1] < 5:
                sell_order_sizes[-2] += sell_order_sizes[-1]
                sell_order_sizes[-2] = math.floor(sell_order_sizes[-2] * 100) / 100.0
                sell_order_sizes.pop()

            tasks = [asyncio.create_task(self.place_sell_target(sp, chunk_size, order, total_sell_size, buy_price)) for sp, chunk_size in zip(self.sell_targets, sell_order_sizes)]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            # Track the position
            self.add_position(Position(
                token_id=order.token_id,
                side='BUY',
                size=total_sell_size,
                entry_price=buy_price,
            ))

    async def place_sell_target(self, sell_price: float, chunk_size: float, order: OrderInfo, total_sell_size: float, buy_price: float):
        logger.info(
            f"Placing SELL {chunk_size:.2f} shares @ {sell_price} "
            f"(filled size: {total_sell_size:.4f})"
        )


        sell_order = None
        floored_size = math.floor((chunk_size - 0.05))
        
        
        for attempt in range(SELL_ORDER_RETRY_ATTEMPTS):
            # Try regular size
            sell_order = await self.place_order(
                token_id=order.token_id,
                price=sell_price,
                size=chunk_size,
                side="SELL",
                send_error_to_telegram=False,
            )
            if sell_order:
                break
            
            logger.info(f"Sell try {attempt + 1}/6 failed without flooring, trying with flooring: {floored_size}")
                
            # Try floored size
            sell_order = await self.place_order(
                token_id=order.token_id,
                price=sell_price,
                size=floored_size,
                side="SELL",
                send_error_to_telegram=(attempt == SELL_ORDER_RETRY_ATTEMPTS - 1),
            )
            
            if sell_order:
                break
            
            logger.info(f"Flooring retry {attempt}/6 , sleeping 1 second...")
            await asyncio.sleep(1)

        if sell_order:
            self._order_buy_price[sell_order.order_id] = buy_price
            logger.info(f"Sell order placed: {sell_order.order_id}")
        else:
            logger.warning(f"Failed to place sell order for token {order.token_id} at price {sell_price} (even after size retry)")
            await self._notify(f"\u26a0\ufe0f <b>SELL FAILED:</b> Could not place auto-sell for {order.token_id[:16]}... at price {sell_price} even after retrying.")



    async def on_order_update(self, order: OrderInfo) -> None:
        """
        Called when an order changes status (via sync_orders).

        - BUY filled → place a SELL at buy_price × SELL_MULTIPLIER
        - SELL filled → log profit secured + Telegram notification
        """
        if order.status not in ('filled', 'MATCHED'):
            return

        if order.side == 'BUY':
            buy_price = self._order_buy_price.get(order.order_id, order.price)

            logger.info(
                f"BUY filled! order_id={order.order_id}  buy_price={buy_price}  "
                f"token={order.token_id[:16]}..."
            )

            # Offload the waiting and sell placement logic to a background task
            # so we don't block the main loop (or other order updates).
            asyncio.create_task(self._handle_buy_fill(order, buy_price))

        elif order.side == 'SELL':
            # Try to get exact buy price, fallback to estimating with / 2
            buy_price = self._order_buy_price.get(order.order_id, order.price / 2)
            sell_price = order.price
            profit_estimate = (sell_price - buy_price) * order.size

            logger.info(
                f"SELL filled! order_id={order.order_id}  sell_price={sell_price}  "
                f"profit_estimate=${profit_estimate:.4f}"
            )

            balance = await self.bot.get_balance()

            await self._notify(
                f"\U0001f7e2 <b>SELL FILLED — Profit secured!</b>\n"
                f"Sell price: <b>{sell_price}</b>\n"
                f"Size: {order.size:.2f} shares\n"
                f"Est. profit: <b>${profit_estimate:.4f}</b>\n"
                f"Current balance: <b>${balance:.2f} USDC</b>"
            )

            self.close_position(order.token_id, 'BUY')


# ---------------------------------------------------------------------------
# Entry point Helper
# ---------------------------------------------------------------------------

def run_strategy(ticker: str):
    """Entry point helper for specific ticker strategies."""
    # Update logger name dynamically for better context
    global logger
    logger = logging.getLogger(f"{ticker}_Lowball_Strategy")

    load_dotenv()
    private_key = os.environ.get("POLY_PRIVATE_KEY")
    if not private_key:
        logger.error("POLY_PRIVATE_KEY not set")
        sys.exit(1)

    config = Config.from_env()
    bot = TradingBot(config=config, private_key=private_key)

    if not bot.config.use_gasless:
        logger.warning("Gasless mode not enabled")

    strategy = BaseLowballStrategy(bot, ticker=ticker, params={"check_interval": 1})

    logger.info(f"Starting {ticker} Lowball Grid Strategy (Forever)...")
    logger.info(f"Buy levels: {BUY_PRICES}")
    logger.info(f"Sell targets: {strategy.sell_targets}")
    logger.info(f"Orders cancel at: cycle_end − {ORDER_CANCEL_BEFORE_END}s (3.5 min before end)")

    try:
        asyncio.run(strategy.run(token_ids=[], duration=None))
    except KeyboardInterrupt:
        print("\nStopping...")

