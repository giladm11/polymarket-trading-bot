#!/usr/bin/env python3
"""
Crypto Target Lowball Strategy

Runs continuously on 5-min BTC/ETH/SOL/XRP markets.

Logic:
  - Connects to the Polymarket Market WebSocket to receive live
    last_trade_price events for the current tokens.
  - Connects to the Polymarket RTDS (wss://ws-live-data.polymarket.com)
    and subscribes to crypto_prices_chainlink for live BTC/ETH/SOL/XRP
    prices — the exact same Chainlink feed Polymarket resolves against.
  - Parses the "strike price" (target) from the market question
    (e.g. "Will BTC be above $84,950 at 11:20 PM?").
  - In the LAST MINUTE (60s → 20s before cycle end):
      If |current_price − target_price| <= threshold → place BUY orders
      on BOTH UP and DOWN tokens at `buy_price` (default 0.10).
  - On BUY fill → place SELL at `sell_price` (default 0.70).
  - All thresholds, buy_price and sell_price are configurable at runtime
    via Telegram commands.

Thresholds (default):
  BTC  $20.00
  ETH   $1.00
  SOL   $0.50
  XRP   $0.05

Usage:
  python strategy/lowball/crypto_target_lowball.py BTC
  python strategy/lowball/crypto_target_lowball.py ETH
"""

import sys
import asyncio
import logging
import time
import os
import re
import math
import json
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from dotenv import load_dotenv

from src.bot import TradingBot
from src.gamma_client import GammaClient
from src.config import Config
from examples.strategy_example import BaseStrategy, Position, OrderInfo, StrategyStatus

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MARKET_DURATION = 5   # minutes per cycle
SELL_DELAY_SECONDS = 0
SELL_ORDER_RETRY_ATTEMPTS = 7
START_ORDERS_SECONDS_BEFORE_CLOSE = 60
STOP_ORDERS_SECONDS_BEFORE_CLOSE = 20

# Default thresholds: how close (in USD) the current price must be to the
# target price to trigger an order.
DEFAULT_THRESHOLDS: Dict[str, float] = {
    "BTC": 15.0,
    "ETH": 1.0,
    "SOL": 0.5,
    "XRP": 0.05,
}

# Polymarket RTDS — Chainlink crypto price stream (no auth required)
POLY_RTDS_WS_URL = "wss://ws-live-data.polymarket.com"

# Chainlink symbol names on the RTDS feed
CHAINLINK_SYMBOLS: Dict[str, str] = {
    "BTC": "btc/usd",
    "ETH": "eth/usd",
    "SOL": "sol/usd",
    "XRP": "xrp/usd",
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("CryptoTargetLowball")


# ---------------------------------------------------------------------------
# Telegram Notifier
# ---------------------------------------------------------------------------

class TelegramNotifier:
    """Sends messages to a Telegram group via Bot API, with command polling."""

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = str(chat_id)
        self._base_url = f"https://api.telegram.org/bot{token}"
        self._last_update_id = 0

    async def send(self, text: str, chat_id: str = None) -> bool:
        import urllib.request
        url = f"{self._base_url}/sendMessage"
        payload = json.dumps({
            "chat_id": chat_id or self.chat_id,
            "text": text,
            "parse_mode": "HTML",
        }).encode("utf-8")
        try:
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"}, method="POST"
            )
            await asyncio.to_thread(urllib.request.urlopen, req, timeout=10)
            return True
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")
            return False

    async def get_updates(self) -> list:
        import urllib.request
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
# Polymarket RTDS — Chainlink price feed
# ---------------------------------------------------------------------------

class PolymarketRTDSFeed:
    """
    Connects to the Polymarket Real-Time Data Socket (RTDS) and subscribes
    to the crypto_prices_chainlink topic for a single symbol.

    No API key required. Provides the exact Chainlink price that
    Polymarket uses to resolve its 5-min crypto markets.

    Endpoint : wss://ws-live-data.polymarket.com
    Symbols  : btc/usd  eth/usd  sol/usd  xrp/usd
    """

    def __init__(self, symbol: str):
        """
        Args:
            symbol: Chainlink symbol, e.g. "btc/usd"
        """
        self._symbol = symbol.lower()
        self._price: Optional[float] = None
        self._last_update: float = 0.0
        self._ws = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        # Optional callback: called with (price: float) on every new price tick
        self._on_price_update = None
        self._history: List[tuple] = []
        self._on_health_change = None
        self._health_alert_sent = False

    def set_on_price_update(self, callback) -> None:
        """Register a callback invoked with the new price on every RTDS update."""
        self._on_price_update = callback

    def set_on_health_change(self, callback) -> None:
        """Register a callback invoked with (is_healthy: bool) on disconnects and reconnects."""
        self._on_health_change = callback

    def get_price(self) -> Optional[float]:
        """Return the latest Chainlink price, or None if not yet received."""
        return self._price

    def get_price_at(self, ts: float) -> Optional[float]:
        """Return the Chainlink price closest to the given Unix timestamp.

        Uses the stored history from the subscribe backfill + live updates.
        Returns None if no history is available.
        """
        if not self._history:
            return None
        # Find the entry with the smallest time difference
        closest = min(self._history, key=lambda x: abs(x[0] - ts))
        gap = abs(closest[0] - ts)
        if gap > 600:  # more than 10 minutes away — not reliable
            logger.warning(
                f"[RTDS] get_price_at({ts:.0f}): closest entry is {gap:.0f}s away"
            )
        return closest[1]

    async def connect(self, max_retries: int = -1) -> bool:
        """Connect to RTDS and start streaming Chainlink prices.

        Runs a persistent background task that automatically reconnects
        with exponential backoff if the connection is dropped or rejected.
        """
        self._running = True
        self._task = asyncio.create_task(self._connection_manager())
        # Wait briefly to let the initial connection attempt run
        await asyncio.sleep(1)
        return True

    async def _connection_manager(self) -> None:
        import websockets
        
        attempt = 0
        was_connected = False

        while self._running:
            attempt += 1
            try:
                self._ws = await asyncio.wait_for(
                    websockets.connect(POLY_RTDS_WS_URL), timeout=10.0
                )
                
                # Subscribe to the full topic
                sub_msg = json.dumps({
                    "action": "subscribe",
                    "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*"}]
                })
                await self._ws.send(sub_msg)
                
                logger.info(
                    f"[RTDS] Connected for {self._symbol.upper()} — listening for prices"
                )
                
                if was_connected is False and attempt > 1:
                    logger.info(f"Polymarket RTDS reconnected after {attempt-1} failed attempts.")
                
                # If we previously sent a down alert, send an up alert
                if self._health_alert_sent:
                    self._health_alert_sent = False
                    if self._on_health_change:
                        try:
                            res = self._on_health_change(True)
                            if asyncio.iscoroutine(res):
                                asyncio.create_task(res)
                        except Exception as cb_err:
                            logger.debug(f"Health callback error: {cb_err}")

                attempt = 0  # reset on successful connect
                was_connected = True

                # Block here reading messages until disconnected
                await self._read_loop()

            except Exception as e:
                # Disconnected or failed to connect
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

                if not self._running:
                    break
                
                if was_connected:
                    was_connected = False
                    logger.warning(f"Polymarket RTDS disconnected: {e}. Reconnecting...")
                
                # If we've failed 3 times, and haven't alerted yet, send alert
                if attempt >= 3 and not self._health_alert_sent:
                    self._health_alert_sent = True
                    if self._on_health_change:
                        try:
                            res = self._on_health_change(False)
                            if asyncio.iscoroutine(res):
                                asyncio.create_task(res)
                        except Exception as cb_err:
                            logger.debug(f"Health callback error: {cb_err}")
                
                # Backoff: 1s, 2s, 4s... capped at 10s
                wait_time = min(10.0, 2.0 ** (attempt - 1))
                logger.warning(
                    f"Polymarket RTDS connect failed (attempt {attempt}): {e} "
                    f"— retrying in {wait_time:.1f}s..."
                )
                await asyncio.sleep(wait_time)

    async def _read_loop(self) -> None:
        try:
            async for raw_msg in self._ws:
                if not self._running:
                    break
                # Heartbeat
                if raw_msg == "PING":
                    try:
                        await self._ws.send("PONG")
                        self._last_update = time.time()
                    except Exception:
                        pass
                    continue
                try:
                    data = json.loads(raw_msg)
                    topic = data.get("topic", "")
                    msg_type = data.get("type", "")
                    payload = data.get("payload", {})

                    logger.debug(f"[RTDS] raw topic={topic} type={msg_type}")

                    # Both 'crypto_prices' and 'crypto_prices_chainlink' carry price data
                    if topic in ("crypto_prices", "crypto_prices_chainlink"):

                        if msg_type == "subscribe":
                            # Subscribe confirmation — server sends historical backfill in payload.data[]
                            price_data = payload.get("data", [])
                            symbol = payload.get("symbol", "").lower()
                            if price_data and symbol == self._symbol:
                                # Store full backfill in history (timestamp is ms — convert to seconds)
                                for entry in price_data:
                                    ts_s = entry["timestamp"] / 1000.0
                                    val = entry.get("value")
                                    if val is not None:
                                        self._history.append((ts_s, float(val)))

                                latest = price_data[-1]
                                value = latest.get("value")
                                if value is not None:
                                    self._price = float(value)
                                    self._last_update = time.time()
                                    logger.debug(
                                        f"[RTDS-CL] {self._symbol.upper()} initial price = "
                                        f"${self._price:,.4f} "
                                        f"(from subscribe backfill, {len(price_data)} ticks)"
                                    )
                                    if self._on_price_update:
                                        try:
                                            self._on_price_update(self._price)
                                        except Exception as cb_err:
                                            logger.debug(f"Price callback error: {cb_err}")

                        elif msg_type == "update":
                            # Real-time single-price update
                            symbol = payload.get("symbol", "").lower()
                            if symbol == self._symbol:
                                value = payload.get("value")
                                if value is not None:
                                    self._price = float(value)
                                    self._last_update = time.time()
                                    # Append to history for future get_price_at() lookups
                                    ts_s = data.get("timestamp", time.time() * 1000) / 1000.0
                                    self._history.append((ts_s, self._price))
                                    logger.debug(
                                        f"[RTDS-CL] {self._symbol.upper()} = "
                                        f"${self._price:,.4f}"
                                    )
                                    if self._on_price_update:
                                        try:
                                            self._on_price_update(self._price)
                                        except Exception as cb_err:
                                            logger.debug(f"Price callback error: {cb_err}")
                        else:
                            logger.debug(f"[RTDS] unhandled: topic={topic!r} type={msg_type!r}")
                    else:
                        logger.debug(f"[RTDS] received: topic={topic!r} type={msg_type!r}")

                except Exception as e:
                    logger.debug(f"RTDS parse error: {e}")
        except Exception as e:
            logger.debug(f"RTDS read loop ended: {e}")
        finally:
            self._running = False


    async def disconnect(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None


# ---------------------------------------------------------------------------
# Target price parser
# ---------------------------------------------------------------------------

def parse_target_price_from_question(question: str) -> Optional[float]:
    """
    Extract the USD strike price from a Polymarket question string.

    Examples:
      "Will BTC be above $84,950 at 11:20 PM?"   → 84950.0
      "Will ETH be above $2,600 on Feb 26?"       → 2600.0
      "Will SOL be above $150.50 at close?"        → 150.5
      "Will XRP be above $2.35 at close?"          → 2.35
    """
    # Match dollar amounts like $84,950 or $150.50 or $2.35
    match = re.search(r'\$([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)', question)
    if match:
        raw = match.group(1).replace(",", "")
        try:
            return float(raw)
        except ValueError:
            pass
    # Fallback: plain number anywhere after "above"
    match2 = re.search(r'above\s+([0-9]+(?:\.[0-9]+)?)', question, re.IGNORECASE)
    if match2:
        try:
            return float(match2.group(1))
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class CryptoTargetLowballStrategy(BaseStrategy):
    """
    Places BUY orders at the end of a 5-min window when the current
    asset price is close to the market's target (strike) price.

    Price data comes from:
      - Polymarket RTDS crypto_prices_chainlink (live Chainlink BTC/ETH/SOL/XRP)
      - Polymarket Market WebSocket CLOB (token last_trade_price events)
    Target price:
      - Parsed from the market question string (e.g. "Will BTC be above $84,950?")
    """

    def __init__(self, bot: TradingBot, ticker: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(bot, params or {}, name=f"{ticker}CryptoTarget")
        self.ticker = ticker.upper()
        self.config_file = Path(__file__).parent / f"bot_config_crypto_target_{self.ticker.lower()}.json"

        self.gamma = GammaClient(duration_minutes=MARKET_DURATION)
        self.current_market: Optional[Dict[str, Any]] = None
        self.token_ids: Dict[str, str] = {}

        self.cycle_end_time: float = 0.0
        self.cycle_active: bool = False
        self._orders_placed_this_cycle: bool = False
        self._target_price: float = 0.0

        # Telegram
        tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "")
        self.telegram: Optional[TelegramNotifier] = (
            TelegramNotifier(tg_token, tg_chat) if tg_token and tg_chat else None
        )
        if self.telegram:
            logger.info("Telegram notifications enabled.")

        # ------------------------------------------------------------------
        # Runtime-configurable parameters (loaded from config or env)
        # ------------------------------------------------------------------
        self.order_amount_usd: float = self._load_config_value(
            "order_amount_usd", float(os.environ.get("ORDER_AMOUNT_USD", "1"))
        )
        # List of {"buy": price, "sell": target} levels — all placed on trigger
        self.price_levels: List[Dict[str, float]] = self._load_price_levels()

        self.threshold: float = self._load_config_value(
            "threshold", DEFAULT_THRESHOLDS.get(self.ticker, 1.0)
        )

        # Dry-run mode
        self.dry_run: bool = os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes")
        if self.dry_run:
            logger.warning("*** DRY RUN MODE — orders will NOT be submitted ***")

        # Chainlink price feed via Polymarket RTDS
        chainlink_symbol = CHAINLINK_SYMBOLS.get(self.ticker, f"{self.ticker.lower()}/usd")
        self._rtds_feed = PolymarketRTDSFeed(symbol=chainlink_symbol)
        self._chainlink_symbol = chainlink_symbol

        # Track which cycles we've already entered
        self.entered_cycles: set = set()

        # Per-order metadata: buy price and sell target for each open order
        self._order_buy_price: Dict[str, float] = {}
        self._order_sell_price: Dict[str, float] = {}

        # Balance reporting
        self._last_balance_report: float = 0.0
        self.balance_report_interval: float = self._load_config_value(
            "balance_report_interval", 3600.0
        )

    # ------------------------------------------------------------------
    # Persistent config
    # ------------------------------------------------------------------

    def _load_config_value(self, key: str, default):
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

    def _load_price_levels(self) -> List[Dict[str, float]]:
        """Load price levels from config, falling back to sensible defaults."""
        default = [{"buy": 0.10, "sell": 0.70}, {"buy": 0.05, "sell": 0.20}]
        try:
            if self.config_file.exists():
                data = json.loads(self.config_file.read_text(encoding="utf-8"))
                levels = data.get("price_levels")
                if isinstance(levels, list) and levels:
                    parsed = [{"buy": float(l["buy"]), "sell": float(l["sell"])} for l in levels]
                    logger.info(f"Loaded {len(parsed)} price level(s) from config")
                    return parsed
        except Exception as e:
            logger.warning(f"Could not load price_levels from config: {e}")
        return default

    def _save_config(self) -> None:
        try:
            data = {}
            if self.config_file.exists():
                try:
                    data = json.loads(self.config_file.read_text(encoding="utf-8"))
                except Exception:
                    pass
            data["order_amount_usd"] = self.order_amount_usd
            data["price_levels"] = self.price_levels
            data["threshold"] = self.threshold
            data["balance_report_interval"] = self.balance_report_interval
            self.config_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
            logger.info(f"Config saved to {self.config_file.name}")
        except Exception as e:
            logger.warning(f"Could not save config: {e}")

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
    ) -> Optional[OrderInfo]:
        if self.dry_run:
            logger.info(
                f"[DRY RUN] Would place {order_type} {side} {size:.2f} @ {price} "
                f"(token: {token_id[:16]}...)"
            )
            fake = OrderInfo(
                order_id=f"dryrun_{int(time.time()*1000)}_{price}",
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                status="pending",
            )
            self.orders[fake.order_id] = fake
            await asyncio.sleep(0.05)
            return fake

        result = await self.bot.place_order(
            token_id=token_id, price=price, size=size,
            side=side, order_type=order_type, expiration=expiration,
        )
        if not result or not result.success or not result.order_id:
            import html
            error_msg = result.message if result else "no result"
            logger.warning(f"Order failed: {error_msg} ({side} {size} @ {price})")
            if send_error_to_telegram:
                safe = html.escape(str(error_msg))
                await self._notify(
                    f"❌ <b>ORDER FAILED:</b> {safe}\nSide: {side}\nSize: {size}\nPrice: {price}"
                )
            return None

        order_info = OrderInfo(
            order_id=result.order_id, token_id=token_id, side=side,
            price=price, size=size, status="pending",
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
        balance = await self.bot.get_balance()
        prefix = f"[{context}] " if context else ""
        dry = " [DRY RUN]" if self.dry_run else ""
        msg = f"💰 {prefix}Current balance: <b>${balance:.2f} USDC</b>{dry}"
        logger.info(msg)
        await self._notify(msg)
        self._last_balance_report = time.time()

    # ------------------------------------------------------------------
    # Telegram command handler
    # ------------------------------------------------------------------

    async def _handle_telegram_commands(self) -> None:
        if not self.telegram:
            return

        dry_tag = " <i>[DRY RUN]</i>" if self.dry_run else ""
        HELP = (
            f"🤖 <b>[{self.name}] Available commands:</b>\n"
            "/balance — current USDC balance\n"
            "/price — live Chainlink price (via Polymarket RTDS)\n"
            "/target — current market target price + current diff\n"
            "/size — current order size in USD\n"
            "/setsize &lt;amount&gt; — e.g. /setsize 2\n"
            "/levels — list all buy/sell price levels\n"
            "/addlevel &lt;buy&gt; &lt;sell&gt; — e.g. /addlevel 0.03 0.15\n"
            "/removelevel &lt;index&gt; — e.g. /removelevel 1\n"
            "/setlevel &lt;index&gt; &lt;buy&gt; &lt;sell&gt; — e.g. /setlevel 0 0.10 0.80\n"
            "/threshold — current price threshold\n"
            f"/setthreshold &lt;value&gt; — e.g. /setthreshold 20  (default: {DEFAULT_THRESHOLDS.get(self.ticker, 1.0)})\n"
            "/interval — balance report interval\n"
            "/setinterval &lt;minutes&gt; — e.g. /setinterval 60\n"
            "/help — this message"
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

                    # ---- balance ----
                    if command == "/balance":
                        balance = await self.bot.get_balance()
                        reply = (
                            f"<b>[{self.name}]</b> 💰 Balance: "
                            f"<b>${balance:.2f} USDC</b>{dry_tag}"
                        )

                    # ---- price ----
                    elif command == "/price":
                        p = self._rtds_feed.get_price()
                        if p is not None:
                            reply = (
                                f"<b>[{self.name}]</b> 📊 {self.ticker} Chainlink price: "
                                f"<b>${p:,.4f}</b> (Polymarket RTDS)"
                            )
                        else:
                            reply = f"<b>[{self.name}]</b> ⚠️ No Chainlink price yet — RTDS connecting..."

                    # ---- target ----
                    elif command == "/target":
                        if self._target_price > 0:
                            p = self._rtds_feed.get_price()
                            diff = abs(p - self._target_price) if p is not None else None
                            diff_str = f" | Δ = <b>${diff:,.4f}</b> (threshold ${self.threshold})" if diff is not None else ""
                            reply = (
                                f"<b>[{self.name}]</b> 🎯 Target: "
                                f"<b>${self._target_price:,.2f}</b>{diff_str}"
                            )
                        else:
                            reply = f"<b>[{self.name}]</b> ⚠️ No active cycle or target not yet parsed."

                    # ---- size ----
                    elif command == "/size":
                        reply = (
                            f"<b>[{self.name}]</b> 📏 Order size: "
                            f"<b>${self.order_amount_usd:.2f} USD</b> per side{dry_tag}"
                        )

                    elif command == "/setsize":
                        if not args:
                            reply = "❌ Usage: /setsize &lt;amount&gt;"
                        else:
                            try:
                                new_size = float(args[0])
                                if new_size <= 0:
                                    raise ValueError("must be positive")
                                old = self.order_amount_usd
                                self.order_amount_usd = new_size
                                self._save_config()
                                reply = (
                                    f"<b>[{self.name}]</b> ✅ Order size: "
                                    f"<b>${old:.2f}</b> → <b>${new_size:.2f}</b>{dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"❌ {e}"

                    # ---- price levels ----
                    elif command == "/levels":
                        if not self.price_levels:
                            reply = f"<b>[{self.name}]</b> ⚠️ No price levels configured."
                        else:
                            lines = [f"<b>[{self.name}]</b> 📊 Price levels ({len(self.price_levels)} total):{dry_tag}"]
                            for i, lvl in enumerate(self.price_levels):
                                size = round(self.order_amount_usd / lvl['buy'], 2)
                                lines.append(
                                    f"  [{i}] buy=<b>{lvl['buy']:.2f}</b> ({size:.2f} shares) → sell=<b>{lvl['sell']:.2f}</b>"
                                )
                            reply = "\n".join(lines)

                    elif command == "/addlevel":
                        if len(args) < 2:
                            reply = "❌ Usage: /addlevel &lt;buy&gt; &lt;sell&gt;  (e.g. /addlevel 0.03 0.15)"
                        else:
                            try:
                                bp = float(args[0])
                                sp = float(args[1])
                                if not (0 < bp < 1) or not (0 < sp < 1):
                                    raise ValueError("both must be between 0 and 1")
                                self.price_levels.append({"buy": bp, "sell": sp})
                                self._save_config()
                                reply = (
                                    f"<b>[{self.name}]</b> ✅ Added level [{len(self.price_levels)-1}]: "
                                    f"buy=<b>{bp:.2f}</b> → sell=<b>{sp:.2f}</b>{dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"❌ {e}"

                    elif command == "/removelevel":
                        if not args:
                            reply = "❌ Usage: /removelevel &lt;index&gt;"
                        else:
                            try:
                                idx = int(args[0])
                                if idx < 0 or idx >= len(self.price_levels):
                                    raise ValueError(f"index {idx} out of range (0–{len(self.price_levels)-1})")
                                if len(self.price_levels) == 1:
                                    raise ValueError("cannot remove the last level")
                                removed = self.price_levels.pop(idx)
                                self._save_config()
                                reply = (
                                    f"<b>[{self.name}]</b> ✅ Removed level [{idx}]: "
                                    f"buy={removed['buy']:.2f} → sell={removed['sell']:.2f}{dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"❌ {e}"

                    elif command == "/setlevel":
                        if len(args) < 3:
                            reply = "❌ Usage: /setlevel &lt;index&gt; &lt;buy&gt; &lt;sell&gt;  (e.g. /setlevel 0 0.10 0.80)"
                        else:
                            try:
                                idx = int(args[0])
                                bp = float(args[1])
                                sp = float(args[2])
                                if idx < 0 or idx >= len(self.price_levels):
                                    raise ValueError(f"index {idx} out of range (0–{len(self.price_levels)-1})")
                                if not (0 < bp < 1) or not (0 < sp < 1):
                                    raise ValueError("both prices must be between 0 and 1")
                                old = self.price_levels[idx]
                                self.price_levels[idx] = {"buy": bp, "sell": sp}
                                self._save_config()
                                reply = (
                                    f"<b>[{self.name}]</b> ✅ Level [{idx}] updated: "
                                    f"buy={old['buy']:.2f}→<b>{bp:.2f}</b>  "
                                    f"sell={old['sell']:.2f}→<b>{sp:.2f}</b>{dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"❌ {e}"

                    # ---- threshold ----
                    elif command == "/threshold":
                        reply = (
                            f"<b>[{self.name}]</b> ⚖️ Price threshold: "
                            f"<b>${self.threshold}</b> USD{dry_tag}"
                        )

                    elif command == "/setthreshold":
                        if not args:
                            reply = "❌ Usage: /setthreshold &lt;value&gt;"
                        else:
                            try:
                                new_t = float(args[0])
                                if new_t <= 0:
                                    raise ValueError("must be positive")
                                old = self.threshold
                                self.threshold = new_t
                                self._save_config()
                                reply = (
                                    f"<b>[{self.name}]</b> ✅ Threshold: "
                                    f"<b>${old}</b> → <b>${new_t}</b>{dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"❌ {e}"

                    # ---- interval ----
                    elif command == "/interval":
                        reply = (
                            f"<b>[{self.name}]</b> ⏱ Balance report interval: "
                            f"<b>{self.balance_report_interval/60:.0f} min</b>{dry_tag}"
                        )

                    elif command == "/setinterval":
                        if not args:
                            reply = "❌ Usage: /setinterval &lt;minutes&gt;"
                        else:
                            try:
                                new_m = float(args[0])
                                if new_m <= 0:
                                    raise ValueError("must be positive")
                                old_m = self.balance_report_interval / 60
                                self.balance_report_interval = new_m * 60
                                self._save_config()
                                reply = (
                                    f"<b>[{self.name}]</b> ✅ Interval: "
                                    f"<b>{old_m:.0f} min</b> → <b>{new_m:.0f} min</b>{dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"❌ {e}"

                    elif command in ("/help", "/start"):
                        reply = HELP
                    else:
                        reply = HELP

                    await self.telegram.send(reply, chat_id=chat_id)

            except Exception as e:
                logger.debug(f"Telegram command handler error: {e}")

            await asyncio.sleep(2)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        await super().initialize()
        await self._rtds_feed.connect()

        # Register price callback: every Chainlink push logs vs current target
        def _on_chainlink_price(price: float) -> None:
            target = self._target_price
            if target > 0:
                diff = abs(price - target)
                tag = "\u2705 WITHIN" if diff <= self.threshold else "\u274c outside"
                logger.debug(
                    f"[RTDS-CL] {self._chainlink_symbol.upper()}  "
                    f"price=${price:,.4f}  "
                    f"target=${target:,.2f}  "
                    f"diff=${diff:.4f}  "
                    f"threshold=${self.threshold}  {tag}"
                )
        self._rtds_feed.set_on_price_update(_on_chainlink_price)

        # Register health callback: alert telegram if RTDS fails repeatedly
        async def _on_health(is_up: bool) -> None:
            if is_up:
                await self._notify("🟢 <b>RTDS Reconnected:</b> Data stream is back online.")
            else:
                await self._notify("🔴 <b>RTDS Disconnected:</b> Data stream has failed 3+ times. Retrying in background...")
        self._rtds_feed.set_on_health_change(_on_health)

        dry = " [DRY RUN]" if self.dry_run else ""
        await self._report_balance(f"{self.name} started{dry}")

    async def cleanup(self) -> None:
        await self._rtds_feed.disconnect()
        dry = " [DRY RUN]" if self.dry_run else ""
        await self._report_balance(f"{self.name} stopped{dry}")
        await super().cleanup()

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    async def run(self, token_ids: List[str] = None, duration: int = None):
        await self.initialize()

        async def _ws_health_check_loop():
            # Check every 30 seconds that we are still receiving info from the websocket
            while self.status == StrategyStatus.RUNNING:
                await asyncio.sleep(30)  # 30 seconds
                now = time.time()
                # If no update (price or ping) for 30 seconds, restart it
                if self._rtds_feed._last_update > 0 and (now - self._rtds_feed._last_update) >= 30:
                    logger.warning("[HEALTH] No RTDS ws data for 30 seconds. Restarting...")
                    if self.telegram:
                        await self._notify("⚠️ <b>RTDS Health Check:</b> No data received for 30s. Restarting stream...")
                    await self._rtds_feed.disconnect()
                    # small wait to let it unbind
                    await asyncio.sleep(2)
                    await self._rtds_feed.connect()

        async def _strategy_loop():
            start_time = time.time()
            try:
                while self.status == StrategyStatus.RUNNING:
                    if duration and (time.time() - start_time) > duration:
                        break

                    await self.sync_orders()
                    await self.on_tick({})

                    if time.time() - self._last_balance_report >= self.balance_report_interval:
                        interval_mins = self.balance_report_interval / 60
                        await self._report_balance(f"Update (every {interval_mins:.0f}min)")

                    await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                pass
            finally:
                self.status = StrategyStatus.STOPPED
                await self.cleanup()

        await asyncio.gather(
            _strategy_loop(),
            self._handle_telegram_commands(),
            _ws_health_check_loop(),
        )

    # ------------------------------------------------------------------
    # Tick logic
    # ------------------------------------------------------------------

    async def on_tick(self, _: Dict[str, Any]) -> None:
        now = time.time()

        # ── 1. Detect cycle end → reset ──────────────────────────────
        if self.cycle_active and now >= self.cycle_end_time:
            logger.info("Cycle ended. Resetting state.")
            self.cycle_active = False
            self.current_market = None
            self.token_ids = {}
            self._target_price = 0.0
            self._orders_placed_this_cycle = False
            # Drop unfilled orders
            for order_id in list(self.orders.keys()):
                if self.orders[order_id].status not in ('filled', 'MATCHED', 'cancelled'):
                    del self.orders[order_id]
            self._order_buy_price.clear()

        # ── 2. Try to enter the current active market ─────────────────
        await self._try_enter_market()

        # ── 3. Price + decision logging ────────────────────────────────
        if self.cycle_active and self._target_price > 0:
            current_price = self._rtds_feed.get_price()
            secs_left = max(0.0, self.cycle_end_time - now)
            in_window = (
                now >= self.cycle_end_time - START_ORDERS_SECONDS_BEFORE_CLOSE 
                and now < self.cycle_end_time - STOP_ORDERS_SECONDS_BEFORE_CLOSE
            )

            # Collect Polymarket token prices from CLOB WS
            window_tag = f"IN WINDOW ({secs_left:.0f}s left)" if in_window else f"waiting ({secs_left:.0f}s left)"

            if current_price is not None:
                diff = abs(current_price - self._target_price)
                threshold_tag = "\u2705 WITHIN" if diff <= self.threshold else "\u274c outside"
                logger.debug(
                    f"[TICK] {self.ticker}  "
                    f"Chainlink=${current_price:,.4f}  "
                    f"target=${self._target_price:,.2f}  "
                    f"diff=${diff:.4f}  "
                    f"threshold=${self.threshold}  "
                    f"{threshold_tag}  | {window_tag}"
                )
            else:
                logger.debug(
                    f"[TICK] {self.ticker}  "
                    f"Chainlink=n/a (RTDS connecting...)  "
                    f"target=${self._target_price:,.2f}  "
                    f"| {secs_left:.0f}s left"
                )

            # ── 4. Trigger: fire orders when conditions are met ─────────
            if (
                in_window
                and not self._orders_placed_this_cycle
                and current_price is not None
            ):
                diff = abs(current_price - self._target_price)
                if diff <= self.threshold:
                    logger.info(
                        f"[TRIGGER] ✅ diff=${diff:.4f} <= ${self.threshold} "
                        f"— placing BUY orders now!"
                    )
                    self._orders_placed_this_cycle = True
                    asyncio.create_task(self._place_close_orders(current_price))
                # (no else needed — already logged above in the TICK line)

    # ------------------------------------------------------------------
    # Market entry
    # ------------------------------------------------------------------

    async def _try_enter_market(self) -> None:
        """
        Find & track the current active 5-min market.
        The target price is the live Chainlink price at the moment the window opens.
        """
        market = self.gamma.get_current_market(self.ticker)
        if not market or not market.get("acceptingOrders", False):
            return

        end_date_iso = market.get("endDate", "")
        try:
            if end_date_iso.endswith("Z"):
                end_date_iso = end_date_iso[:-1] + "+00:00"
            cycle_end = datetime.fromisoformat(end_date_iso).timestamp()
        except Exception as e:
            logger.error(f"Failed parsing endDate '{end_date_iso}': {e}")
            return

        if cycle_end in self.entered_cycles:
            return

        # Don't enter a market that has less than 80 seconds remaining
        # (we need to be in before the trigger window starts at 60s)
        now = time.time()
        secs_remaining = cycle_end - now
        if secs_remaining < START_ORDERS_SECONDS_BEFORE_CLOSE + 20:
            logger.info(
                f"Skipping market {market.get('slug')} — only {secs_remaining:.0f}s left "
                f"(need >= {START_ORDERS_SECONDS_BEFORE_CLOSE + 20}s to enter before trigger window)"
            )
            self.entered_cycles.add(cycle_end)  # don't retry this one
            return

        self.entered_cycles.add(cycle_end)
        self.cycle_end_time = cycle_end
        self.cycle_active = True
        self.current_market = market
        self.token_ids = self.gamma.parse_token_ids(market)
        self._orders_placed_this_cycle = False
        self._target_price = 0.0  # will be set by _fetch_price_to_beat

        slug = market.get("slug", "")
        question = market.get("question", "")

        # Parse cycle_start from the market's eventStartTime field
        start_date_iso = (
            market.get("eventStartTime", "")
            or market.get("eventStartDate", "")
            or market.get("startDate", "")
        )
        cycle_start: float = cycle_end - 300  # default: derive from end
        if start_date_iso:
            try:
                if start_date_iso.endswith("Z"):
                    start_date_iso = start_date_iso[:-1] + "+00:00"
                cycle_start = datetime.fromisoformat(start_date_iso).timestamp()
            except Exception:
                pass

        # Fetch the official priceToBeat from the Polymarket events API.
        # This is only populated once the market window has started.
        asyncio.create_task(self._fetch_price_to_beat(slug, cycle_start))

        logger.info(
            f"New cycle started | {slug} | Target: fetching priceToBeat... | "
            f"Threshold: ${self.threshold} | "
            f"Ends: {datetime.fromtimestamp(cycle_end).strftime('%H:%M:%S UTC')} "
            f"({secs_remaining:.0f}s remaining)"
        )

    async def _fetch_price_to_beat(self, slug: str, cycle_start: float) -> None:
        """Fetch the candle open price from Polymarket's crypto-price API.

        URL: https://polymarket.com/api/crypto/crypto-price
             ?symbol=BTC&eventStartTime=...Z&variant=fiveminute&endDate=...Z
        Response field: openPrice  (null while candle is still being built)
        Retries every 4s until available or cycle ends.
        """
        import urllib.request as _urllib
        from urllib.parse import urlencode

        # Wait until the market has actually started (openPrice only appears then)
        now = time.time()
        if now < cycle_start:
            wait_secs = cycle_start - now
            logger.info(
                f"[{slug}] Market starts in {wait_secs:.0f}s — "
                f"will fetch openPrice at {datetime.fromtimestamp(cycle_start).strftime('%H:%M:%S')}"
            )
            await asyncio.sleep(wait_secs)

        # Build URL — timestamps in UTC ISO format with Z suffix
        start_dt = datetime.fromtimestamp(cycle_start, tz=timezone.utc)
        end_dt   = datetime.fromtimestamp(self.cycle_end_time, tz=timezone.utc)
        params = urlencode({
            "symbol":         self.ticker,
            "eventStartTime": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "variant":        "fiveminute",
            "endDate":        end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        url = f"https://polymarket.com/api/crypto/crypto-price?{params}"
        logger.debug(f"[{slug}] Fetching open price: {url}")

        for attempt in range(30):  # poll up to 2 minutes (30 × 4s)
            if not self.cycle_active:
                return  # cycle ended before we got the price

            try:
                def do_fetch():
                    req = _urllib.Request(url, headers={"User-Agent": "polymarket-bot/1.0"})
                    resp = _urllib.urlopen(req, timeout=8)
                    return json.loads(resp.read().decode())

                data = await asyncio.to_thread(do_fetch)
                logger.debug(f"[{slug}] crypto-price API response: {data}")

                open_price = data.get("openPrice") if isinstance(data, dict) else None
                if open_price is not None:
                    self._target_price = float(open_price)
                    logger.info(
                        f"[{slug}] openPrice (target) = ${self._target_price:,.4f} "
                        f"(attempt {attempt + 1})"
                    )
                    return
                else:
                    logger.debug(
                        f"[{slug}] openPrice not yet available "
                        f"(attempt {attempt + 1}) — retrying in 4s"
                    )

            except Exception as e:
                logger.warning(f"[{slug}] crypto-price API error (attempt {attempt + 1}): {e} — retrying in 4s")

            await asyncio.sleep(4)

        msg = f"[{slug}] openPrice not available after 30 attempts — no target price set"
        logger.warning(msg)
        await self._notify(f"⚠️ {msg}")


    # ------------------------------------------------------------------
    # Order placement on trigger
    # ------------------------------------------------------------------

    async def _place_close_orders(self, current_price: float) -> None:
        if not self.price_levels:
            logger.warning("No price levels configured — nothing to place!")
            return

        levels_str = "  ".join(
            f"buy={l['buy']:.2f}→sell={l['sell']:.2f}" for l in self.price_levels
        )
        # Background the notification so it doesn't delay order placement by even a ms
        # asyncio.create_task(self._notify(
        #     f"🎯 <b>TARGET MATCHED — PLACING ORDERS</b>\n"
        #     f"Asset: <b>{self.ticker}</b>\n"
        #     f"Current Price: <b>${current_price:,.4f}</b>\n"
        #     f"Target Price: <b>${self._target_price:,.2f}</b>\n"
        #     f"Diff: <b>${abs(current_price - self._target_price):.4f}</b> "
        #     f"(threshold ${self.threshold})\n"
        #     f"Levels: {levels_str}"
        # ))

        async def place(side_name: str, token_id: str):
            # API GTD security rule: submitted expiration = desired_expiry + 60s.
            # We want orders to physically cancel at (cycle_end - STOP_ORDERS_SECONDS_BEFORE_CLOSE), so:
            buy_expiry = int(self.cycle_end_time) - STOP_ORDERS_SECONDS_BEFORE_CLOSE + 60

            async def place_single(i: int, lvl: dict):
                bp = lvl["buy"]
                sp = lvl["sell"]
                size = round(self.order_amount_usd / bp, 2)
                logger.info(
                    f"Placing BUY{i+1} {side_name} — {size:.2f} shares @ {bp} "
                    f"(sell target: {sp}, expires: {datetime.fromtimestamp(buy_expiry).strftime('%H:%M:%S')})"
                )
                order = await self.place_order(
                    token_id=token_id, price=bp, size=size, side="BUY",
                    order_type="GTD", expiration=buy_expiry,
                )
                if order:
                    self._order_buy_price[order.order_id] = bp
                    self._order_sell_price[order.order_id] = sp
            
            # Run all price levels for this side concurrently
            tasks = [place_single(i, lvl) for i, lvl in enumerate(self.price_levels)]
            if tasks:
                await asyncio.gather(*tasks)

        await asyncio.gather(
            place("UP", self.token_ids["up"]),
            place("DOWN", self.token_ids["down"]),
        )

    # ------------------------------------------------------------------
    # Order fill handling
    # ------------------------------------------------------------------

    async def _handle_buy_fill(self, order: OrderInfo) -> None:
        now_str = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
        current_price = self._rtds_feed.get_price()
        p_str = f"${current_price:,.4f}" if current_price is not None else "n/a"

        # Look up which sell price was planned for this specific order
        fallback_sell = self.price_levels[0]["sell"] if self.price_levels else 0.70
        sell_price = self._order_sell_price.get(order.order_id, fallback_sell)

        asyncio.create_task(self._notify(
            f"🟡 <b>BUY FILLED at {now_str}</b>\n"
            f"Token: {order.token_id[:20]}...\n"
            f"Buy price: <b>{order.price}</b>\n"
            f"Sell target: <b>{sell_price}</b>\n"
            f"Size: {order.size:.2f} shares\n"
            f"Current {self.ticker}: <b>{p_str}</b>"
        ))

        if SELL_DELAY_SECONDS > 0:
            await asyncio.sleep(SELL_DELAY_SECONDS)

        sell_size = math.floor(order.size_matched * 100) / 100.0

        logger.info(f"Placing SELL {sell_size:.2f} shares @ {sell_price}")

        sell_order = None
        floored_size = math.floor((sell_size - 0.05))

        for attempt in range(SELL_ORDER_RETRY_ATTEMPTS):
            sell_order = await self.place_order(
                token_id=order.token_id,
                price=sell_price,
                size=sell_size,
                side="SELL",
                send_error_to_telegram=False,
            )
            if sell_order:
                break

            logger.info(
                f"Sell attempt {attempt+1}/{SELL_ORDER_RETRY_ATTEMPTS} failed "
                f"(size={sell_size:.2f}) — retrying with floored size={floored_size:.2f}"
            )

            sell_order = await self.place_order(
                token_id=order.token_id,
                price=sell_price,
                size=floored_size,
                side="SELL",
                send_error_to_telegram=(attempt == SELL_ORDER_RETRY_ATTEMPTS - 1),
            )
            if sell_order:
                break

            await asyncio.sleep(1)

        if sell_order:
            self._order_buy_price[sell_order.order_id] = order.price
            self._order_sell_price[sell_order.order_id] = sell_price
            logger.info(f"Sell order placed: {sell_order.order_id}")
        else:
            logger.warning(
                f"Failed to place sell order for token {order.token_id} "
                f"after {SELL_ORDER_RETRY_ATTEMPTS} attempts"
            )
            asyncio.create_task(self._notify(
                f"⚠️ <b>SELL FAILED:</b> Could not place auto-sell for "
                f"{order.token_id[:16]}... at {sell_price} "
                f"after {SELL_ORDER_RETRY_ATTEMPTS} retries."
            ))

    async def on_order_update(self, order: OrderInfo) -> None:
        if order.status not in ('filled', 'MATCHED'):
            return

        if order.side == 'BUY':
            buy_price = self._order_buy_price.get(order.order_id, order.price)
            logger.info(
                f"BUY filled! order_id={order.order_id} "
                f"buy_price={buy_price} token={order.token_id[:16]}..."
            )
            asyncio.create_task(self._handle_buy_fill(order))

        elif order.side == 'SELL':
            buy_price = self._order_buy_price.get(order.order_id, order.price / 2)
            sell_price = order.price
            profit_est = (sell_price - buy_price) * order.size

            logger.info(
                f"SELL filled! order_id={order.order_id} "
                f"sell_price={sell_price} profit≈${profit_est:.4f}"
            )

            balance = await self.bot.get_balance()
            asyncio.create_task(self._notify(
                f"🟢 <b>SELL FILLED — Profit secured!</b>\n"
                f"Sell price: <b>{sell_price}</b>\n"
                f"Size: {order.size:.2f} shares\n"
                f"Est. profit: <b>${profit_est:.4f}</b>\n"
                f"Balance: <b>${balance:.2f} USDC</b>"
            ))
            self.close_position(order.token_id, 'BUY')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_strategy(ticker: str):
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    global logger
    ticker = ticker.upper()
    logger = logging.getLogger(f"{ticker}_CryptoTarget_Strategy")

    load_dotenv()
    private_key = os.environ.get("POLY_PRIVATE_KEY")
    if not private_key:
        logger.error("POLY_PRIVATE_KEY not set")
        sys.exit(1)

    bot = TradingBot(config=Config.from_env(), private_key=private_key)
    strategy = CryptoTargetLowballStrategy(bot, ticker=ticker, params={"check_interval": 1})

    logger.info(f"Starting {ticker} Crypto Target Lowball Strategy...")
    levels_str = "  ".join(
        f"buy={l['buy']:.2f}→sell={l['sell']:.2f}" for l in strategy.price_levels
    )
    logger.info(f"Price levels ({len(strategy.price_levels)}): {levels_str} | Threshold: ${strategy.threshold}")
    logger.info(
        f"Trigger window: last 60s → {STOP_ORDERS_SECONDS_BEFORE_CLOSE}s before cycle end | "
        f"Price feed: Chainlink via Polymarket RTDS ({strategy._chainlink_symbol.upper()}) "
        f"+ Polymarket CLOB WS for token events"
    )

    try:
        asyncio.run(strategy.run())
    except KeyboardInterrupt:
        logger.info("\nReceived exit signal. Stopping...")
        strategy.status = StrategyStatus.STOPPED
        os._exit(0)
    except Exception as e:
        logger.error(f"Strategy error: {e}")
        raise


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_strategy(sys.argv[1].upper())
    else:
        run_strategy("BTC")
