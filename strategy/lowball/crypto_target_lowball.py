#!/usr/bin/env python3
"""
Crypto Target Lowball Strategy

Runs continuously on 5-min BTC/ETH/SOL/XRP markets.

Logic:
  - Connects to the Polymarket Market WebSocket to receive live
    best_bid_ask / last_trade_price events for the current tokens.
  - Polls CoinGecko for the current real-world price of the asset.
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
SELL_DELAY_SECONDS = 5
SELL_ORDER_RETRY_ATTEMPTS = 7

# Default thresholds: how close (in USD) the current price must be to the
# target price to trigger an order.
DEFAULT_THRESHOLDS: Dict[str, float] = {
    "BTC": 20.0,
    "ETH": 1.0,
    "SOL": 0.5,
    "XRP": 0.05,
}

# Polymarket Market WebSocket endpoint
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# CoinGecko coin IDs
COINGECKO_IDS: Dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "XRP": "ripple",
}

# How often to refresh the current price from CoinGecko (seconds)
PRICE_POLL_INTERVAL = 5

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
# Polymarket Market WebSocket feed
# ---------------------------------------------------------------------------

class PolymarketMarketFeed:
    """
    Subscribes to the Polymarket Market WebSocket for a set of token IDs.

    Tracks:
      - best_bid / best_ask per token (from best_bid_ask events)
      - last_trade_price per token
    """

    def __init__(self):
        self._ws = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        # token_id → {"best_bid": float, "best_ask": float, "last_trade": float}
        self._token_data: Dict[str, Dict[str, float]] = {}
        self._subscribed_tokens: List[str] = []

    def get_best_bid(self, token_id: str) -> Optional[float]:
        return self._token_data.get(token_id, {}).get("best_bid")

    def get_best_ask(self, token_id: str) -> Optional[float]:
        return self._token_data.get(token_id, {}).get("best_ask")

    def get_last_trade(self, token_id: str) -> Optional[float]:
        return self._token_data.get(token_id, {}).get("last_trade")

    async def subscribe(self, token_ids: List[str]) -> bool:
        """Connect to WS and subscribe to the given token IDs."""
        self._subscribed_tokens = list(token_ids)
        # Initialize data slots
        for tid in token_ids:
            self._token_data.setdefault(tid, {})

        await self.disconnect()  # close any previous connection
        return await self._connect()

    async def _connect(self) -> bool:
        try:
            import websockets
            self._ws = await asyncio.wait_for(
                websockets.connect(POLY_WS_URL), timeout=10.0
            )
            self._running = True
            # Send subscription message
            sub_msg = json.dumps({
                "assets_ids": self._subscribed_tokens,
                "type": "market",
                "custom_feature_enabled": True,   # enables best_bid_ask events
            })
            await self._ws.send(sub_msg)
            self._task = asyncio.create_task(self._read_loop())
            logger.info(
                f"Polymarket WS connected & subscribed to "
                f"{len(self._subscribed_tokens)} token(s)"
            )
            return True
        except Exception as e:
            logger.warning(f"Polymarket WS connect failed: {e}")
            self._running = False
            return False

    async def _read_loop(self) -> None:
        try:
            async for raw_msg in self._ws:
                if not self._running:
                    break
                try:
                    msgs = json.loads(raw_msg)
                    # Polymarket sends a list of events
                    if isinstance(msgs, dict):
                        msgs = [msgs]
                    for msg in msgs:
                        self._handle_message(msg)
                except Exception as e:
                    logger.debug(f"WS parse error: {e}")
        except Exception as e:
            logger.debug(f"WS read loop ended: {e}")
        finally:
            self._running = False

    def _handle_message(self, msg: Dict[str, Any]) -> None:
        event_type = msg.get("event_type", "")

        if event_type == "best_bid_ask":
            token_id = msg.get("asset_id", "")
            if token_id not in self._token_data:
                return
            bid = msg.get("best_bid")
            ask = msg.get("best_ask")
            if bid is not None:
                self._token_data[token_id]["best_bid"] = float(bid)
            if ask is not None:
                self._token_data[token_id]["best_ask"] = float(ask)
            logger.debug(
                f"WS best_bid_ask token={token_id[:16]}... "
                f"bid={bid} ask={ask}"
            )

        elif event_type == "last_trade_price":
            token_id = msg.get("asset_id", "")
            if token_id not in self._token_data:
                return
            price = msg.get("price")
            if price is not None:
                self._token_data[token_id]["last_trade"] = float(price)
            logger.debug(
                f"WS last_trade_price token={token_id[:16]}... price={price}"
            )

        elif event_type == "price_change":
            # price_change contains a list of per-asset changes
            for change in msg.get("price_changes", []):
                token_id = change.get("asset_id", "")
                if token_id not in self._token_data:
                    continue
                bid = change.get("best_bid")
                ask = change.get("best_ask")
                if bid is not None:
                    self._token_data[token_id]["best_bid"] = float(bid)
                if ask is not None:
                    self._token_data[token_id]["best_ask"] = float(ask)

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
# CoinGecko price poller
# ---------------------------------------------------------------------------

class CoinGeckoFeed:
    """
    Polls CoinGecko's free public API every PRICE_POLL_INTERVAL seconds
    for the current USD price of one or more coins.

    No API key required.
    """

    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, coin_ids: List[str]):
        """
        Args:
            coin_ids: List of CoinGecko IDs (e.g. ["bitcoin", "ethereum"])
        """
        self._coin_ids = coin_ids
        self._prices: Dict[str, float] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def get_price(self, coin_id: str) -> Optional[float]:
        return self._prices.get(coin_id)

    async def start(self) -> None:
        self._running = True
        await self._fetch()   # initial fetch before returning
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _poll_loop(self) -> None:
        while self._running:
            await asyncio.sleep(PRICE_POLL_INTERVAL)
            await self._fetch()

    async def _fetch(self) -> None:
        import urllib.request
        ids_str = ",".join(self._coin_ids)
        url = f"{self.BASE_URL}/simple/price?ids={ids_str}&vs_currencies=usd"
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "polymarket-bot/1.0"}
            )
            raw = await asyncio.to_thread(urllib.request.urlopen, req, timeout=10)
            data = json.loads(raw.read().decode())
            for coin_id in self._coin_ids:
                price = data.get(coin_id, {}).get("usd")
                if price is not None:
                    self._prices[coin_id] = float(price)
                    logger.debug(f"CoinGecko {coin_id} = ${price:.4f}")
        except Exception as e:
            logger.warning(f"CoinGecko fetch failed: {e}")


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
      - CoinGecko REST (current BTC/ETH/SOL/XRP spot price)
      - Polymarket Market WebSocket (token orderbook / trade events)
    Target price:
      - Parsed from the market question string
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
        self.buy_price: float = self._load_config_value("buy_price", 0.10)
        self.sell_price: float = self._load_config_value("sell_price", 0.70)
        self.threshold: float = self._load_config_value(
            "threshold", DEFAULT_THRESHOLDS.get(self.ticker, 1.0)
        )

        # Dry-run mode
        self.dry_run: bool = os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes")
        if self.dry_run:
            logger.warning("*** DRY RUN MODE — orders will NOT be submitted ***")

        # Price feeds
        coin_id = COINGECKO_IDS.get(self.ticker, self.ticker.lower())
        self._coingecko = CoinGeckoFeed(coin_ids=[coin_id])
        self._coin_id = coin_id

        self._poly_ws = PolymarketMarketFeed()

        # Track which cycles we've already entered
        self.entered_cycles: set = set()

        # Per-order metadata
        self._order_buy_price: Dict[str, float] = {}

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

    def _save_config(self) -> None:
        try:
            data = {}
            if self.config_file.exists():
                try:
                    data = json.loads(self.config_file.read_text(encoding="utf-8"))
                except Exception:
                    pass
            data["order_amount_usd"] = self.order_amount_usd
            data["buy_price"] = self.buy_price
            data["sell_price"] = self.sell_price
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
            "/price — current asset price from CoinGecko\n"
            "/target — current cycle target price (from market question)\n"
            "/size — current order size in USD\n"
            "/setsize &lt;amount&gt; — e.g. /setsize 2\n"
            "/buy — current buy price\n"
            "/setbuy &lt;price&gt; — e.g. /setbuy 0.10\n"
            "/sell — current sell price\n"
            "/setsell &lt;price&gt; — e.g. /setsell 0.70\n"
            "/threshold — current price threshold\n"
            f"/setthreshold &lt;value&gt; — e.g. /setthreshold 20   (default: {DEFAULT_THRESHOLDS.get(self.ticker, 1.0)})\n"
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
                        p = self._coingecko.get_price(self._coin_id)
                        if p:
                            reply = (
                                f"<b>[{self.name}]</b> 📊 {self.ticker} price: "
                                f"<b>${p:,.4f}</b> (CoinGecko)"
                            )
                        else:
                            reply = f"<b>[{self.name}]</b> ⚠️ No price data yet."

                    # ---- target ----
                    elif command == "/target":
                        if self._target_price > 0:
                            p = self._coingecko.get_price(self._coin_id)
                            diff = abs(p - self._target_price) if p else None
                            diff_str = f" | Δ = <b>${diff:,.4f}</b> (threshold ${self.threshold})" if diff is not None else ""
                            reply = (
                                f"<b>[{self.name}]</b> 🎯 Target price: "
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

                    # ---- buy price ----
                    elif command == "/buy":
                        reply = f"<b>[{self.name}]</b> 📉 Buy price: <b>{self.buy_price:.2f}</b>{dry_tag}"

                    elif command == "/setbuy":
                        if not args:
                            reply = "❌ Usage: /setbuy &lt;price&gt;"
                        else:
                            try:
                                new_p = float(args[0])
                                if not (0 < new_p < 1):
                                    raise ValueError("must be between 0 and 1")
                                old = self.buy_price
                                self.buy_price = new_p
                                self._save_config()
                                reply = (
                                    f"<b>[{self.name}]</b> ✅ Buy price: "
                                    f"<b>{old:.2f}</b> → <b>{new_p:.2f}</b>{dry_tag}"
                                )
                            except ValueError as e:
                                reply = f"❌ {e}"

                    # ---- sell price ----
                    elif command == "/sell":
                        reply = f"<b>[{self.name}]</b> 📈 Sell price: <b>{self.sell_price:.2f}</b>{dry_tag}"

                    elif command == "/setsell":
                        if not args:
                            reply = "❌ Usage: /setsell &lt;price&gt;"
                        else:
                            try:
                                new_p = float(args[0])
                                if not (0 < new_p < 1):
                                    raise ValueError("must be between 0 and 1")
                                old = self.sell_price
                                self.sell_price = new_p
                                self._save_config()
                                reply = (
                                    f"<b>[{self.name}]</b> ✅ Sell price: "
                                    f"<b>{old:.2f}</b> → <b>{new_p:.2f}</b>{dry_tag}"
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
        await self._coingecko.start()
        logger.info(f"CoinGecko price feed started for {self._coin_id}")
        dry = " [DRY RUN]" if self.dry_run else ""
        await self._report_balance(f"{self.name} started{dry}")

    async def cleanup(self) -> None:
        await self._coingecko.stop()
        await self._poly_ws.disconnect()
        dry = " [DRY RUN]" if self.dry_run else ""
        await self._report_balance(f"{self.name} stopped{dry}")
        await super().cleanup()

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    async def run(self, token_ids: List[str] = None, duration: int = None):
        await self.initialize()

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
            # Disconnect WS — will reconnect on next cycle
            await self._poly_ws.disconnect()

        # ── 2. Try to enter the current active market ─────────────────
        await self._try_enter_market()

        # ── 3. Trigger logic: last-minute price check ─────────────────
        if (
            self.cycle_active
            and not self._orders_placed_this_cycle
            and self._target_price > 0
        ):
            in_window = (
                now >= self.cycle_end_time - 60
                and now < self.cycle_end_time - 20
            )
            if in_window:
                current_price = self._coingecko.get_price(self._coin_id)
                if current_price is not None:
                    diff = abs(current_price - self._target_price)
                    logger.info(
                        f"In window — {self.ticker}: current=${current_price:,.4f}, "
                        f"target=${self._target_price:,.4f}, "
                        f"diff=${diff:.4f}, threshold=${self.threshold}"
                    )
                    if diff <= self.threshold:
                        logger.info(
                            f"✅ Threshold hit! diff={diff:.4f} <= {self.threshold} "
                            f"— placing BUY orders"
                        )
                        self._orders_placed_this_cycle = True
                        asyncio.create_task(self._place_close_orders(current_price))
                else:
                    logger.warning("CoinGecko price not yet available — waiting...")

    # ------------------------------------------------------------------
    # Market entry
    # ------------------------------------------------------------------

    async def _try_enter_market(self) -> None:
        """
        Find & track the current active 5-min market.
        Subscribes the Polymarket WS to its token IDs.
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

        self.entered_cycles.add(cycle_end)
        self.cycle_end_time = cycle_end
        self.cycle_active = True
        self.current_market = market
        self.token_ids = self.gamma.parse_token_ids(market)
        self._orders_placed_this_cycle = False

        # Parse target price from question
        question = market.get("question", "")
        self._target_price = parse_target_price_from_question(question) or 0.0
        if self._target_price > 0:
            logger.info(
                f"New market: {market.get('slug')} | "
                f"Target: ${self._target_price:,.2f} | "
                f"Ends: {datetime.fromtimestamp(cycle_end)}"
            )
        else:
            logger.warning(
                f"New market: {market.get('slug')} — could not parse target "
                f"price from question: '{question}'"
            )

        # Subscribe Polymarket WS to this market's tokens
        token_list = [v for v in self.token_ids.values() if v]
        connected = await self._poly_ws.subscribe(token_list)
        if connected:
            logger.info(f"Polymarket WS subscribed to {len(token_list)} token(s) for {self.ticker}")
        else:
            logger.warning("Polymarket WS subscription failed — will rely on CoinGecko only")

        current_price = self._coingecko.get_price(self._coin_id)
        p_str = f"${current_price:,.4f}" if current_price else "n/a"
        await self._notify(
            f"📋 <b>New cycle started</b>\n"
            f"Market: <b>{market.get('slug')}</b>\n"
            f"Target: <b>${self._target_price:,.2f}</b>\n"
            f"Current {self.ticker}: <b>{p_str}</b>\n"
            f"Threshold: <b>${self.threshold}</b>\n"
            f"Ends: {datetime.fromtimestamp(cycle_end).strftime('%H:%M:%S UTC')}"
        )

    # ------------------------------------------------------------------
    # Order placement on trigger
    # ------------------------------------------------------------------

    async def _place_close_orders(self, current_price: float) -> None:
        size = round(self.order_amount_usd / self.buy_price, 2)

        await self._notify(
            f"🎯 <b>TARGET MATCHED — PLACING ORDERS</b>\n"
            f"Asset: <b>{self.ticker}</b>\n"
            f"Current Price: <b>${current_price:,.4f}</b>\n"
            f"Target Price: <b>${self._target_price:,.2f}</b>\n"
            f"Diff: <b>${abs(current_price - self._target_price):.4f}</b> "
            f"(threshold ${self.threshold})\n"
            f"Order: {size:.2f} shares @ {self.buy_price} (both UP & DOWN)"
        )

        async def place(side_name: str, token_id: str):
            logger.info(
                f"Placing BUY {side_name} — {size:.2f} shares @ {self.buy_price}"
            )
            order = await self.place_order(
                token_id=token_id,
                price=self.buy_price,
                size=size,
                side="BUY",
            )
            if order:
                self._order_buy_price[order.order_id] = self.buy_price

        await asyncio.gather(
            place("UP", self.token_ids["up"]),
            place("DOWN", self.token_ids["down"]),
        )

    # ------------------------------------------------------------------
    # Order fill handling
    # ------------------------------------------------------------------

    async def _handle_buy_fill(self, order: OrderInfo) -> None:
        now_str = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
        current_price = self._coingecko.get_price(self._coin_id)
        p_str = f"${current_price:,.4f}" if current_price else "n/a"

        await self._notify(
            f"🟡 <b>BUY FILLED at {now_str}</b>\n"
            f"Token: {order.token_id[:20]}...\n"
            f"Buy price: <b>{order.price}</b>\n"
            f"Sell target: <b>{self.sell_price}</b>\n"
            f"Size: {order.size:.2f} shares\n"
            f"Current {self.ticker}: <b>{p_str}</b>"
        )

        await asyncio.sleep(SELL_DELAY_SECONDS)

        sell_size = math.floor(order.size_matched * 100) / 100.0

        logger.info(f"Placing SELL {sell_size:.2f} shares @ {self.sell_price}")

        sell_order = None
        floored_size = math.floor((sell_size - 0.05))

        for attempt in range(SELL_ORDER_RETRY_ATTEMPTS):
            sell_order = await self.place_order(
                token_id=order.token_id,
                price=self.sell_price,
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
                price=self.sell_price,
                size=floored_size,
                side="SELL",
                send_error_to_telegram=(attempt == SELL_ORDER_RETRY_ATTEMPTS - 1),
            )
            if sell_order:
                break

            await asyncio.sleep(1)

        if sell_order:
            self._order_buy_price[sell_order.order_id] = order.price
            logger.info(f"Sell order placed: {sell_order.order_id}")
        else:
            logger.warning(
                f"Failed to place sell order for token {order.token_id} "
                f"after {SELL_ORDER_RETRY_ATTEMPTS} attempts"
            )
            await self._notify(
                f"⚠️ <b>SELL FAILED:</b> Could not place auto-sell for "
                f"{order.token_id[:16]}... at {self.sell_price} "
                f"after {SELL_ORDER_RETRY_ATTEMPTS} retries."
            )

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
            await self._notify(
                f"🟢 <b>SELL FILLED — Profit secured!</b>\n"
                f"Sell price: <b>{sell_price}</b>\n"
                f"Size: {order.size:.2f} shares\n"
                f"Est. profit: <b>${profit_est:.4f}</b>\n"
                f"Balance: <b>${balance:.2f} USDC</b>"
            )
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
    logger.info(f"Buy @ {strategy.buy_price} | Sell @ {strategy.sell_price} | "
                f"Threshold: ${strategy.threshold}")
    logger.info(
        f"Trigger window: last 60s → 20s before cycle end\n"
        f"Price feed: CoinGecko ({strategy._coin_id}) "
        f"+ Polymarket WS for market events"
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
