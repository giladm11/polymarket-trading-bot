#!/usr/bin/env python3
"""
Close Price Lowball Strategy

Runs continuously, cycling through 5-min markets.
Instead of placing orders all the time, this checks the price of the asset
against the target price (price at market start).
If in the last minute of the market, the difference is within the threshold:
  - Place UP and DOWN orders at 0.10
  - If filled, place a SELL order at 0.70


  python strategy/lowball/close_price_lowball.py BTC
"""

import sys
import asyncio
import logging
import time
import os
import math
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

# Add project root directory to path (three levels up)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from dotenv import load_dotenv

from src.bot import TradingBot, OrderResult, OrderSide
from src.gamma_client import GammaClient
from src.config import Config
from examples.strategy_example import BaseStrategy, Position, OrderInfo, StrategyStatus
from lib.binance_feed import BinancePriceFeed, fetch_klines

# ---------------------------------------------------------------------------
# Strategy constants
# ---------------------------------------------------------------------------

MARKET_DURATION = 5            # minutes per cycle
SELL_DELAY_SECONDS = 5        # wait after fill before placing sell

THRESHOLDS = {
    "BTC": 20.0,
    "ETH": 1.0,
    "SOL": 0.1,
    "XRP": 0.01,
}

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ClosePriceLowball")

class TelegramNotifier:
    """Sends messages to a Telegram group via Bot API, with command polling."""
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = str(chat_id)
        self._base_url = f"https://api.telegram.org/bot{token}"
        self._last_update_id = 0

    async def send(self, text: str, chat_id: str = None) -> bool:
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
                url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
            )
            await asyncio.to_thread(urllib.request.urlopen, req, timeout=10)
            return True
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")
            return False

    async def get_updates(self) -> list:
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

class ClosePriceLowballStrategy(BaseStrategy):
    def __init__(self, bot: TradingBot, ticker: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(bot, params or {}, name=f"{ticker}ClosePriceLowball")
        self.ticker = ticker.upper()

        self.gamma = GammaClient(duration_minutes=MARKET_DURATION)
        self.current_market: Optional[Dict[str, Any]] = None
        self.token_ids: Dict[str, str] = {}
        
        self.cycle_start_time: float = 0.0
        self.cycle_end_time: float = 0.0
        self.cycle_active: bool = False

        self._target_price: float = 0.0
        self._orders_placed_this_cycle: bool = False

        self._binance = BinancePriceFeed(symbol=f"{self.ticker.lower()}usdt")

        self.config_file = Path(__file__).parent / f"bot_config_close_lowball_{self.ticker.lower()}.json"

        # Telegram
        tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "")
        self.telegram: Optional[TelegramNotifier] = (
            TelegramNotifier(tg_token, tg_chat) if tg_token and tg_chat else None
        )

        self.order_amount_usd = self._load_config_value("order_amount_usd", float(os.environ.get("ORDER_AMOUNT_USD", "2")))
        self.buy_price = self._load_config_value("buy_price", 0.10)
        self.sell_price = self._load_config_value("sell_price", 0.70)
        
        self.entered_cycles: set = set()

    def _load_config_value(self, key: str, default):
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
        import json
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
            self.config_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
            logger.info(f"Config saved to {self.config_file.name}")
        except Exception as e:
            logger.warning(f"Could not save {self.config_file.name}: {e}")

    async def initialize(self) -> None:
        await super().initialize()
        await self._binance.connect()
        logger.info(f"Connected to Binance feed for {self.ticker}USDT")

        @self._binance.on_price
        def on_price_update(price: float):
            if not getattr(self, 'cycle_active', False) or getattr(self, '_orders_placed_this_cycle', False):
                return
            
            target = getattr(self, '_target_price', 0.0)
            if target <= 0:
                return

            now = time.time()
            in_window = (now >= self.cycle_end_time - 60) and (now < self.cycle_end_time - 20)
            diff = abs(price - target) 
            threshold = THRESHOLDS.get(self.ticker, 0.0)
            
            if in_window and diff <= threshold:
                logger.info(f"Target matched in real-time! Prc={price:.2f}, Tgt={target:.2f}, Diff={diff:.4f} <= {threshold:.2f}. Proceeding to place UP and DOWN orders.")
                self._orders_placed_this_cycle = True
                asyncio.create_task(self._place_close_orders())
    async def cleanup(self) -> None:
        await self._binance.disconnect()
        await super().cleanup()

    async def place_order(
        self, token_id: str, price: float, size: float, side: str,
        order_type: str = "GTC", expiration: int = 0
    ) -> Optional[OrderInfo]:
        result = await self.bot.place_order(
            token_id=token_id, price=price, size=size, side=side,
            order_type=order_type, expiration=expiration,
        )
        if not result or not result.success or not result.order_id:
            error_msg = result.message if result else 'no result'
            logger.warning(f"Order placement failed: {error_msg}")
            await self._notify(f"\u274c <b>ORDER FAILED:</b> {error_msg}\nToken: {token_id[:16]}...\nSide: {side} | Size: {size} | Price: {price}")
            return None

        order_info = OrderInfo(
            order_id=result.order_id, token_id=token_id, side=side,
            price=price, size=size, status="pending",
        )
        self.orders[order_info.order_id] = order_info
        return order_info

    async def _notify(self, text: str) -> None:
        if self.telegram:
            await self.telegram.send(f"<b>[{self.name}]</b> {text}")

    async def _report_balance(self, context: str = "") -> None:
        """Fetch balance and send it to Telegram."""
        balance = await self.bot.get_balance()
        prefix = f"{context}: " if context else ""
        msg = f"💰 {prefix}Current balance: <b>${balance:.2f} USDC</b>"
        await self._notify(msg)

    async def _handle_telegram_commands(self) -> None:
        if not self.telegram:
            return

        HELP = (
            f"🤖 <b>[{self.name}] Available commands:</b>\n"
            "/balance — current USDC balance\n"
            "/size — current order size per price level\n"
            f"/setsize &lt;amount&gt; — set order size usdc per order\n"
            "/buyprice — current buy price\n"
            "/setbuy &lt;amount&gt; — set buy price\n"
            "/sellprice — current sell price\n"
            "/setsell &lt;amount&gt; — set sell price"
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
                        reply = f"<b>[{self.name}]</b> 💰 Current balance: <b>${balance:.2f} USDC</b>"

                    elif command == "/size":
                        reply = f"<b>[{self.name}]</b> 📏 Current order size: <b>${self.order_amount_usd:.2f} USD</b>"

                    elif command == "/setsize":
                        if not args:
                            reply = "❌ Usage: /setsize &lt;amount&gt;  (e.g. /setsize 2)"
                        else:
                            try:
                                new_size = float(args[0])
                                if new_size <= 0:
                                    raise ValueError("must be positive")
                                old_size = self.order_amount_usd
                                self.order_amount_usd = new_size
                                self._save_config()
                                reply = f"<b>[{self.name}]</b> ✅ Order size updated: <b>${old_size:.2f}</b> ➡️ <b>${new_size:.2f} USD</b>"
                            except ValueError as e:
                                reply = f"❌ Invalid amount: {e}"

                    elif command == "/buyprice":
                        reply = f"<b>[{self.name}]</b> 📉 Current buy price: <b>{self.buy_price:.2f}</b>"

                    elif command == "/setbuy":
                        if not args:
                            reply = "❌ Usage: /setbuy &lt;amount&gt;  (e.g. /setbuy 0.10)"
                        else:
                            try:
                                new_prc = float(args[0])
                                if new_prc <= 0:
                                    raise ValueError("must be positive")
                                old_prc = self.buy_price
                                self.buy_price = new_prc
                                self._save_config()
                                reply = f"<b>[{self.name}]</b> ✅ Buy price updated: <b>{old_prc:.2f}</b> ➡️ <b>{new_prc:.2f}</b>"
                            except ValueError as e:
                                reply = f"❌ Invalid amount: {e}"
                                
                    elif command == "/sellprice":
                        reply = f"<b>[{self.name}]</b> 📈 Current sell price: <b>{self.sell_price:.2f}</b>"

                    elif command == "/setsell":
                        if not args:
                            reply = "❌ Usage: /setsell &lt;amount&gt;  (e.g. /setsell 0.70)"
                        else:
                            try:
                                new_prc = float(args[0])
                                if new_prc <= 0:
                                    raise ValueError("must be positive")
                                old_prc = self.sell_price
                                self.sell_price = new_prc
                                self._save_config()
                                reply = f"<b>[{self.name}]</b> ✅ Sell price updated: <b>{old_prc:.2f}</b> ➡️ <b>{new_prc:.2f}</b>"
                            except ValueError as e:
                                reply = f"❌ Invalid amount: {e}"

                    elif command in ("/help", "/start"):
                        reply = HELP
                    else:
                        reply = HELP

                    await self.telegram.send(reply, chat_id=chat_id)

            except Exception as e:
                logger.debug(f"Command handler error: {e}")

            await asyncio.sleep(2)

    async def run(self, token_ids: List[str] = None, duration: int = None):
        await self.initialize()
        await self._report_balance("Lowball bot started")
        
        async def _strategy_loop():
            start_time = time.time()
            try:
                while self.status == StrategyStatus.RUNNING:
                    if duration and (time.time() - start_time) > duration:
                        break

                    await self.sync_orders()
                    await self.on_tick({})
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

    async def _get_target_price(self) -> float:
        """Fetch the exact open price at the market start time using Binance REST API."""
        if self._target_price > 0:
            return self._target_price
            
        start_ms = int(self.cycle_start_time * 1000)
        symbol = f"{self.ticker.upper()}USDT"
        
        def do_fetch():
            return fetch_klines(symbol, "1m", start_ms, start_ms + 60000)
            
        try:
            klines = await asyncio.to_thread(do_fetch)
            if klines and len(klines) > 0:
                self._target_price = klines[0][1] # Open price of that minute
                logger.info(f"Target price for market {self.current_market.get('slug')} resolved to {self._target_price}")
            else:
                logger.warning(f"No kline data found for target price resolution at {datetime.fromtimestamp(self.cycle_start_time)}.")
        except Exception as e:
            logger.error(f"Error fetching target price: {e}")
            
        return self._target_price

    async def on_tick(self, _: Dict[str, Any]) -> None:
        now = time.time()

        if self.cycle_active and now >= self.cycle_end_time:
            logger.info("Cycle ended. Resetting state.")
            self.cycle_active = False
            self.current_market = None
            self.token_ids = {}
            self._target_price = 0.0
            self._orders_placed_this_cycle = False
            for order_id in list(self.orders.keys()):
                if self.orders[order_id].status not in ('filled', 'MATCHED', 'cancelled'):
                    del self.orders[order_id]

        await self._try_enter_next_market()

        # Decision logic is handled in on_price_update() to execute instantly on tick
        pass

    async def _try_enter_next_market(self) -> None:
        market = self.gamma.get_current_market(self.ticker)
        if not market or not market.get("acceptingOrders", False):
            return

        end_date_iso = market.get("endDate", "")
        start_date_iso = market.get("eventStartTime", "")
        try:
            if end_date_iso.endswith("Z"):
                end_date_iso = end_date_iso[:-1] + "+00:00"
            cycle_end = datetime.fromisoformat(end_date_iso).timestamp()
            
            if start_date_iso.endswith("Z"):
                start_date_iso = start_date_iso[:-1] + "+00:00"
            cycle_start = datetime.fromisoformat(start_date_iso).timestamp()
        except Exception as e:
            logger.error(f"Failed parsing dates {end_date_iso}, {start_date_iso}: {e}")
            return

        if cycle_end in self.entered_cycles:
            return

        self.entered_cycles.add(cycle_end)
        self.cycle_start_time = cycle_start
        self.cycle_end_time = cycle_end
        self.cycle_active = True
        self.current_market = market
        self.token_ids = self.gamma.parse_token_ids(market)
        
        logger.info(f"Tracking new market: {market.get('slug')} (Ends {datetime.fromtimestamp(cycle_end)})")
        
        # Start fetching target price in background so the tracker updates immediately
        asyncio.create_task(self._fetch_target_price_now())

    async def _fetch_target_price_now(self):
        """Pre-fetch target price so live logs show it."""
        for _ in range(60): # try for a minute
            if not self.cycle_active:
                break
            target = await self._get_target_price()
            if target > 0:
                break
            try:
                await asyncio.sleep(2)
            except asyncio.CancelledError:
                break
            
    async def _place_close_orders(self) -> None:
        size = round(self.order_amount_usd / self.buy_price, 2)
        current_price = self._binance.get_price()
        target_price = getattr(self, '_target_price', 0.0)

        await self._notify(
            f"🎯 <b>TARGET MATCHED - PLACING ORDERS</b>\n"
            f"Target Asset Price: <b>{target_price:.2f}</b>\n"
            f"Current Asset Price: <b>{current_price:.2f}</b>\n"
            f"Size: {size:.2f} shares per side @ {self.buy_price}"
        )

        async def place(side_name, token_id):
            logger.info(f"Placing BUY {side_name} {size} shares @ {self.buy_price}")
            return await self.place_order(token_id, self.buy_price, size, "BUY")

        await asyncio.gather(
            place("UP", self.token_ids["up"]),
            place("DOWN", self.token_ids["down"])
        )

    async def _handle_buy_fill(self, order: OrderInfo) -> None:
        await self._notify(
            f"\U0001f7e1 <b>BUY FILLED</b>\nToken: {order.token_id[:20]}...\nBuy price: <b>{order.price}</b>\nSell target: <b>{self.sell_price}</b>\nSize: {order.size:.2f}"
        )
        await asyncio.sleep(SELL_DELAY_SECONDS)
        
        sell_size = math.floor(order.size_matched * 100) / 100.0

        logger.info(f"Placing SELL {sell_size:.2f} shares @ {self.sell_price}")
        
        sell_order = await self.place_order(
            token_id=order.token_id, price=self.sell_price, size=sell_size, side="SELL",
        )
        if not sell_order:
            sell_size = math.floor((sell_size - 0.05))
            sell_order = await self.place_order(
                token_id=order.token_id, price=self.sell_price, size=sell_size, side="SELL",
            )
            
        if sell_order:
            logger.info(f"Sell order placed: {sell_order.order_id}")
        else:
            await self._notify(f"\u26a0\ufe0f <b>SELL FAILED:</b> Could not place auto-sell for {order.token_id[:16]}")
            
    async def on_order_update(self, order: OrderInfo) -> None:
        if order.status not in ('filled', 'MATCHED'):
            return

        if order.side == 'BUY':
            asyncio.create_task(self._handle_buy_fill(order))

        elif order.side == 'SELL':
            logger.info(f"SELL filled! order_id={order.order_id} sell_price={order.price}")
            await self._notify(f"\U0001f7e2 <b>SELL FILLED — Profit secured!</b>\nSell price: <b>{order.price}</b>\nSize: {order.size:.2f} shares")

def run_strategy(ticker: str):
    # Ensure Ctrl+C works instantly on Windows
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    
    global logger
    logger = logging.getLogger(f"{ticker}_ClosePrice_Strategy")
    load_dotenv()
    private_key = os.environ.get("POLY_PRIVATE_KEY")
    bot = TradingBot(config=Config.from_env(), private_key=private_key)
    strategy = ClosePriceLowballStrategy(bot, ticker=ticker, params={"check_interval": 1})

    logger.info(f"Starting {ticker} Close Price Lowball Strategy...")
    logger.info(f"Buy @ {strategy.buy_price} | Sell @ {strategy.sell_price}")
    logger.info(f"Will trigger in the last minute if price delta <= {THRESHOLDS.get(ticker.upper(), 0.0)}")

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
