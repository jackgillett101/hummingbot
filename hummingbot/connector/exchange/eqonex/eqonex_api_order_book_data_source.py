#!/usr/bin/env python

import aiohttp
import asyncio

import json
import logging
import pandas as pd
import time
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
)
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils import async_ttl_cache
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.eqonex.eqonex_order_book import EqonexOrderBook
from hummingbot.connector.exchange.eqonex import eqonex_utils
from hummingbot.connector.exchange.eqonex.eqonex_constants import (
    EQONEX_INSTRUMENTS_URL,
    EQONEX_TRADE_HISTORY_URL,
    EQONEX_ORDER_BOOK_URL,
    EQONEX_TRADING_PAIR_IDS,
    EQONEX_PRICE_SCALES,
    EQONEX_WS_URI,
)

from dateutil.parser import parse as dataparse


class EqonexAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _eqonexaobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._eqonexaobds_logger is None:
            cls._eqonexaobds_logger = logging.getLogger(__name__)
        return cls._eqonexaobds_logger

    def __init__(self, trading_pairs: List[str]):
        super().__init__(trading_pairs)
        self._trading_pairs: List[str] = trading_pairs

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Performs the necessary API request(s) to get all currently active trading pairs on the
        exchange and returns a pandas.DataFrame with each row representing one active trading pair.

        Also the the base and quote currency should be represented under the baseAsset and quoteAsset
        columns respectively in the DataFrame.

        Refer to Calling a Class method for an example on how to test this particular function.
        Returned data frame should have trading pair as index and include usd volume, baseAsset and quoteAsset
        """
        async with aiohttp.ClientSession() as client:
            async with client.get(EQONEX_INSTRUMENTS_URL) as products_response:

                products_response: aiohttp.ClientResponse = products_response
                if products_response.status != 200:
                    raise IOError(f"Error fetching active Eqonex markets. HTTP status is {products_response.status}.")

                data = await products_response.json()
                data = [x for x in data['instrumentPairs'] if x[8] == "PAIR"]
                all_markets: pd.DataFrame = pd.DataFrame.from_records(data=data)
                all_markets[1] = all_markets[1].apply(lambda x: eqonex_utils.convert_from_exchange_trading_pair(x))

                return all_markets

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        # Returns a List of str, representing each active trading pair on the exchange.
        async with aiohttp.ClientSession() as client:
            async with client.get(EQONEX_INSTRUMENTS_URL) as products_response:

                products_response: aiohttp.ClientResponse = products_response
                if products_response.status != 200:
                    raise IOError(f"Error fetching active Eqonex markets. HTTP status is {products_response.status}.")

                data = await products_response.json()
                data = data['instrumentPairs']

                trading_pairs = []
                for item in data:
                    if item[8] == "PAIR":
                        trading_pair = eqonex_utils.convert_from_exchange_trading_pair(item[1])
                        trading_pairs.append(trading_pair)

        return trading_pairs

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)

            snapshot_msg: OrderBookMessage = EqonexOrderBook.snapshot_message_from_exchange(
                snapshot,
                trading_pair,
                timestamp=snapshot['bids'][0][2],
                metadata={"trading_pair": trading_pair})
            order_book: OrderBook = self.order_book_create_function()
            order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
            return order_book

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        async with aiohttp.ClientSession() as client:

            # EQONEX has no 'tickers' endpoint, need to query trade histories independently, this
            # could be slow. The WS API *does* support ticker however, so use that wherever possible!
            out: Dict[str, float] = {}
            for trading_pair in trading_pairs:
                trading_pair_id = EQONEX_TRADING_PAIR_IDS[trading_pair]

                async with client.get(EQONEX_TRADE_HISTORY_URL.format(trading_pair_id=trading_pair_id)) as products_response:

                    products_response: aiohttp.ClientResponse = products_response
                    if products_response.status != 200:
                        raise IOError(f"Error fetching active Eqonex markets. HTTP status is {products_response.status}.")

                    data = await products_response.json()
                    data = data['trades']
                    price_scale = EQONEX_PRICE_SCALES[trading_pair]

                    out[trading_pair] = float(data[0][1] / pow(10, price_scale))

            return out

    async def get_trading_pairs(self) -> List[str]:
        if not self._trading_pairs:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._trading_pairs = active_markets['instId'].tolist()
            except Exception:
                self._trading_pairs = []
                self.logger().network(
                    "Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg="Error getting active exchange information. Check network connection."
                )
        return self._trading_pairs

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        """Fetches order book snapshot for a particular trading pair from the exchange REST API."""
        trading_pair_id = EQONEX_TRADING_PAIR_IDS[trading_pair]
        async with client.get(EQONEX_ORDER_BOOK_URL.format(trading_pair_id=trading_pair_id)) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching Eqonex market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            api_data = await response.read()
            data: Dict[str, Any] = json.loads(api_data)

            return data

    @classmethod
    def iso_to_timestamp(cls, date: str):
        return dataparse(date).timestamp()

    async def listen_for_trades(self, ev_loop: Optional[asyncio.BaseEventLoop], output: asyncio.Queue):
        """Subscribes to the trade channel of the exchange. Adds incoming messages(of filled orders) to the output queue, to be processed by"""

        while True:
            try:
                trading_pairs: List[str] = self._trading_pairs
                async with websockets.connect(EQONEX_WS_URI) as ws:
                    ws: websockets.WebSocketClientProtocol = ws

                    for trading_pair in trading_pairs:
                        subscribe_request: Dict[str, Any] = {
                            "op": "subscribe",
                            "args": [
                                {
                                    "channel": "trades",
                                    "instType": "SPOT",
                                    "instId": trading_pair,
                                }
                            ]
                        }
                        await ws.send(json.dumps(subscribe_request))

                    async for raw_msg in self._inner_messages(ws):
                        decoded_msg: str = raw_msg

                        self.logger().debug("decode menssage:" + decoded_msg)

                        if '"event":"subscribe"' in decoded_msg:
                            self.logger().debug(f"Subscribed to channel, full message: {decoded_msg}")
                        elif '"channel": "orders"' in decoded_msg:
                            self.logger().debug(f"Received new trade: {decoded_msg}")

                            for data in json.loads(decoded_msg)['data']:
                                trading_pair = data['instId']
                                trade_message: OrderBookMessage = EqonexOrderBook.trade_message_from_exchange(
                                    data, data['uTime'], metadata={"trading_pair": trading_pair}
                                )
                                self.logger().debug(f"Putting msg in queue: {str(trade_message)}")
                                output.put_nowait(trade_message)
                        else:
                            self.logger().debug(f"Unrecognized message received from Eqonex websocket: {decoded_msg}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    pong_waiter = await ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_order_book_diffs(self, ev_loop: Optional[asyncio.BaseEventLoop], output: asyncio.Queue):
        """Fetches or Subscribes to the order book snapshots for each trading pair. Additionally, parses the incoming message into a OrderBookMessage and appends it into the output Queue."""
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with websockets.connect(EQONEX_WS_URI) as ws:
                    ws: websockets.WebSocketClientProtocol = ws

                    for trading_pair in trading_pairs:
                        subscribe_request: Dict[str, Any] = {
                            "requestId": "orderbook",
                            "event": "S",
                            "types": [2],
                            "symbols": [eqonex_utils.convert_to_exchange_trading_pair(trading_pair)],
                            "level": 2
                        }
                        #self.logger().info(subscribe_request)
                        await ws.send(json.dumps(subscribe_request).replace(" ", ""))

                    async for raw_msg in self._inner_messages(ws):
                        decoded_msg: str = raw_msg
                        #self.logger().info(decoded_msg)

                        if '"response":"Subscribed Successfully"' in decoded_msg:
                            self.logger().debug(f"Subscribed to channel, full message: {decoded_msg}")

                        elif 'bids' in decoded_msg and 'asks' in decoded_msg:
                            msg = json.loads(decoded_msg)
                            ts = (msg['bids'] if len(msg['bids']) > 0 else msg['asks'])[0][2]  # Timestamps are on the bid/ask records
                            order_book_message: OrderBookMessage = EqonexOrderBook.diff_message_from_exchange(msg, int(ts))
                            output.put_nowait(order_book_message)

                        else:
                            self.logger().debug(f"Unrecognized message received from Eqonex websocket: {decoded_msg}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """Fetches or Subscribes to the order book deltas(diffs) for each trading pair. Additionally, parses the incoming message into a OrderBookMessage and appends it into the output Queue."""
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with aiohttp.ClientSession() as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_msg: OrderBookMessage = EqonexOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                trading_pair,
                                timestamp=snapshot['bids'][0][2], # Timestamp is on each bid/ask record
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
                            await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
