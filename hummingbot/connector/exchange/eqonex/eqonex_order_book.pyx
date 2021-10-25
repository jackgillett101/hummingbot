#!/usr/bin/env python
from decimal import Decimal

from aiokafka import ConsumerRecord
import bz2
import logging
import numpy as np
from sqlalchemy.engine import RowProxy
from typing import (
    Any,
    Optional,
    Dict
)
import ujson

from hummingbot.logger import HummingbotLogger
from hummingbot.core.event.events import TradeType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.connector.exchange.eqonex.eqonex_constants import (
    EQONEX_TRADING_PAIR_IDS,
    EQONEX_PRICE_SCALES,
    EQONEX_QUANTITY_SCALES,
    EQONEX_TRADING_PAIR_SYMBOLS
)

_eqonexob_logger = None


cdef class EqonexOrderBook(OrderBook):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _eqonexob_logger
        if _eqonexob_logger is None:
            _eqonexob_logger = logging.getLogger(__name__)
        return _eqonexob_logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, Any],
                                       trading_pair: str,
                                       timestamp: Optional[float] = None,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        msg_ts = int(msg["bids"][0][2] * 1e-3)  # TODO is this required?

        price_scale = pow(10, EQONEX_PRICE_SCALES[trading_pair])
        quantity_scale = pow(10, EQONEX_QUANTITY_SCALES[trading_pair])
        scale_array = np.array([price_scale, quantity_scale])

        scaled_bids = np.array(msg['bids'])[:,:2] / scale_array.reshape(1,-1)
        scaled_asks = np.array(msg['asks'])[:,:2] / scale_array.reshape(1,-1)

        content = {
            "trading_pair": trading_pair,
            "update_id": msg_ts,
            "bids": scaled_bids,
            "asks": scaled_asks
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp or msg_ts)

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, Any],
                                    timestamp: Optional[float] = None,
                                    metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        msg_ts = int(timestamp * 1e-3)  # TODO is this required?

        content = {
            "trading_pair": msg["trading_pair"],
            "trade_type": float(TradeType.SELL.value) if msg["side"] == "buy" else float(TradeType.BUY.value),
            "trade_id": msg["trade_id"],
            "update_id": msg_ts,
            "amount": msg["size"],
            "price": msg["price"]
        }
        return OrderBookMessage(OrderBookMessageType.TRADE, content, timestamp or msg_ts)

    @classmethod
    def diff_message_from_exchange(cls,
                                   data: Dict[str, Any],
                                   timestamp: float = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:

        msg_ts = int(timestamp * 1e-3)

        trading_pair = EQONEX_TRADING_PAIR_SYMBOLS[data["pairId"]]
        price_scale = pow(10, EQONEX_PRICE_SCALES[trading_pair])
        quantity_scale = pow(10, EQONEX_QUANTITY_SCALES[trading_pair])
        scale_array = np.array([price_scale, quantity_scale])

        scaled_bids = np.array([]) if len(data['bids']) == 0 else np.array(data['bids'])[:,:2] / scale_array.reshape(1,-1)
        scaled_asks = np.array([]) if len(data['asks']) == 0 else np.array(data['asks'])[:,:2] / scale_array.reshape(1,-1)

        content = {
            "trading_pair": trading_pair,
            "update_id": msg_ts,
            "bids": scaled_bids,
            "asks": scaled_asks
        }
        
        return OrderBookMessage(OrderBookMessageType.DIFF, content, timestamp or msg_ts)

    @classmethod
    def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        ts = record["timestamp"]
        msg = record["json"] if type(record["json"])==dict else ujson.loads(record["json"])
        if metadata:
            msg.update(metadata)

        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["ch"].split(".")[1],
            "update_id": int(ts),
            "bids": msg["tick"]["bids"],
            "asks": msg["tick"]["asks"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        ts = record["timestamp"]
        msg = record["json"] if type(record["json"])==dict else ujson.loads(record["json"])
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["s"],
            "update_id": int(ts),
            "bids": msg["b"],
            "asks": msg["a"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def snapshot_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        ts = record.timestamp
        msg = ujson.loads(record.value.decode())
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["ch"].split(".")[1],
            "update_id": ts,
            "bids": msg["tick"]["bids"],
            "asks": msg["tick"]["asks"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def diff_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        decompressed = bz2.decompress(record.value)
        msg = ujson.loads(decompressed)
        ts = record.timestamp
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["s"],
            "update_id": ts,
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def trade_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        msg = record["json"]
        ts = record.timestamp
        data = msg["tick"]["data"][0]
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["ch"].split(".")[1],
            "trade_type": float(TradeType.BUY.value) if data["direction"] == "sell"
            else float(TradeType.SELL.value),
            "trade_id": ts,
            "update_id": ts,
            "price": data["price"],
            "amount": data["amount"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def from_snapshot(cls, msg: OrderBookMessage) -> "OrderBook":
        retval = EqonexOrderBook()
        retval.apply_snapshot(msg.bids, msg.asks, msg.update_id)
        return retval
