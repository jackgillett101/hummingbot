from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional
)
import asyncio
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.connector.in_flight_order_base import InFlightOrderBase


class EqonexInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = "live"):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state  # submitted, partial-filled, cancelling, filled, canceled, partial-canceled
        )
        self.trade_id_set = set()
        self.cancelled_event = asyncio.Event()

    @property
    def is_done(self) -> bool:
        return self.last_state in {"filled", "canceled"}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"canceled"}

    @property
    def is_failure(self) -> bool:
        return self.last_state in {"canceled"}

    @property
    def is_open(self) -> bool:
        return self.last_state in {"live", "partially_filled"}

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        """
        :param data: json data from API
        :return: formatted InFlightOrder
        """
        retval = EqonexInFlightOrder(
            client_order_id=data["client_order_id"],
            exchange_order_id=data["exchange_order_id"],
            trading_pair=data["trading_pair"],
            order_type=getattr(OrderType, data["order_type"]),
            trade_type=getattr(TradeType, data["trade_type"]),
            price=Decimal(data["price"]),
            amount=Decimal(data["amount"]),
            initial_state=data["last_state"]
        )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        return retval

    def update_with_order_status_update(self, trade_update: Dict[str, Any]) -> bool:
        """
        Updates the in flight order with order update (from private/getOrderStatus end point)
        return: True if the order gets updated otherwise False
        
        An example of an order status report:
        {'orderId': 1122334455,  'clOrdId': 'abc123', 'symbol': 'ETH/USDC[F]', 'instrumentId': 29,
         'side': '1', 'userId': 70123, 'account': 70123, 'execType': '4',
         'ordType': '2', 'ordStatus': '4', 'timeInForce': '8', 'timeStamp': '20211009-12:34:00.069',
         'execId': 0, 'targetStrategy': 0, 'isHidden': False, 'isReduceOnly': False,
         'isLiquidation': False, 'fee': 0, 'fee_scale': 0, 'feeInstrumentId': 0,
         'feeTotal': 0, 'price': 250000, 'price_scale': 2, 'quantity': 1000,
         'quantity_scale': 6, 'leavesQty': 1000, 'leavesQty_scale': 6, 'cumQty': 0,
         'cumQty_scale': 0, 'lastPx': 0, 'lastPx_scale': 2, 'avgPx': 0,
         'avgPx_scale': 0, 'lastQty': 0, 'lastQty_scale': 6}
        """
        trade_id = trade_update["orderId"]
        if str(trade_update["orderId"]) != self.exchange_order_id or trade_id in self.trade_id_set:
            # trade already recorded
            return False

        # How should we track these, no trade messages, just updated order message?
        #self.trade_id_set.add(trade_id)

        cumulative_quantity_executed = Decimal(str(trade_update["cumQty"] / pow(10, trade_update["cumQty_scale"])))
        fee = Decimal(str(trade_update["fee"] / pow(10, trade_update["fee_scale"])))
        price = Decimal(str(trade_update["lastPx"] / pow(10, trade_update["lastPx_scale"])))

        if cumulative_quantity_executed <= self.executed_amount_base:
            # No trade update
            return False

        self.executed_amount_base = cumulative_quantity_executed
        self.fee_paid = fee
        self.executed_amount_quote = cumulative_quantity_executed * price

#        if not self.fee_asset:
#            self.fee_asset = trade_update["feeInstrumentId"]

        return True