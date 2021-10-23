from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from urllib.parse import urljoin
import requests
from hummingbot.connector.exchange.eqonex import eqonex_utils


# URLs

EQONEX_BASE_URL = "https://eqonex.com/api/"

EQONEX_INSTRUMENTS_URL = urljoin(EQONEX_BASE_URL, 'getInstrumentPairs')
EQONEX_TRADE_HISTORY_URL = urljoin(EQONEX_BASE_URL, 'getTradeHistory?pairId={trading_pair_id}')
EQONEX_ORDER_BOOK_URL = urljoin(EQONEX_BASE_URL, 'getOrderBook?pairId={trading_pair_id}')

# Doesn't include base URL as the tail is required to generate the signature

EQONEX_SERVER_TIME = 'api/v5/public/time'

# Auth required

EQONEX_BALANCE_URL = urljoin(EQONEX_BASE_URL, 'getPositions')
EQONEX_PLACE_ORDER = urljoin(EQONEX_BASE_URL, 'order')
EQONEX_ORDER_CANCEL = urljoin(EQONEX_BASE_URL, 'cancelOrder')
EQONEX_ORDER_STATUS = urljoin(EQONEX_BASE_URL, 'getOrderStatus')
EQONEX_CANCEL_ALL = urljoin(EQONEX_BASE_URL, 'cancelAll')
EQONEX_GET_OPEN_ORDERS = urljoin(EQONEX_BASE_URL, 'getOpenOrders')
EQONEX_FEE_URL = 'api/v5/account/trade-fee?instType={instType}&instId={trading_pair}'

PUBLIC_URL_POINTS_LIMIT_ID = "PublicPoints"
PRIVATE_URL_POINTS_LIMIT_ID = "PrivatePoints"  # includes place-orders
RATE_LIMITS = [
    RateLimit(limit_id=PUBLIC_URL_POINTS_LIMIT_ID, limit=900, time_interval=1),
    RateLimit(limit_id=PRIVATE_URL_POINTS_LIMIT_ID, limit=900, time_interval=1),
    RateLimit(limit_id=EQONEX_INSTRUMENTS_URL, limit=900, time_interval=1, linked_limits=[LinkedLimitWeightPair(PUBLIC_URL_POINTS_LIMIT_ID)]),
    RateLimit(limit_id=EQONEX_TRADE_HISTORY_URL, limit=900, time_interval=1, linked_limits=[LinkedLimitWeightPair(PUBLIC_URL_POINTS_LIMIT_ID)]),
    RateLimit(limit_id=EQONEX_ORDER_BOOK_URL, limit=900, time_interval=1, linked_limits=[LinkedLimitWeightPair(PUBLIC_URL_POINTS_LIMIT_ID)]),
    RateLimit(limit_id=EQONEX_BALANCE_URL, limit=900, time_interval=1, linked_limits=[LinkedLimitWeightPair(PRIVATE_URL_POINTS_LIMIT_ID)]),
    RateLimit(limit_id=EQONEX_PLACE_ORDER, limit=5_000, time_interval=1, linked_limits=[LinkedLimitWeightPair(PRIVATE_URL_POINTS_LIMIT_ID)]),
    RateLimit(limit_id=EQONEX_ORDER_CANCEL, limit=900, time_interval=1, linked_limits=[LinkedLimitWeightPair(PRIVATE_URL_POINTS_LIMIT_ID)]),
    RateLimit(limit_id=EQONEX_ORDER_STATUS, limit=900, time_interval=1, linked_limits=[LinkedLimitWeightPair(PRIVATE_URL_POINTS_LIMIT_ID)]),
    RateLimit(limit_id=EQONEX_CANCEL_ALL, limit=900, time_interval=1, linked_limits=[LinkedLimitWeightPair(PRIVATE_URL_POINTS_LIMIT_ID)]),
    RateLimit(limit_id=EQONEX_GET_OPEN_ORDERS, limit=900, time_interval=1, linked_limits=[LinkedLimitWeightPair(PRIVATE_URL_POINTS_LIMIT_ID)]),
    RateLimit(limit_id=EQONEX_FEE_URL, limit=900, time_interval=1, linked_limits=[LinkedLimitWeightPair(PRIVATE_URL_POINTS_LIMIT_ID)])
]


# EQONEX returns all prices and quantities as integers, with a price_scale and 
# a quantity_scale adjustment required that varies by trading pair
exchange_markets = requests.get(EQONEX_INSTRUMENTS_URL)
if exchange_markets.status_code != 200:
    raise IOError(f"Error fetching Eqonex constants from API. HTTP status is {exchange_markets.status}.")

exchange_markets = exchange_markets.json()['instrumentPairs']
trading_pairs = [eqonex_utils.convert_from_exchange_trading_pair(x[1]) for x in exchange_markets]

EQONEX_TRADING_PAIR_IDS = dict(zip(trading_pairs, [x[0] for x in exchange_markets]))
EQONEX_PRICE_SCALES = dict(zip(trading_pairs, [x[4] for x in exchange_markets]))
EQONEX_QUANTITY_SCALES = dict(zip(trading_pairs, [x[5] for x in exchange_markets]))



# WS
EQONEX_WS_URI = "wss://eqonex.com/wsapi"

EQONEX_WS_CHANNEL_ACCOUNT = "account"
EQONEX_WS_CHANNEL_ORDERS = "orders"

EQONEX_WS_CHANNELS = {
    EQONEX_WS_CHANNEL_ACCOUNT,
    EQONEX_WS_CHANNEL_ORDERS
}
