from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange
import zlib

CENTRALIZED = True
EXAMPLE_PAIR = "BTC/USDC"
DEFAULT_FEES = [0.08, 0.09]

def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    return exchange_trading_pair.replace("/", "-")

def convert_to_exchange_trading_pair(exchange_trading_pair: str) -> str:
    return exchange_trading_pair.replace("-", "/")


KEYS = {
    "eqonex_api_key":
        ConfigVar(key="eqonex_api_key",
                  prompt="Enter your Eqonex API key >>> ",
                  required_if=using_exchange("Eqonex"),
                  is_secure=True,
                  is_connect_key=True),
    "eqonex_secret_key":
        ConfigVar(key="eqonex_secret_key",
                  prompt="Enter your Eqonex secret key >>> ",
                  required_if=using_exchange("Eqonex"),
                  is_secure=True,
                  is_connect_key=True),
    "eqonex_user_id":
        ConfigVar(key="eqonex_user_id",
                  prompt="Enter your Eqonex user ID >>> ",
                  required_if=using_exchange("Eqonex"),
                  # Want `is_secure' to be False here, but HummingBot seems to
                  # occasionally forget it during runtime if it is set to False!
                  is_secure=True,
                  is_connect_key=True),
    "eqonex_subaccount_id":
        ConfigVar(key="eqonex_subaccount_id",
                  prompt="Enter your Eqonex subaccount ID >>> ",
                  required_if=using_exchange("Eqonex"),
                  is_secure=True,
                  is_connect_key=True),
    "eqonex_username":
        ConfigVar(key="eqonex_username",
                  prompt="Enter your Eqonex username (for WS) >>> ",
                  required_if=using_exchange("Eqonex"),
                  is_secure=True,
                  is_connect_key=True),
    "eqonex_password":
        ConfigVar(key="eqonex_password",
                  prompt="Enter your Eqonex password (for WS) >>> ",
                  required_if=using_exchange("Eqonex"),
                  is_secure=True,
                  is_connect_key=True),
}
