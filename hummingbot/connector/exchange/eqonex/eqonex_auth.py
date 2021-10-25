import base64
from datetime import datetime
import hashlib
import hmac
import time
from typing import (
    Any,
    Dict
)
from collections import OrderedDict
import json

class EqonexAuth:
    def __init__(self, api_key: str, secret_key: str, user_id: str, username: str, password: str, subaccount_id: str):
        self.api_key: str = api_key
        self.secret_key: str = secret_key
        self.user_id: str = user_id
        self.subaccount_id: str = subaccount_id
        self.username: str = username
        self.password: str = password

    def generate_auth_dict(
        self,
        url: str,
        params: Dict[str, Any] = None
    ) -> Dict[str, any]:

        params = {**{"userId": int(self.user_id)}, **params}

        if self.subaccount_id is not None:
            params["account"] = int(self.subaccount_id)

        requestbody = json.dumps(params, separators=(',', ':')).replace(" ", "").replace("\'", "\"")

        signature = hmac.new(self.secret_key.encode(), requestbody.encode(),  hashlib.sha384).hexdigest()
        headers = {'requestToken': self.api_key, 'signature': signature}
        auth_dict = {'headers': headers, 'params': requestbody}

        return auth_dict

    @staticmethod
    def keysort(dictionary: Dict[str, str]) -> Dict[str, str]:
        return OrderedDict(sorted(dictionary.items(), key=lambda t: t[0]))

    @staticmethod
    def get_timestamp() -> str:
        milliseconds = int(time.time() * 1000)
        utc = datetime.utcfromtimestamp(milliseconds // 1000)
        return utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-6] + "{:03d}".format(int(milliseconds) % 1000) + 'Z'

    def get_signature(self, timestamp, method, path_url, body: str) -> str:
        auth = timestamp + method + path_url
        if body:
            auth += body

        _hash = hmac.new(self.secret_key.encode(), auth.encode(), hashlib.sha256).digest()
        signature = base64.b64encode(_hash).decode()
        return signature

    def add_auth_to_params(self,
                           method: str,
                           path_url: str,
                           args: Dict[str, Any] = {}) -> Dict[str, Any]:

        uppercase_method = method.upper()

        timestamp = self.get_timestamp()

        request = {
            "EQONEX-KEY": self.api_key,
            "EQONEX-SIGN": self.get_signature(timestamp, uppercase_method, path_url, args),
            "EQONEX-TIMESTAMP": timestamp,
            "EQONEX-PASSPHRASE": self.passphrase,
        }

        sorted_request = self.keysort(request)

        return sorted_request

    def generate_ws_auth(self):
        return {
                "requestId": "WS-HummingBot-connect",
                "event": "S",
                "types":[6],
                "username": self.username,
                "password": self.password,
                "account": self.subaccount_id,
                "isInitialSnap": "true"
            }

    def generate_ws_subscribe_balance(self):
        return {
                "requestId": "WS-HummingBot-balance",
                "event": "S",
                "types":[9],
                "username": self.username,
                "password": self.password,
                "account": self.subaccount_id
            }