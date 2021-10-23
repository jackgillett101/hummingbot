#!/usr/bin/env python

import asyncio
import logging
from typing import (
    Optional,
    List
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.connector.exchange.eqonex.eqonex_api_user_stream_data_source import EqonexAPIUserStreamDataSource
from hummingbot.connector.exchange.eqonex.eqonex_auth import EqonexAuth


class EqonexUserStreamTracker(UserStreamTracker):
    _eqonexust_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._eqonexust_logger is None:
            cls._eqonexust_logger = logging.getLogger(__name__)
        return cls._eqonexust_logger

    def __init__(self,
                 eqonex_auth: Optional[EqonexAuth] = None,
                 trading_pairs: Optional[List[str]] = [],
                 ):
        super().__init__()
        self._ev_loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[UserStreamTrackerDataSource] = None
        self._user_stream_tracking_task: Optional[asyncio.Task] = None
        self._eqonex_auth: EqonexAuth = eqonex_auth
        self._trading_pairs: List[str] = trading_pairs

    @property
    def data_source(self) -> UserStreamTrackerDataSource:
        if not self._data_source:
            self._data_source = EqonexAPIUserStreamDataSource(eqonex_auth=self._eqonex_auth, trading_pairs=self._trading_pairs)
        return self._data_source

    @property
    def exchange_name(self) -> str:
        return "Eqonex"

    async def start(self):
        self._user_stream_tracking_task = safe_ensure_future(
            self.data_source.listen_for_user_stream(self._ev_loop, self._user_stream)
        )
        await safe_gather(self._user_stream_tracking_task)
