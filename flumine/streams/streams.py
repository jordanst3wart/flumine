import time
import logging
from typing import Optional, Union, Iterator

from ..clients.baseclient import BaseClient
from ..clients.exchangetype import ExchangeType
from ..strategy.strategy import BaseStrategy
from .marketstream import MarketStream
from .datastream import DataStream
from .orderstream import OrderStream
from .simulatedorderstream import SimulatedOrderStream

logger = logging.getLogger(__name__)


class Streams:
    def __init__(self, flumine):
        self.flumine = flumine
        self._streams = []
        self._stream_id = 0

    def __call__(self, strategy: BaseStrategy) -> None:
        logger.info("this function is not called at all")
        if self.flumine.SIMULATED:  # probably don't need this
            logger.info("actually need simulated stream thing")
            markets = strategy.market_filter.get("markets")
            market_types = strategy.market_filter.get("market_types")
            country_codes = strategy.market_filter.get("country_codes")
            event_processing = strategy.market_filter.get("event_processing", False)
            events = strategy.market_filter.get("events")
            listener_kwargs = strategy.market_filter.get("listener_kwargs", {})
            if markets and events:
                logger.warning(
                    "Markets and events found for strategy %s skipping as flumine can only handle one type",
                    strategy,
                )
            elif markets is None and events is None:
                logger.warning("No markets or events found for strategy %s", strategy)
            elif events:
                raise NotImplementedError()
        else:
            self.add_stream(strategy)

    def add_client(self, client: BaseClient) -> None:
        if client.order_stream:
            if client.paper_trade:
                self.add_simulated_order_stream(client)
            elif client.EXCHANGE == ExchangeType.BETFAIR:
                self.add_order_stream(client)

    def add_stream(self, strategy: BaseStrategy) -> None:
        # markets
        if isinstance(strategy.market_filter, dict) or strategy.market_filter is None:
            market_filters = [strategy.market_filter]
        else:
            market_filters = strategy.market_filter
        for market_filter in market_filters:
            for stream in self:  # check if market stream already exists
                if (
                    isinstance(stream, strategy.stream_class)
                    and stream.market_filter == market_filter
                    and stream.market_data_filter == strategy.market_data_filter
                    and stream.streaming_timeout == strategy.streaming_timeout
                    and stream.conflate_ms == strategy.conflate_ms
                ):
                    logger.info(
                        "Using %s (%s) for strategy %s",
                        strategy.stream_class,
                        stream.stream_id,
                        strategy,
                    )
                    strategy.streams.append(stream)
                    break
            else:  # nope? lets create a new one
                stream_id = self._increment_stream_id()
                logger.info(
                    "Creating new %s (%s) for strategy %s",
                    strategy.stream_class,
                    stream_id,
                    strategy,
                )
                stream = strategy.stream_class(
                    flumine=self.flumine,
                    stream_id=stream_id,
                    market_filter=market_filter,
                    market_data_filter=strategy.market_data_filter,
                    streaming_timeout=strategy.streaming_timeout,
                    conflate_ms=strategy.conflate_ms,
                )
                self._streams.append(stream)
                strategy.streams.append(stream)

    def add_order_stream(
        self,
        client: BaseClient,
        conflate_ms: int = None,
        streaming_timeout: float = 0.25,
    ) -> OrderStream:
        stream_id = self._increment_stream_id()
        stream = OrderStream(
            flumine=self.flumine,
            stream_id=stream_id,
            conflate_ms=conflate_ms,
            streaming_timeout=streaming_timeout,
            client=client,
        )
        self._streams.append(stream)
        return stream

    def add_simulated_order_stream(
        self,
        client: BaseClient,
        conflate_ms: int = None,
        streaming_timeout: float = 0.25,
    ) -> SimulatedOrderStream:
        logger.warning("Client %s now paper trading", client.betting_client.username)
        stream_id = self._increment_stream_id()
        stream = SimulatedOrderStream(
            flumine=self.flumine,
            stream_id=stream_id,
            conflate_ms=conflate_ms,
            streaming_timeout=streaming_timeout,
            client=client,
            custom=True,
        )
        self._streams.append(stream)
        return stream

    def start(self) -> None:
        if not self.flumine.SIMULATED:
            logger.info("Starting streams..")
            for stream in self:
                stream.start()
                # wait for successful start
                while not stream.custom and not stream.stream_running:
                    time.sleep(0.25)

    def stop(self) -> None:
        for stream in self:
            stream.stop()

    def _increment_stream_id(self) -> int:
        self._stream_id += int(1e3)
        return self._stream_id

    def __iter__(self) -> Iterator[Union[MarketStream, DataStream]]:
        return iter(self._streams)

    def __len__(self) -> int:
        return len(self._streams)
