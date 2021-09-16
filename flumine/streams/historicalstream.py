import logging
import datetime
from typing import Optional
from betfairlightweight.streaming import StreamListener, HistoricalGeneratorStream
from betfairlightweight.streaming.stream import MarketStream, RaceStream
from betfairlightweight.streaming.cache import MarketBookCache, RaceCache
from betfairlightweight.resources.baseresource import BaseResource
from betfairlightweight.compat import json

from .basestream import BaseStream
from ..exceptions import ListenerError

logger = logging.getLogger(__name__)


class FlumineMarketStream(MarketStream):
    """
    Custom bflw stream to speed up processing
    by limiting to inplay/not inplay or limited
    seconds to start.
    `_process` updated to not call `on_process`
    which reduces some function calls.
    """

    def _process(self, data: list, publish_time: int) -> bool:
        active = False
        for market_book in data:
            if "id" not in market_book:
                continue
            market_id = market_book["id"]
            full_image = market_book.get("img", False)
            market_book_cache = self._caches.get(market_id)

            if (
                full_image or market_book_cache is None
            ):  # historic data does not contain img
                if "marketDefinition" not in market_book:
                    logger.warning(
                        "[%s: %s]: Missing marketDefinition on market %s resulting "
                        "in potential missing data in the MarketBook (make sure "
                        "EX_MARKET_DEF is requested)"
                        % (self, self.unique_id, market_id)
                    )
                market_book_cache = MarketBookCache(
                    market_id,
                    publish_time,
                    self._lightweight,
                    self._calculate_market_tv,
                    self._cumulative_runner_tv,
                )
                self._caches[market_id] = market_book_cache
                logger.info(
                    "[%s: %s]: %s added, %s markets in cache"
                    % (self, self.unique_id, market_id, len(self._caches))
                )

            # listener_kwargs filtering
            active = True
            if "marketDefinition" in market_book:
                _definition_status = market_book["marketDefinition"].get("status")
                _definition_in_play = market_book["marketDefinition"].get("inPlay")
                _definition_market_time = market_book["marketDefinition"].get(
                    "marketTime"
                )
            else:
                _definition_status = market_book_cache._definition_status
                _definition_in_play = market_book_cache._definition_in_play
                _definition_market_time = market_book_cache.market_definition[
                    "marketTime"
                ]

            # if market is not open (closed/suspended) process regardless
            if _definition_status == "OPEN":
                if self._listener.inplay:
                    if not _definition_in_play:
                        active = False
                elif self._listener.seconds_to_start:
                    _now = datetime.datetime.utcfromtimestamp(publish_time / 1e3)
                    _market_time = BaseResource.strip_datetime(_definition_market_time)
                    seconds_to_start = (_market_time - _now).total_seconds()
                    if seconds_to_start > self._listener.seconds_to_start:
                        active = False
                if self._listener.inplay is False:
                    if _definition_in_play:
                        active = False
            # check if refresh required
            if active and not market_book_cache.active:
                market_book_cache.refresh_cache()

            market_book_cache.update_cache(market_book, publish_time, active=active)
            self._updates_processed += 1
        return active


class FlumineRaceStream(RaceStream):
    """
    `_process` updated to not call `on_process`
    which reduces some function calls.
    # todo snap optimisation?
    """

    def _process(self, race_updates: list, publish_time: int) -> bool:
        for update in race_updates:
            market_id = update["mid"]
            race_cache = self._caches.get(market_id)
            if race_cache is None:
                race_id = update.get("id")
                race_cache = RaceCache(
                    market_id, publish_time, race_id, self._lightweight
                )
                self._caches[market_id] = race_cache
                logger.info(
                    "[%s: %s]: %s added, %s markets in cache"
                    % (self, self.unique_id, market_id, len(self._caches))
                )
            race_cache.update_cache(update, publish_time)
            self._updates_processed += 1
        return True


class HistoricListener(StreamListener):
    """
    Custom listener to restrict processing by
    inplay or seconds_to_start.
    """

    def __init__(self, inplay: bool = None, seconds_to_start: float = None, **kwargs):
        super(HistoricListener, self).__init__(**kwargs)
        self.inplay = inplay
        self.seconds_to_start = seconds_to_start

    def _add_stream(self, unique_id: int, operation: str):
        if operation == "marketSubscription":
            return FlumineMarketStream(self, unique_id)
        elif operation == "orderSubscription":
            raise ListenerError("Unable to process order stream")
        elif operation == "raceSubscription":
            return FlumineRaceStream(self, unique_id)

    def on_data(self, raw_data: str) -> Optional[bool]:
        try:
            data = json.loads(raw_data)
        except ValueError:
            logger.error("value error: %s" % raw_data)
            return

        # remove error handler / operation check

        # skip on_change / on_update as we know it is always an update
        publish_time = data["pt"]
        return self.stream._process(data[self.stream._lookup], publish_time)


class FlumineHistoricalGeneratorStream(HistoricalGeneratorStream):
    """Super fast historical stream"""

    def _read_loop(self) -> dict:
        self.listener.register_stream(self.unique_id, self.operation)
        listener_on_data = self.listener.on_data  # cache functions
        stream_snap = self.listener.stream.snap
        with open(self.file_path, "r") as f:
            for update in f:
                if listener_on_data(update):
                    yield stream_snap()


class HistoricalStream(BaseStream):

    LISTENER = HistoricListener
    MAX_LATENCY = None

    def run(self) -> None:
        pass

    def handle_output(self) -> None:
        pass

    def create_generator(self):
        self._listener.debug = False  # prevent logging calls on each update (slow)
        self._listener.update_clk = (
            False  # do not update clk on updates (not required when backtesting)
        )
        stream = FlumineHistoricalGeneratorStream(
            file_path=self.market_filter,
            listener=self._listener,
            operation=self.operation,
            unique_id=self.stream_id,
        )
        return stream.get_generator()
