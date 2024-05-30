import logging
from typing import Type, Iterator, Union, List
from betfairlightweight import filters
from betfairlightweight.resources import MarketBook, MarketCatalogue, Race, CricketMatch

from .runnercontext import RunnerContext
from ..markets.market import Market
from ..streams.marketstream import BaseStream, MarketStream
from ..utils import create_cheap_hash, STRATEGY_NAME_HASH_LENGTH

logger = logging.getLogger(__name__)

DEFAULT_MARKET_DATA_FILTER = filters.streaming_market_data_filter(
    fields=[
        "EX_ALL_OFFERS",
        "EX_TRADED",
        "EX_TRADED_VOL",
        "EX_LTP",
        "EX_MARKET_DEF",
        "SP_TRADED",
        "SP_PROJECTED",
    ]
)


class BaseStrategy:
    """
    Strategy object to process MarketBook data
    from streams, order placement and handling
    logic to be added where required. Only
    MarketBooks from provided filter and data
    filter are processed.
    Runner context available to store current
    live trades.
    """

    def __init__(
        self,
        market_filter: Union[dict, list],
        market_data_filter: dict = None,
        sports_data_filter: List[
            str
        ] = None,  # 'raceSubscription', 'cricketSubscription'
        streaming_timeout: float = None,
        conflate_ms: int = None,
        stream_class: Type[BaseStream] = MarketStream,
        name: str = None,
        context: dict = None,
        max_selection_exposure: float = 100,
        max_order_exposure: float = 10,
        multi_order_trades: bool = False,
    ):
        """
        :param market_filter: Streaming market filter dict or list of market filters
        :param market_data_filter: Streaming market data filter
        :param sports_data_filter: Streaming sports data filter (e.g. ["raceSubscription"])
        :param streaming_timeout: Streaming timeout in seconds, will call snap() on cache
        :param conflate_ms: Streaming conflation
        :param stream_class: Can be Market or Data (raw)
        :param name: Strategy name (will default to class name)
        :param context: Dictionary holding additional user specific vars
        :param max_selection_exposure: Max exposure per selection
        :param max_order_exposure: Max exposure per order
        :param multi_order_trades: allow multiple live orders per trade
        """
        self.market_filter = market_filter
        self.market_data_filter = market_data_filter or DEFAULT_MARKET_DATA_FILTER
        self.sports_data_filter = sports_data_filter or []
        self.streaming_timeout = streaming_timeout
        self.conflate_ms = conflate_ms
        self.stream_class = stream_class
        self._name = name
        self.context = context or {}
        self.max_selection_exposure = max_selection_exposure
        self.max_order_exposure = max_order_exposure
        self.clients = None
        self.multi_order_trades = multi_order_trades

        self._invested = {}  # {(marketId, selectionId, handicap): RunnerContext}
        self.streams = []  # list of streams strategy is subscribed
        # cache
        self.name_hash = create_cheap_hash(self.name, STRATEGY_NAME_HASH_LENGTH)

    def add(self, flumine) -> None:
        # called when strategy is added to framework
        return

    def start(self, flumine) -> None:
        # called when flumine starts but before streams start
        # e.g. subscribe to extra streams
        return

    def process_new_market(self, market: Market, market_book: MarketBook) -> None:
        # called when a market is newly added to the framework
        return

    def check_market_book(self, market: Market, market_book: MarketBook) -> bool:
        # process_market_book only executed if this returns True
        return False

    def process_market_book(self, market: Market, market_book: MarketBook) -> None:
        # process marketBook; place/cancel/replace orders
        return

    def process_orders(self, market: Market, orders: list) -> None:
        # process list of Order objects for strategy and Market
        return

    def process_closed_market(self, market: Market, market_book: MarketBook) -> None:
        # process marketBook after closure
        return

    def finish(self, flumine) -> None:
        # called before flumine ends
        return

    def remove_market(self, market_id: str) -> None:
        to_remove = []
        for invested in self._invested:
            if invested[0] == market_id:
                to_remove.append(invested)
        for i in to_remove:
            del self._invested[i]

    def validate_order(self, runner_context: RunnerContext, order) -> bool:
        # allow multiple orders per trade
        if self.multi_order_trades:
            if order.trade.id in runner_context.live_trades:
                return True
        # validate context
        reset_elapsed_seconds = runner_context.reset_elapsed_seconds
        if reset_elapsed_seconds and reset_elapsed_seconds < order.trade.reset_seconds:
            order.violation_msg = (
                "strategy.validate_order failed: reset_elapsed_seconds (%s) < reset_seconds (%s)"
                % (
                    reset_elapsed_seconds,
                    order.trade.reset_seconds,
                )
            )
            return False

        placed_elapsed_seconds = runner_context.placed_elapsed_seconds
        if (
            placed_elapsed_seconds
            and placed_elapsed_seconds < order.trade.place_reset_seconds
        ):
            order.violation_msg = (
                "strategy.validate_order failed: placed_elapsed_seconds (%s) < place_reset_seconds (%s)"
                % (
                    placed_elapsed_seconds,
                    order.trade.place_reset_seconds,
                )
            )
            return False
        return True

    def get_runner_context(
        self, market_id: str, selection_id: int, handicap: float = 0
    ) -> RunnerContext:
        try:
            return self._invested[(market_id, selection_id, handicap)]
        except KeyError:
            self._invested[(market_id, selection_id, handicap)] = runner_context = (
                RunnerContext(selection_id)
            )
            return runner_context

    def market_cached(self, market_id: str) -> bool:
        """Checks if market_id is present in any of the strategy's stream caches."""
        # This is a slower but more comprehensive test to find out whether a market is
        # associated with a strategy than checking "if streaming_unique_id in self.stream_ids".
        for stream in self.streams:
            try:
                if market_id in stream._listener.stream._caches:
                    return True
            except AttributeError:
                continue
        return False

    @property
    def stream_ids(self) -> Union[list, set]:
        return [stream.stream_id for stream in self.streams]

    @property
    def info(self) -> dict:
        return {
            "strategy_name": self.name,
            "market_filter": self.market_filter,
            "market_data_filter": self.market_data_filter,
            "streaming_timeout": self.streaming_timeout,
            "conflate_ms": self.conflate_ms,
            "stream_ids": list(self.stream_ids),
            "max_selection_exposure": self.max_selection_exposure,
            "max_order_exposure": self.max_order_exposure,
            "context": self.context,
            "name_hash": self.name_hash,
        }

    @property
    def name(self) -> str:
        return self._name or self.__class__.__name__

    def __str__(self):
        return "{0}".format(self.name)


class Strategies:
    def __init__(self):
        self._strategies = []

    def __call__(self, strategy: BaseStrategy, clients, flumine) -> None:
        if strategy.name in [s.name for s in self]:
            logger.warning("Strategy of same name '%s' already added", strategy)
        strategy.clients = clients
        self._strategies.append(strategy)
        try:
            strategy.add(flumine)
        except TypeError:  # Wrong call signature
            logger.warning(
                "Deprecation warning: Call signature of BaseStrategy.add(self) "
                "has changed to BaseStrategy.add(self, flumine). Please update "
                f"{strategy.__class__.__name__} to match the new call signature."
            )
            strategy.add()

    def start(self, flumine) -> None:
        for s in self:
            try:
                s.start(flumine)
            except TypeError:  # Wrong call signature
                logger.warning(
                    "Deprecation warning: Call signature of BaseStrategy.start(self) "
                    "has changed to BaseStrategy.start(self, flumine). Please update "
                    f"{s.__class__.__name__} to match the new call signature."
                )
                s.start()

    def finish(self, flumine) -> None:
        for s in self:
            s.finish(flumine)

    @property
    def hashes(self) -> dict:
        return {strategy.name_hash: strategy for strategy in self}

    def __iter__(self) -> Iterator[BaseStrategy]:
        return iter(self._strategies)

    def __len__(self) -> int:
        return len(self._strategies)
