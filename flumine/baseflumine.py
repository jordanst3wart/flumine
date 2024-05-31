import time
import queue
import logging
import threading
from typing import Type
from betfairlightweight import resources

from .controls.basecontrol import BaseControl
from .strategy.strategy import Strategies, BaseStrategy
from .streams.streams import Streams
from .events import events
from .worker import BackgroundWorker
from .clients import Clients, BaseClient
from .markets.markets import Markets
from .markets.market import Market
from .markets.middleware import Middleware, SimulatedMiddleware
from .execution.betfairexecution import BetfairExecution
from .execution.simulatedexecution import SimulatedExecution
from .order.process import process_current_orders
from .controls.tradingcontrols import (
    StrategyExposure,
)
from .exceptions import ClientError
from . import config, utils

logger = logging.getLogger(__name__)


class BaseFlumine:
    SIMULATED = False

    def __init__(self, client: BaseClient = None):
        """
        Base framework class

        :param client: flumine client instance
        """
        logger.info("Running custom flumine")
        self._running = False
        self.streams = Streams(self)

        self.clients = Clients()
        self.handler_queue = queue.Queue()
        self.markets = Markets()
        self._market_middleware = []
        self.strategies = Strategies()

        # order execution class
        self.simulated_execution = SimulatedExecution(self)
        self.betfair_execution = BetfairExecution(self)

        if client:
            self.add_client(client)

        self.trading_controls = []
        self.add_trading_control(StrategyExposure)

        self._workers = []

    def run(self) -> None:
        raise NotImplementedError

    def add_client(self, client: BaseClient) -> None:
        self.clients.add_client(client)
        self.streams.add_client(client)
        # add execution
        client.add_execution(self)
        # add simulation middleware if required
        if self.clients.simulated and not any(
            isinstance(val, SimulatedMiddleware) for val in self._market_middleware
        ):
            logger.info("using simulated middleware")
            self.add_market_middleware(SimulatedMiddleware())

    def add_strategy(self, strategy: BaseStrategy) -> None:
        logger.info("Adding strategy %s", strategy)
        self.streams(strategy)  # create required streams
        self.strategies(strategy, self.clients, self)  # store in strategies

    def add_worker(self, worker: BackgroundWorker) -> None:
        logger.info("Adding worker %s", worker.name)
        self._workers.append(worker)

    def add_client_control(
        self, client: BaseClient, client_control: Type[BaseControl], **kwargs
    ) -> None:
        logger.info("Adding client control %s", client_control.NAME)
        client.trading_controls.append(client_control(self, client, **kwargs))

    def add_trading_control(self, trading_control: Type[BaseControl], **kwargs) -> None:
        logger.info("Adding trading control %s", trading_control.NAME)
        self.trading_controls.append(trading_control(self, **kwargs))

    def add_market_middleware(self, middleware: Middleware) -> None:
        logger.info("Adding market middleware %s", middleware)
        self._market_middleware.append(middleware)

    def _add_default_workers(self) -> None:
        return

    def _process_market_books(self, event: events.MarketBookEvent) -> None:
        for market_book in event.event:
            market_id = market_book.market_id

            # check latency (only if marketBook is from a stream update)
            if market_book.streaming_snap is False:
                latency = time.time() - (market_book.publish_time_epoch / 1e3)
                if latency > 2:
                    logger.warning(
                        "High latency between current time and MarketBook publish time",
                        extra={
                            "market_id": market_id,
                            "latency": latency,
                            "pt": market_book.publish_time,
                        },
                    )

            market = self.markets.markets.get(market_id)
            market_is_new = market is None
            if market_is_new:
                market = self._add_market(market_id, market_book)
            elif market.closed:
                self.markets.add_market(market_id, market)

            if market_book.status == "CLOSED":
                self.handler_queue.put(events.CloseMarketEvent(market_book))
                continue

            # process market
            market(market_book)

            # process middleware
            for middleware in self._market_middleware:
                utils.call_middleware_error_handling(middleware, market)

            for strategy in self.strategies:
                if market_book.streaming_unique_id in strategy.stream_ids:
                    if market_is_new:
                        utils.call_strategy_error_handling(
                            strategy.process_new_market, market, market_book
                        )
                    if utils.call_strategy_error_handling(
                        strategy.check_market_book, market, market_book
                    ):
                        utils.call_strategy_error_handling(
                            strategy.process_market_book, market, market_book
                        )

    def process_order_package(self, order_package) -> None:
        """Execute through client."""
        order_package.client.execution.handler(order_package)

    def _add_market(self, market_id: str, market_book: resources.MarketBook) -> Market:
        logger.debug("Adding: %s to markets", market_id)
        market = Market(self, market_id, market_book)
        self.markets.add_market(market_id, market)
        for middleware in self._market_middleware:
            middleware.add_market(market)
        return market

    def _remove_market(self, market: Market, clear: bool = True) -> None:
        logger.debug("Removing market %s", market.market_id, extra=self.info)
        for middleware in self._market_middleware:
            middleware.remove_market(market)
        for strategy in self.strategies:
            strategy.remove_market(market.market_id)
        if clear:
            self.markets.remove_market(market.market_id)

    def _process_market_catalogues(self, event: events.MarketCatalogueEvent) -> None:
        for market_catalogue in event.event:
            market = self.markets.markets.get(market_catalogue.market_id)
            if market:
                if market.market_catalogue is None:
                    market.market_catalogue = market_catalogue
                    logger.debug(
                        "Created marketCatalogue for %s",
                        market.market_id,
                        extra=market.info,
                    )
                else:
                    market.market_catalogue = market_catalogue
                    logger.debug(
                        "Updated marketCatalogue for %s",
                        market.market_id,
                        extra=market.info,
                    )
                market.update_market_catalogue = False

    # TODO investigate why this function is called so much
    def _process_current_orders(self, event: events.CurrentOrdersEvent) -> None:
        # update state
        if event.event:
            process_current_orders(
                self.markets, self.strategies, event, self._add_market
            )
        # iterating over all the markets seems a bit dumb...
        # shouldn't it just iterate over the markets that have orders?
        # or just the current orders
        for market in self.markets:
            if market.closed is False and market.blotter.active:
                for strategy in self.strategies:
                    strategy_orders = market.blotter.strategy_orders(strategy)
                    if strategy_orders:
                        utils.call_process_orders_error_handling(
                            strategy, market, strategy_orders
                        )

    def _process_close_market(self, event: events.CloseMarketEvent) -> None:
        logger.info("close market event actually called")
        market_book = event.event
        recorder = False
        market_id = market_book.market_id
        stream_id = market_book.streaming_unique_id
        market = self.markets.markets.get(market_id)

        # process market
        if market.closed is False:
            market.close_market()
        market(market_book)
        market.blotter.process_closed_market(market, event.event)

        for strategy in self.strategies:
            if stream_id in strategy.stream_ids:
                strategy.process_closed_market(market, event.event)

        if self.clients.simulated:
            # simulate ClearedOrdersEvent
            cleared_orders = resources.ClearedOrders(
                moreAvailable=False, clearedOrders=[]
            )
            cleared_orders.market_id = market_id
            self._process_cleared_orders(events.ClearedOrdersEvent(cleared_orders))
            for client in self.clients:
                # simulate ClearedMarketsEvent
                cleared_markets = resources.ClearedOrders(
                    moreAvailable=False,
                    clearedOrders=[market.cleared(client)],
                )
                self._process_cleared_markets(
                    events.ClearedMarketsEvent(cleared_markets)
                )
        logger.debug("Market closed", extra={"market_id": market_id, **self.info})

        # check for markets that have been closed for x seconds and remove
        if not self.clients.simulated:
            # due to monkey patching this will clear simulated markets
            closed_markets = [
                m
                for m in self.markets
                if m.closed
                and m.elapsed_seconds_closed
                and m.elapsed_seconds_closed > 3600
            ]
            for market in closed_markets:
                self._remove_market(market)
        else:
            self._remove_market(market, clear=False)

    def _process_cleared_orders(self, event):
        market_id = event.event.market_id
        market = self.markets.markets.get(market_id)

        meta_orders = market.blotter.process_cleared_orders(event.event)
        logger.debug(
            "Market cleared",
            extra={
                "market_id": market_id,
                "order_count": len(meta_orders),
                **self.info,
            },
        )

    def _process_cleared_markets(self, event: events.ClearedMarketsEvent):
        # todo update blotter?
        for cleared_market in event.event.orders:
            logger.info(
                "Market level cleared",
                extra={
                    "market_id": cleared_market.market_id,
                    "profit": cleared_market.profit,
                    "bet_count": cleared_market.bet_count,
                },
            )

    def _process_end_flumine(self) -> None:
        self.strategies.finish(self)

    @property
    def info(self) -> dict:
        return {
            "clients": self.clients.info,
            "markets": {
                "market_count": len(self.markets),
                "open_market_count": len(self.markets.open_market_ids),
            },
            "streams": [s for s in self.streams],
            "threads": threading.enumerate(),
            "threads_len": len(threading.enumerate()),
        }

    def __enter__(self):
        logger.info("Starting flumine", extra=self.info)
        if len(self.clients) == 0:
            raise ClientError("No clients provided")

        config.simulated = self.SIMULATED
        # login
        self.clients.login()
        self.clients.update_account_details()
        # add default and start all workers
        self._add_default_workers()
        for w in self._workers:
            w.start()
        # start strategies
        self.strategies.start(self)
        # start streams
        self.streams.start()

        self._running = True

    def __exit__(self, *args):
        # shutdown framework
        self._process_end_flumine()
        # shutdown workers
        for w in self._workers:
            w.shutdown()
        # shutdown streams
        self.streams.stop()
        # shutdown thread pools
        self.simulated_execution.shutdown()
        self.betfair_execution.shutdown()
        # logout
        self.clients.logout()
        self._running = False
        logger.info("Exiting flumine", extra=self.info)
