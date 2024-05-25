import logging

from .baseflumine import BaseFlumine
from .events.events import EventType
from . import worker

logger = logging.getLogger(__name__)

MARKET_BOOK_EVENT = EventType.MARKET_BOOK
CURRENT_ORDERS_EVENT = EventType.CURRENT_ORDERS
MARKET_CATALOGUE_EVENT = EventType.MARKET_CATALOGUE
CLEARED_MARKETS_EVENT = EventType.CLEARED_MARKETS
CLEARED_ORDERS_EVENT = EventType.CLEARED_ORDERS
CLOSE_MARKET_EVENT = EventType.CLOSE_MARKET
TERMINATOR_EVENT = EventType.TERMINATOR


class Flumine(BaseFlumine):
    def run(self) -> None:
        """
        Main run thread
        """
        event_handlers = {
            MARKET_BOOK_EVENT: self._process_market_books,
            CURRENT_ORDERS_EVENT: self._process_current_orders,
            MARKET_CATALOGUE_EVENT: self._process_market_catalogues,
            CLEARED_MARKETS_EVENT: self._process_cleared_markets,
            CLEARED_ORDERS_EVENT: self._process_cleared_orders,
            CLOSE_MARKET_EVENT: self._process_close_market,
            TERMINATOR_EVENT: "break",
        }

        with self:
            while True:
                event = self.handler_queue.get()
                handler = event_handlers.get(event.EVENT_TYPE)

                if handler == "break":
                    break
                elif handler:
                    handler(event)
                else:
                    logger.error("Unknown item in handler_queue: %s" % str(event))
                del event

    def _add_default_workers(self):
        client_timeouts = [
            client.betting_client.session_timeout for client in self.clients
        ]
        ka_interval = min((min(client_timeouts) / 2), 1200)
        self.add_worker(
            worker.BackgroundWorker(
                self, function=worker.keep_alive, interval=ka_interval
            )
        )
        self.add_worker(
            worker.BackgroundWorker(
                self,
                function=worker.poll_market_catalogue,
                interval=120,
                start_delay=10,  # wait for streams to populate
            )
        )
        if not all([client.market_recording_mode for client in self.clients]):
            self.add_worker(
                worker.BackgroundWorker(
                    self,
                    function=worker.poll_account_balance,
                    interval=120,
                    start_delay=10,  # wait for login
                )
            )
            self.add_worker(
                worker.BackgroundWorker(
                    self,
                    function=worker.poll_market_closure,
                    interval=60,
                    start_delay=10,  # wait for login
                )
            )

    def __repr__(self) -> str:
        return "<Flumine>"

    def __str__(self) -> str:
        return "<Flumine>"
