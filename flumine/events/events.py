import datetime
from enum import Enum


class EventType(Enum):
    TERMINATOR = "Terminator"
    # betfair objects
    MARKET_CATALOGUE = "MarketCatalogue"
    MARKET_BOOK = "MarketBook"
    RAW_DATA = "Raw streaming data"
    CURRENT_ORDERS = "CurrentOrders"
    CLEARED_MARKETS = "ClearedMarkets"
    CLEARED_ORDERS = "ClearedOrders"
    BALANCE = "Balance"
    # flumine objects
    STRATEGY = "Strategy"
    MARKET = "Market"
    TRADE = "Trade"
    ORDER = "Order"
    ORDER_PACKAGE = "Order package"
    CLOSE_MARKET = "Closed market"


class QueueType(Enum):
    HANDLER = "Handler queue"
    LOGGING = "Logging queue"


class BaseEvent:
    EVENT_TYPE = None
    QUEUE_TYPE = None

    __slots__ = ["_time_created", "event", "callback"]

    def __init__(self, event):
        self._time_created = datetime.datetime.now(datetime.UTC)
        self.event = event

    @property
    def elapsed_seconds(self):
        return (
            datetime.datetime.now(datetime.UTC) - self._time_created
        ).total_seconds()

    def __str__(self):
        return "<{0} [{1}]>".format(self.EVENT_TYPE.name, self.QUEUE_TYPE.name)


# HANDLER


class MarketCatalogueEvent(BaseEvent):
    EVENT_TYPE = EventType.MARKET_CATALOGUE
    QUEUE_TYPE = QueueType.HANDLER


class MarketBookEvent(BaseEvent):
    EVENT_TYPE = EventType.MARKET_BOOK
    QUEUE_TYPE = QueueType.HANDLER


# can probably delete
class RawDataEvent(BaseEvent):
    EVENT_TYPE = EventType.RAW_DATA
    QUEUE_TYPE = QueueType.HANDLER


class CurrentOrdersEvent(BaseEvent):
    EVENT_TYPE = EventType.CURRENT_ORDERS
    QUEUE_TYPE = QueueType.HANDLER


class ClearedMarketsEvent(BaseEvent):
    EVENT_TYPE = EventType.CLEARED_MARKETS
    QUEUE_TYPE = QueueType.HANDLER


class ClearedOrdersEvent(BaseEvent):
    EVENT_TYPE = EventType.CLEARED_ORDERS
    QUEUE_TYPE = QueueType.HANDLER


class CloseMarketEvent(BaseEvent):
    EVENT_TYPE = EventType.CLOSE_MARKET
    QUEUE_TYPE = QueueType.HANDLER


class MarketEvent(BaseEvent):
    EVENT_TYPE = EventType.MARKET
    QUEUE_TYPE = QueueType.LOGGING


class OrderEvent(BaseEvent):
    EVENT_TYPE = EventType.ORDER
    QUEUE_TYPE = QueueType.LOGGING
