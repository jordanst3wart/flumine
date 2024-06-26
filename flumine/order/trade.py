import uuid
import logging
import datetime
import collections
from enum import Enum
from typing import Union, Type, Optional
from betfairlightweight.resources.bettingresources import CurrentOrder

from ..strategy.strategy import BaseStrategy
from .order import BetfairOrder
from .ordertype import LimitOrder, LimitOnCloseOrder, MarketOnCloseOrder
from ..exceptions import OrderError
from .. import config

logger = logging.getLogger(__name__)


class TradeStatus(Enum):
    PENDING = "Pending"  # pending exchange processing
    LIVE = "Live"
    COMPLETE = "Complete"


class Trade:
    def __init__(
        self,
        market_id: str,
        selection_id: int,
        strategy: BaseStrategy,
        notes: collections.OrderedDict = None,  # trade notes (e.g. triggers/market state)
        place_reset_seconds: float = 0.0,  # seconds to wait since `runner_context.place` before allowing another order
        reset_seconds: float = 0.0,  # seconds to wait since `runner_context.reset` before allowing another order
    ):
        self.id = str(uuid.uuid4())
        self.market_id = market_id
        self.selection_id = selection_id
        self.strategy = strategy
        self.notes = notes or collections.OrderedDict()
        self.market_notes = None  # back,lay,lpt
        self.place_reset_seconds = place_reset_seconds
        self.reset_seconds = reset_seconds
        self.orders = []  # all orders linked to trade
        self.offset_orders = []  # pending offset orders once initial order has matched
        self.status_log = []
        self.status = TradeStatus.LIVE
        self.date_time_created = datetime.datetime.now(datetime.UTC)
        self.date_time_complete = None

    # status
    def _update_status(self, status: TradeStatus) -> None:
        self.status_log.append(status)
        self.status = status
        if logger.isEnabledFor(logging.INFO):
            logger.info("Trade status update: %s" % self.status.value, extra=self.info)
        if self.complete:
            self.complete_trade()

    def complete_trade(self) -> None:
        self._update_status(TradeStatus.COMPLETE)
        self.date_time_complete = datetime.datetime.now(datetime.UTC)
        # reset strategy context
        runner_context = self.strategy.get_runner_context(
            self.market_id, self.selection_id
        )
        runner_context.reset(self.id)

    @property
    def complete(self) -> bool:
        if self.status != TradeStatus.LIVE:
            return False
        for order in self.offset_orders:
            if not order.complete:
                return False
        for order in self.orders:
            if not order.complete:
                return False
        return True

    def create_order(
        self,
        side: str,
        order_type: Union[LimitOrder, LimitOnCloseOrder, MarketOnCloseOrder],
        order: Type[BetfairOrder] = BetfairOrder,
        sep: str = config.order_sep,
        context: dict = None,
        notes: collections.OrderedDict = None,
    ) -> BetfairOrder:
        if order_type.EXCHANGE != order.EXCHANGE:
            raise OrderError(
                "Incorrect order/order_type exchange combination for trade.create_order"
            )
        order = order(
            trade=self,
            side=side,
            order_type=order_type,
            sep=sep,
            context=context,
            notes=notes,
        )
        self.orders.append(order)
        return order

    def create_order_replacement(
        self,
        order: BetfairOrder,
        new_price: float,
        size: float,
        date_time_created: datetime,
    ) -> BetfairOrder:
        """Create new order due to replace
        execution"""
        order_type = LimitOrder(
            price=new_price,
            size=size,
            persistence_type=order.order_type.persistence_type,
        )
        replacement_order = BetfairOrder(
            trade=self,
            side=order.side,
            order_type=order_type,
            sep=order.sep,
            context=order.context,
            notes=order.notes,
        )
        replacement_order.date_time_created = date_time_created
        replacement_order.responses.date_time_created = date_time_created
        replacement_order.update_client(order.client)
        self.orders.append(replacement_order)
        return replacement_order

    @property
    def elapsed_seconds(self) -> Optional[float]:
        for order in self.orders:
            return order.elapsed_seconds  # return first order

    @property
    def notes_str(self) -> str:
        return ",".join(str(x) for x in self.notes.values())

    @property
    def info(self) -> dict:
        return {
            "id": self.id,
            "strategy": str(self.strategy),
            "place_reset_seconds": self.place_reset_seconds,
            "reset_seconds": self.reset_seconds,
            "orders": [o.id for o in self.orders],
            "offset_orders": [o.id for o in self.offset_orders],
            "notes": self.notes_str,
            "market_notes": self.market_notes,
            "status": self.status.value if self.status else None,
            "status_log": ", ".join([s.value for s in self.status_log]),
        }

    def __enter__(self):
        # todo raise error if already pending?
        self._update_status(TradeStatus.PENDING)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self._update_status(TradeStatus.LIVE)
        else:
            logger.critical("Trade error in %s" % self.id, exc_info=True)
