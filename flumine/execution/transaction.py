import logging

from ..clients.baseclient import BaseClient
from ..order.order import BetfairOrder
from ..order.orderpackage import OrderPackageType
from ..exceptions import ControlError

logger = logging.getLogger(__name__)


class Transaction:

    def __init__(self, market, id_: int, async_place_orders: bool, client: BaseClient):
        self.market = market
        self._client = client
        self._id = id_  # unique per market only
        self._async_place_orders = async_place_orders

    def place_order(
        self,
        order: BetfairOrder,
        market_version: int = None,
        execute: bool = True,
        force: bool = False,
    ) -> bool:
        order.update_client(self._client)
        if (
            execute
            and not force
            and self._validate_controls(order, OrderPackageType.PLACE) is False
        ):
            return False

        order.place(
            self.market.market_book.publish_time,
            market_version,
            self._async_place_orders,
        )
        self.market.blotter[order.id] = order
        if execute:  # handles replaceOrder
            runner_context = order.trade.strategy.get_runner_context(*order.lookup)
            runner_context.place(order.trade.id)
        return True

    def cancel_order(
        self, order: BetfairOrder, size_reduction: float = None, force: bool = False
    ) -> bool:
        if (
            not force
            and self._validate_controls(order, OrderPackageType.CANCEL) is False
        ):
            return False

        order.cancel(size_reduction)
        return True

    def update_order(
        self, order: BetfairOrder, new_persistence_type: str, force: bool = False
    ) -> bool:
        if (
            not force
            and self._validate_controls(order, OrderPackageType.UPDATE) is False
        ):
            return False

        order.update(new_persistence_type)
        return True

    def replace_order(
        self,
        order: BetfairOrder,
        new_price: float,
        market_version: int = None,
        force: bool = False,
    ) -> bool:
        if (
            not force
            and self._validate_controls(order, OrderPackageType.REPLACE) is False
        ):
            return False

        order.replace(new_price)
        return True

    # one of these trading_controls is empty
    def _validate_controls(
        self, order: BetfairOrder, package_type: OrderPackageType
    ) -> bool:
        # return False on violation
        try:
            len(f"len flumine trading {self.market.flumine.trading_controls}")
            len(
                f"this should be empty len client trading {self._client.trading_controls}"
            )
            for control in self.market.flumine.trading_controls:
                control(order, package_type)
            # this should be empty
            for control in self._client.trading_controls:
                control(order, package_type)
        except ControlError:
            return False
        else:
            return True
