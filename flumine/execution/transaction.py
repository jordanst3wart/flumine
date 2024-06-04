import logging

from ..clients.baseclient import BaseClient
from ..order.order import BetfairOrder
from ..order.orderpackage import OrderPackageType, BetfairOrderPackage
from ..exceptions import ControlError

logger = logging.getLogger(__name__)


class Transaction:

    def __init__(self, market, id_: int, async_place_orders: bool, client: BaseClient):
        self.market = market
        self._client = client
        self._id = id_  # unique per market only
        self._async_place_orders = async_place_orders
        self._pending_place = []  # list of (<Order>, market_version)
        self._pending_cancel = []  # list of (<Order>, None)
        self._pending_update = []  # list of (<Order>, None)
        self._pending_replace = []  # list of (<Order>, market_version)

    def place_order(
        self,
        order: BetfairOrder,
        market_version: int = None,
        execute: bool = True,
        force: bool = False,
    ) -> bool:
        logger.info("transaction place_order used")
        order.update_client(self._client)
        if (
            execute
            and not force
            and self._validate_controls(order, OrderPackageType.PLACE) is False
        ):
            return False
        # place
        order.place(
            self.market.market_book.publish_time,
            market_version,
            self._async_place_orders,
        )
        self.market.blotter[order.id] = order
        if execute:  # handles replaceOrder
            runner_context = order.trade.strategy.get_runner_context(*order.lookup)
            runner_context.place(order.trade.id)
            self._pending_place.append((order, market_version))
        return True

    def cancel_order(
        self, order: BetfairOrder, size_reduction: float = None, force: bool = False
    ) -> bool:
        logger.info("transaction cancel_order used")
        if (
            not force
            and self._validate_controls(order, OrderPackageType.CANCEL) is False
        ):
            return False
        # cancel
        order.cancel(size_reduction)
        self._pending_cancel.append((order, None))
        return True

    def update_order(
        self, order: BetfairOrder, new_persistence_type: str, force: bool = False
    ) -> bool:
        logger.info("transaction update_order used")
        if (
            not force
            and self._validate_controls(order, OrderPackageType.UPDATE) is False
        ):
            return False
        # update
        order.update(new_persistence_type)
        self._pending_update.append((order, None))
        return True

    def replace_order(
        self,
        order: BetfairOrder,
        new_price: float,
        market_version: int = None,
        force: bool = False,
    ) -> bool:
        logger.info("transaction replace_order used")
        if (
            not force
            and self._validate_controls(order, OrderPackageType.REPLACE) is False
        ):
            return False

        order.replace(new_price)
        self._pending_replace.append((order, market_version))
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
