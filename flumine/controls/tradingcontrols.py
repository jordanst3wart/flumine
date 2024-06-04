import logging

from .basecontrol import BaseControl
from ..order.order import BaseOrder
from ..order.ordertype import OrderTypes
from ..order.orderpackage import OrderPackageType

logger = logging.getLogger(__name__)


class StrategyExposure(BaseControl):
    """
    Validates:
        - `strategy.validate_order` function
        - `strategy.max_order_exposure` is not violated if order is executed
        - `strategy.max_selection_exposure` is not violated if order is executed

    Exposure calculation includes pending,
    executable and execution complete orders.
    """

    NAME = "STRATEGY_EXPOSURE"

    def _validate(self, order: BaseOrder, package_type: OrderPackageType) -> None:
        if package_type == OrderPackageType.PLACE:
            # strategy.validate_order
            runner_context = order.trade.strategy.get_runner_context(*order.lookup)
            if order.trade.strategy.validate_order(runner_context, order) is False:
                return self._on_error(order, order.violation_msg)

        if package_type in (
            OrderPackageType.PLACE,
            OrderPackageType.REPLACE,
        ):
            strategy = order.trade.strategy
            if order.order_type.ORDER_TYPE == OrderTypes.LIMIT:
                if order.order_type.price_ladder_definition in ["CLASSIC", "FINEST"]:
                    size = order.order_type.size or order.order_type.bet_target_size
                    if order.side == "BACK":
                        order_exposure = size
                    else:
                        order_exposure = (order.order_type.price - 1) * size
                elif order.order_type.price_ladder_definition == "LINE_RANGE":
                    # All bets are struck at 2.0
                    order_exposure = (
                        order.order_type.size or order.order_type.bet_target_size
                    )
                else:
                    return self._on_error(order, "Unknown priceLadderDefinition")
            elif order.order_type.ORDER_TYPE == OrderTypes.LIMIT_ON_CLOSE:
                order_exposure = order.order_type.liability
            elif order.order_type.ORDER_TYPE == OrderTypes.MARKET_ON_CLOSE:
                order_exposure = order.order_type.liability
            else:
                return self._on_error(order, "Unknown order_type")

            # per order
            if order_exposure > strategy.max_order_exposure:
                return self._on_error(
                    order,
                    "Order exposure ({0}) is greater than strategy.max_order_exposure ({1})".format(
                        order_exposure, strategy.max_order_exposure
                    ),
                )

            # per selection
            market = self.flumine.markets.markets[order.market_id]
            if package_type == OrderPackageType.REPLACE:
                exclusion = order
            else:
                exclusion = None

            current_exposures = market.blotter.get_exposures(
                strategy, lookup=order.lookup, exclusion=exclusion
            )
            """
            We use -min(...) in the below, as "worst_possible_profit_on_X" will be negative if the position is
            at risk of loss, while exposure values are always atleast zero.
            Exposure refers to the largest potential loss.
            """
            if order.side == "BACK":
                current_selection_exposure = -current_exposures[
                    "worst_possible_profit_on_lose"
                ]
            else:
                current_selection_exposure = -current_exposures[
                    "worst_possible_profit_on_win"
                ]
            potential_exposure = current_selection_exposure + order_exposure
            if potential_exposure > strategy.max_selection_exposure:
                return self._on_error(
                    order,
                    "Potential selection exposure ({0:.2f}) is greater than strategy.max_selection_exposure ({1})".format(
                        potential_exposure,
                        strategy.max_selection_exposure,
                    ),
                )
