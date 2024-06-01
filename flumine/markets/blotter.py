import logging
from typing import Iterable, Optional, List
from collections import defaultdict

from ..order.ordertype import OrderTypes
from ..utils import (
    calculate_unmatched_exposure,
    calculate_matched_exposure,
    STRATEGY_NAME_HASH_LENGTH,
)
from ..order.order import BaseOrder, OrderStatus

logger = logging.getLogger(__name__)

# https://www.betfair.com/aboutUs/Betfair.Charges/#charges6
IMPLIED_COMMISSION_RATE = 0.03
PENDING_STATUS = [
    OrderStatus.PENDING,
    OrderStatus.VIOLATION,
    OrderStatus.EXPIRED,
]
ORDER_TYPE_LIMIT = OrderTypes.LIMIT
ORDER_TYPES_SP = (OrderTypes.LIMIT_ON_CLOSE, OrderTypes.MARKET_ON_CLOSE)


class Blotter:
    """
    Simple and fast class to hold all orders for
    a particular market.

    `customer_order_ref` used as the key and various
    caches available for faster access.

        blotter["abc"] = <Order>  # set
        "abc" in blotter  # contains
        orders = [o for o in blotter]  # iter
        order = blotter["abc"]  # get
    """

    def __init__(self, market_id: str):
        self.market_id = market_id
        self.active = False
        self._orders = {}  # {Order.id: Order}
        # cached lists/dicts for faster lookup
        self._trades = defaultdict(list)  # {Trade.id: [Order,]}
        self._bet_id_lookup = {}  # {Order.bet_id: Order, }
        self._live_orders = []
        self._strategy_orders = defaultdict(list)
        self._strategy_selection_orders = defaultdict(list)
        self._client_orders = defaultdict(list)
        self._client_strategy_orders = defaultdict(list)

    def get_order_bet_id(self, bet_id: str) -> Optional[BaseOrder]:
        try:
            return self._bet_id_lookup[bet_id]
        except KeyError:
            return

    def strategy_orders(
        self,
        strategy,
        order_status: Optional[List[OrderStatus]] = None,
        matched_only: Optional[bool] = None,
    ) -> list:
        """Returns all orders related to a strategy."""
        orders = self._strategy_orders[strategy]
        if order_status:
            orders = [o for o in orders if o.status in order_status]
        if matched_only:
            orders = [o for o in orders if o.size_matched > 0]
        return orders

    def strategy_selection_orders(
        self,
        strategy,
        selection_id: int,
        order_status: Optional[List[OrderStatus]] = None,
        matched_only: Optional[bool] = None,
    ) -> list:
        """Returns all orders related to a strategy selection."""
        orders = self._strategy_selection_orders[(strategy, selection_id)]
        if order_status:
            orders = [o for o in orders if o.status in order_status]
        if matched_only:
            orders = [o for o in orders if o.size_matched > 0]
        return orders

    def client_orders(
        self,
        client,
        order_status: Optional[List[OrderStatus]] = None,
        matched_only: Optional[bool] = None,
    ) -> list:
        orders = self._client_orders[client]
        if order_status:
            orders = [o for o in orders if o.status in order_status]
        if matched_only:
            orders = [o for o in orders if o.size_matched > 0]
        return orders

    @property
    def live_orders(self) -> Iterable:
        return iter(list(self._live_orders))

    @property
    def has_live_orders(self) -> bool:
        return bool(self._live_orders)

    def process_closed_market(self, market, market_book) -> None:
        number_of_winners = len(
            [runner for runner in market_book.runners if runner.status == "WINNER"]
        )
        for order in self:
            for runner in market_book.runners:
                if order.selection_id == runner.selection_id:
                    order.runner_status = runner.status
                    order.market_type = market_book.market_definition.market_type
                    order.each_way_divisor = (
                        market_book.market_definition.each_way_divisor
                    )
                    if number_of_winners > market_book.number_of_winners:
                        order.number_of_dead_heat_winners = number_of_winners
                    if (
                        order.order_type.ORDER_TYPE == ORDER_TYPE_LIMIT
                        and order.order_type.price_ladder_definition == "LINE_RANGE"
                    ):
                        line_range_result = market.context.get("line_range_result")
                        if line_range_result:
                            order.line_range_result = line_range_result
                        elif order.simulated:
                            logger.warning(
                                "line_range_result unavailable, for simulation results update the market.context['line_range_result']"
                            )

    def process_cleared_orders(self, cleared_orders) -> list:
        for cleared_order in cleared_orders.orders:
            order_id = cleared_order.customer_order_ref[STRATEGY_NAME_HASH_LENGTH + 1 :]
            if order_id in self:
                self[order_id].cleared_order = cleared_order

        return [order for order in self]

    def get_exposures(self, strategy, lookup: tuple, exclusion=None) -> dict:
        """Returns strategy/selection exposures as a dict."""
        mb, ml = [], []  # matched bets, (price, size)
        ub, ul = [], []  # unmatched bets, (price, size)
        moc_win_liability = 0.0
        moc_lose_liability = 0.0
        for order in self.strategy_selection_orders(strategy, *lookup[1:]):
            if order == exclusion:
                continue
            if order.status in PENDING_STATUS:
                continue
            if order.order_type.ORDER_TYPE == ORDER_TYPE_LIMIT:
                _size_matched = order.size_matched  # cache
                _order_side = order.side
                if _size_matched:
                    if order.order_type.price_ladder_definition == "LINE_RANGE":
                        average_price_matched = 2.0
                    else:
                        average_price_matched = order.average_price_matched
                    if _order_side == "BACK":
                        mb.append((average_price_matched, _size_matched))
                    else:
                        ml.append((average_price_matched, _size_matched))
                if not order.complete:
                    _size_remaining = order.size_remaining  # cache
                    if order.order_type.price_ladder_definition == "LINE_RANGE":
                        order_type_price = 2.0
                    else:
                        order_type_price = order.order_type.price
                    if order_type_price and _size_remaining:
                        if _order_side == "BACK":
                            ub.append((order_type_price, _size_remaining))
                        else:
                            ul.append((order_type_price, _size_remaining))
            elif order.order_type.ORDER_TYPE in ORDER_TYPES_SP:
                if order.side == "BACK":
                    moc_lose_liability -= order.order_type.liability
                else:
                    moc_win_liability -= order.order_type.liability
            else:
                raise ValueError(
                    "Unexpected order type: %s" % order.order_type.ORDER_TYPE
                )
        matched_exposure = calculate_matched_exposure(mb, ml)
        unmatched_exposure = calculate_unmatched_exposure(ub, ul)

        worst_possible_profit_on_win = (
            matched_exposure[0] + unmatched_exposure[0] + moc_win_liability
        )
        worst_possible_profit_on_lose = (
            matched_exposure[1] + unmatched_exposure[1] + moc_lose_liability
        )

        return {
            "matched_profit_if_win": matched_exposure[0],
            "matched_profit_if_lose": matched_exposure[1],
            "worst_potential_unmatched_profit_if_win": unmatched_exposure[0],
            "worst_potential_unmatched_profit_if_lose": unmatched_exposure[1],
            "worst_possible_profit_on_win": worst_possible_profit_on_win,
            "worst_possible_profit_on_lose": worst_possible_profit_on_lose,
        }

    """ getters / setters """

    def complete_order(self, order) -> None:
        self._live_orders.remove(order)

    def has_order(self, customer_order_ref: str) -> bool:
        return customer_order_ref in self._orders

    __contains__ = has_order

    def __setitem__(self, customer_order_ref: str, order) -> None:
        self.active = True
        self._orders[customer_order_ref] = order
        self._bet_id_lookup[order.bet_id] = order
        self._live_orders.append(order)
        strategy = order.trade.strategy
        self._trades[order.trade.id].append(order)
        self._strategy_orders[strategy].append(order)
        self._strategy_selection_orders[
            (strategy, order.selection_id)
        ].append(order)
        client = order.client
        self._client_orders[client].append(order)
        self._client_strategy_orders[(client, strategy)].append(order)

    def __getitem__(self, customer_order_ref: str):
        return self._orders[customer_order_ref]

    def __iter__(self) -> Iterable[BaseOrder]:
        return iter(list(self._orders.values()))

    def __len__(self) -> int:
        return len(self._orders)
