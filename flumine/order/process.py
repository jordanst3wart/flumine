import logging

from ..events import events
from ..markets.markets import Markets
from ..order.order import BaseOrder, OrderStatus
from ..utils import STRATEGY_NAME_HASH_LENGTH

logger = logging.getLogger(__name__)

"""
Various functions to update current order status
and update status.

Loop through each current order:
    order = Lookup order in market using marketId and orderId
    if order is None (not present locally):
        create local order using data and make executable #todo!!!
    if order betId != current_order betId:
        Get order using current_order betId due to replace request (new betId)
    if order:
        process()
            update current status
            if betId is None (async placement):
                Update betId and log through logging_control

orderStatus: PENDING, EXECUTION_COMPLETE, EXECUTABLE, EXPIRED
"""


def process_current_orders(markets: Markets, event: events.CurrentOrdersEvent) -> None:
    for current_orders in event.event:
        for current_order in current_orders.orders:
            order_id = current_order.customer_order_ref[STRATEGY_NAME_HASH_LENGTH + 1 :]
            order = markets.get_order(
                market_id=current_order.market_id,
                order_id=order_id,
            )

            process_current_order(order, current_order)
            # complete order if required
            if order.complete:
                market = markets.markets[order.market_id]
                if order in market.blotter.live_orders:
                    market.blotter.complete_order(order)

# this function makes no sense to me
def process_current_order(order: BaseOrder, current_order) -> None:
    # update
    order.update_current_order(current_order)
    # pickup async orders
    if order.async_ and order.bet_id is None and current_order.bet_id:
        order.responses.placed()
        order.bet_id = current_order.bet_id
    # update status
    if order.bet_id and order.status == OrderStatus.PENDING:
        if order.current_order.status == "EXECUTABLE":
            order.executable()
        elif order.current_order.status in ["EXECUTION_COMPLETE", "EXPIRED"]:
            order.execution_complete()
    elif order.status == OrderStatus.EXECUTABLE:
        if order.current_order.status in ["EXECUTION_COMPLETE", "EXPIRED"]:
            order.execution_complete()
