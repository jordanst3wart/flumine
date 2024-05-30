import logging
from typing import Optional

from ..markets.markets import Markets
from ..order.order import BaseOrder, OrderStatus
from ..order.trade import Trade
from ..strategy.strategy import Strategies
from ..events.events import OrderEvent
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


def process_current_orders(
    markets: Markets, strategies: Strategies, event, add_market
) -> None:
    for current_orders in event.event:
        client = current_orders.client
        for current_order in current_orders.orders:
            order_id = current_order.customer_order_ref[STRATEGY_NAME_HASH_LENGTH + 1 :]
            order = markets.get_order(
                market_id=current_order.market_id,
                order_id=order_id,
            )

            if (
                order.bet_id and order.bet_id != current_order.bet_id
            ):  # replaceOrder handling (hacky)
                logger.info("hacky replaceOrder handling is being used")
                order = markets.get_order_from_bet_id(
                    market_id=current_order.market_id,
                    bet_id=current_order.bet_id,
                )
                if order is None:
                    continue
            else:
                logger.info("hacky replaceOrder handling is not being used")
            # process order status
            process_current_order(order, current_order)
            # complete order if required
            if order.complete:
                market = markets.markets[order.market_id]
                if order in market.blotter.live_orders:
                    market.blotter.complete_order(order)


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
