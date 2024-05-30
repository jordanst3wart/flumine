import unittest
from unittest import mock
from betfairlightweight.resources.bettingresources import PriceSize

from flumine.order.order import OrderStatus, OrderTypes
from flumine import config
from flumine.markets.market import Market
from flumine.markets.markets import Markets
from flumine.order.order import (
    BaseOrder,
    BetfairOrder,
)
from flumine.order import process
from flumine.strategy.strategy import Strategies
from flumine.utils import create_cheap_hash


class BaseOrderTest(unittest.TestCase):
    def setUp(self) -> None:
        mock_client = mock.Mock(paper_trade=False)
        self.mock_trade = mock.Mock(client=mock_client)
        self.mock_order_type = mock.Mock()
        self.order = BaseOrder(self.mock_trade, "BACK", self.mock_order_type, 1)
        config.simulated = True

    def tearDown(self) -> None:
        config.simulated = False

    @mock.patch("flumine.order.process.process_current_order")
    def test_process_current_orders_with_default_sep(self, mock_process_current_order):
        mock_add_market = mock.Mock()
        market_book = mock.Mock()
        markets = Markets()
        market = Market(
            flumine=mock.Mock(), market_id="market_id", market_book=market_book
        )
        markets.add_market("market_id", market)
        strategies = Strategies()
        cheap_hash = create_cheap_hash("strategy_name", 13)
        trade = mock.Mock(market_id="market_id")
        trade.strategy.name_hash = cheap_hash
        current_order = mock.Mock(
            customer_order_ref=f"{cheap_hash}I123", market_id="market_id", bet_id=None
        )
        betfair_order = BetfairOrder(trade=trade, side="BACK", order_type=mock.Mock())
        betfair_order.id = "123"
        betfair_order.complete = True
        market.blotter["123"] = betfair_order
        event = mock.Mock(event=[mock.Mock(orders=[current_order])])

        process.process_current_orders(
            markets=markets,
            strategies=strategies,
            event=event,
            add_market=mock_add_market,
        )
        mock_process_current_order.assert_called_with(
            betfair_order,
            current_order,
        )
        self.assertEqual(market.blotter._live_orders, [])

    def test_process_current_order(self):
        mock_order = mock.Mock(status=OrderStatus.EXECUTABLE)
        mock_order.current_order.status = "EXECUTION_COMPLETE"
        mock_current_order = mock.Mock()
        process.process_current_order(mock_order, mock_current_order)
        mock_order.update_current_order.assert_called_with(mock_current_order)
        mock_order.execution_complete.assert_called()

    @mock.patch("flumine.order.process.OrderEvent")
    def test_process_current_order_async(self, mock_order_event):
        mock_order = mock.Mock(status=OrderStatus.EXECUTABLE, async_=True, bet_id=None)
        mock_order.current_order.status = "EXECUTION_COMPLETE"
        mock_current_order = mock.Mock(bet_id=1234)
        process.process_current_order(mock_order, mock_current_order)
        mock_order.update_current_order.assert_called_with(mock_current_order)
        mock_order.execution_complete.assert_called()
        self.assertEqual(mock_order.bet_id, 1234)
        mock_order.responses.placed.assert_called_with()
        # mock_order_event.assert_called_with(mock_order)
