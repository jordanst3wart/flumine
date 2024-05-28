import unittest
from unittest import mock

from flumine.controls.tradingcontrols import (
    StrategyExposure,
    OrderTypes,
    OrderPackageType,
)
from flumine.markets.blotter import Blotter
from flumine.order.order import OrderStatus


class TestStrategyExposure(unittest.TestCase):
    def setUp(self):
        self.market = mock.Mock()
        self.market.blotter = Blotter("market_id")
        self.mock_flumine = mock.Mock()
        self.mock_flumine.markets.markets = {"market_id": self.market}
        self.trading_control = StrategyExposure(self.mock_flumine)

    def test_init(self):
        self.assertEqual(self.trading_control.NAME, "STRATEGY_EXPOSURE")
        self.assertEqual(self.trading_control.flumine, self.mock_flumine)

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_strategy_validate_order(self, mock_on_error):
        mock_order = mock.Mock(market_id="market_id", lookup=(1, 2, 3))
        mock_order.trade.strategy.validate_order.return_value = True
        mock_runner_context = mock.Mock()
        mock_order.trade.strategy.get_runner_context.return_value = mock_runner_context
        self.trading_control._validate(mock_order, OrderPackageType.PLACE)

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_strategy_validate_order_error(self, mock_on_error):
        mock_order = mock.Mock(market_id="market_id", lookup=(1, 2, 3))
        mock_order.trade.strategy.validate_order.return_value = False
        mock_runner_context = mock.Mock()
        mock_order.trade.strategy.get_runner_context.return_value = mock_runner_context
        self.trading_control._validate(mock_order, OrderPackageType.PLACE)
        mock_on_error.assert_called_with(
            mock_order,
            mock_order.violation_msg,
        )

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_limit(self, mock_on_error):
        mock_order = mock.Mock(market_id="market_id", lookup=(1, 2, 3))
        mock_order.trade.strategy.max_order_exposure = 10
        mock_order.trade.strategy.max_selection_exposure = 100
        mock_order.order_type.ORDER_TYPE = OrderTypes.LIMIT
        mock_order.order_type.price_ladder_definition = "CLASSIC"
        mock_order.side = "BACK"
        mock_order.order_type.size = 12.0
        self.trading_control._validate(mock_order, OrderPackageType.PLACE)
        mock_on_error.assert_called_with(
            mock_order,
            "Order exposure (12.0) is greater than strategy.max_order_exposure (10)",
        )

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_limit_target(self, mock_on_error):
        mock_order = mock.Mock(market_id="market_id", lookup=(1, 2, 3))
        mock_order.trade.strategy.max_order_exposure = 10
        mock_order.trade.strategy.max_selection_exposure = 100
        mock_order.order_type.ORDER_TYPE = OrderTypes.LIMIT
        mock_order.order_type.price_ladder_definition = "CLASSIC"
        mock_order.side = "BACK"
        mock_order.order_type.size = None
        mock_order.order_type.bet_target_size = 12.0
        self.trading_control._validate(mock_order, OrderPackageType.PLACE)
        mock_on_error.assert_called_with(
            mock_order,
            "Order exposure (12.0) is greater than strategy.max_order_exposure (10)",
        )

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_limit_with_multiple_strategies_succeeds(self, mock_on_error):
        """
        The 2 orders would exceed selection_exposure limits if they were for the same strategy. But they are
        for different strategies. Assert that they do not cause a validation failure.
        """
        strategy = mock.Mock()
        strategy.max_order_exposure = 10
        strategy.max_selection_exposure = 10

        order1 = mock.Mock(market_id="market_id", lookup=(1, 2, 3))
        order1.trade.strategy.max_order_exposure = 10
        order1.trade.strategy.max_selection_exposure = 10
        order1.order_type.ORDER_TYPE = OrderTypes.LIMIT
        order1.order_type.price_ladder_definition = "CLASSIC"
        order1.side = "BACK"
        order1.order_type.price = 2.0
        order1.order_type.size = 9.0
        order1.size_remaining = 9.0
        order1.average_price_matched = 0.0
        order1.size_matched = 0

        order2 = mock.Mock(lookup=(1, 2, 3))
        order2.trade.strategy.max_order_exposure = 10
        order2.trade.strategy.max_selection_exposure = 10
        order2.trade.strategy = strategy
        order2.order_type.ORDER_TYPE = OrderTypes.LIMIT
        order2.order_type.price_ladder_definition = "CLASSIC"
        order2.side = "BACK"
        order2.order_type.price = 3.0
        order2.order_type.size = 9.0
        order2.size_remaining = 5.0
        order2.average_price_matched = 0.0
        order2.size_matched = 0

        self.market.blotter._orders = {"order1": order1, "order2": order2}

        self.trading_control._validate(order1, OrderPackageType.PLACE)
        mock_on_error.assert_not_called()

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_limit_with_multiple_strategies_fails(self, mock_on_error):
        """
        The 2 orders are from the same strategy. And are each less than strategy.max_order_exposure.
        However, in combination, they exceed strategy.max_selection_exposure.
        """
        strategy = mock.Mock()
        strategy.max_order_exposure = 10
        strategy.max_selection_exposure = 10

        order1 = mock.Mock(
            market_id="market_id",
            lookup=(1, 2, 3),
            selection_id=2,
            handicap=3,
            side="BACK",
            size_remaining=9.0,
            average_price_matched=0.0,
            size_matched=0,
            status=OrderStatus.EXECUTABLE,
            complete=False,
        )
        order1.trade.strategy = strategy
        order1.order_type.ORDER_TYPE = OrderTypes.LIMIT
        order1.order_type.price_ladder_definition = "CLASSIC"
        order1.order_type.price = 2.0
        order1.order_type.size = 9.0

        order2 = mock.Mock(
            lookup=(1, 2, 3),
            selection_id=2,
            handicap=3,
            side="BACK",
            size_remaining=5.0,
            average_price_matched=0.0,
            size_matched=0,
            status=OrderStatus.EXECUTABLE,
            complete=False,
        )
        order2.trade.strategy = strategy
        order2.order_type.ORDER_TYPE = OrderTypes.LIMIT
        order2.order_type.price_ladder_definition = "CLASSIC"
        order2.order_type.price = 3.0
        order2.order_type.size = 9.0

        self.market.blotter["order2"] = order2

        self.trading_control._validate(order1, OrderPackageType.PLACE)
        self.assertEqual(1, mock_on_error.call_count)
        mock_on_error.assert_called_with(
            order1,
            "Potential selection exposure (14.00) is greater than strategy.max_selection_exposure (10)",
        )

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_limit_line_range(self, mock_on_error):
        mock_order = mock.Mock(market_id="market_id", lookup=(1, 2, 3))
        mock_order.trade.strategy.max_order_exposure = 10
        mock_order.trade.strategy.max_selection_exposure = 100
        mock_order.order_type.ORDER_TYPE = OrderTypes.LIMIT
        mock_order.order_type.price_ladder_definition = "LINE_RANGE"
        mock_order.side = "LAY"
        mock_order.order_type.size = 12.0
        self.trading_control._validate(mock_order, OrderPackageType.PLACE)
        mock_on_error.assert_called_with(
            mock_order,
            "Order exposure (12.0) is greater than strategy.max_order_exposure (10)",
        )

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_limit_on_close(self, mock_on_error):
        mock_order = mock.Mock(market_id="market_id", lookup=(1, 2, 3))
        mock_order.trade.strategy.max_order_exposure = 10
        mock_order.trade.strategy.max_selection_exposure = 100
        mock_order.order_type.ORDER_TYPE = OrderTypes.LIMIT_ON_CLOSE
        mock_order.order_type.price_ladder_definition = "CLASSIC"
        mock_order.side = "BACK"
        mock_order.order_type.liability = 12
        self.trading_control._validate(mock_order, OrderPackageType.PLACE)
        mock_on_error.assert_called_with(
            mock_order,
            "Order exposure (12) is greater than strategy.max_order_exposure (10)",
        )

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_selection(self, mock_on_error):
        mock_market = mock.Mock()
        mock_market.blotter.get_exposures.return_value = {
            "worst_possible_profit_on_win": -12.0
        }
        self.mock_flumine.markets.markets = {"1.234": mock_market}
        mock_order = mock.Mock(market_id="1.234", lookup=(1, 2, 3), side="LAY")
        mock_order.trade.strategy.max_order_exposure = 10
        mock_order.trade.strategy.max_selection_exposure = 10
        mock_order.order_type.ORDER_TYPE = OrderTypes.LIMIT
        mock_order.order_type.price_ladder_definition = "CLASSIC"
        mock_order.order_type.size = 12.0
        mock_order.order_type.price = 1.01
        mock_market.blotter._live_orders = [mock_order]
        self.trading_control._validate(mock_order, OrderPackageType.PLACE)
        mock_on_error.assert_called_with(
            mock_order,
            "Potential selection exposure (12.12) is greater than strategy.max_selection_exposure (10)",
        )

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_selection2a(self, mock_on_error):
        """
        test_validate_selection2a expects an error, as it attempts to place a lay bet with a £36 potential
        loss, which is more than the £10 max_selection_exposure
        """
        mock_market = mock.Mock()
        mock_market.blotter = Blotter(market_id="1.234")
        self.mock_flumine.markets.markets = {"1.234": mock_market}

        mock_strategy = mock.Mock()
        mock_strategy.max_order_exposure = 100
        mock_strategy.max_selection_exposure = 10

        mock_trade = mock.Mock()
        mock_trade.strategy = mock_strategy

        mock_order = mock.Mock(market_id="1.234", lookup=(1, 2, 3), side="LAY")
        mock_order.trade = mock_trade
        mock_order.order_type.ORDER_TYPE = OrderTypes.LIMIT
        mock_order.order_type.price_ladder_definition = "CLASSIC"
        mock_order.order_type.size = 9.0
        mock_order.order_type.price = 5.0

        self.trading_control._validate(mock_order, OrderPackageType.PLACE)
        mock_on_error.assert_called_with(
            mock_order,
            "Potential selection exposure (36.00) is greater than strategy.max_selection_exposure (10)",
        )

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_selection2b(self, mock_on_error):
        """
        test_validate_selection2b expects no error.
        Unlike test_validate_selection2a, the blotter contains an existing order. The order that it attempts
        to validate hedges the existing order, and reduces the total exposure.
        """
        mock_market = mock.Mock()
        mock_market.blotter = Blotter(market_id="1.234")

        self.mock_flumine.markets.markets = {"1.234": mock_market}

        mock_strategy = mock.Mock()
        mock_strategy.max_order_exposure = 100
        mock_strategy.max_selection_exposure = 10

        mock_trade = mock.Mock()
        mock_trade.strategy = mock_strategy

        existing_matched_order = mock.Mock(
            market_id="1.234", lookup=(1, 2, 3), side="BACK", selection_id=2, handicap=3
        )
        existing_matched_order.trade = mock_trade
        existing_matched_order.order_type.ORDER_TYPE = OrderTypes.LIMIT
        existing_matched_order.size_matched = 9.0
        existing_matched_order.average_price_matched = 6.0
        existing_matched_order.size_remaining = 0.0

        mock_order = mock.Mock(
            market_id="1.234", lookup=(1, 2, 3), side="LAY", selection_id=2, handicap=3
        )
        mock_order.trade = mock_trade
        mock_order.order_type.ORDER_TYPE = OrderTypes.LIMIT
        mock_order.order_type.price_ladder_definition = "CLASSIC"
        mock_order.order_type.size = 9.0
        mock_order.order_type.price = 5.0

        mock_market.blotter["existing_order"] = existing_matched_order

        self.trading_control._validate(mock_order, OrderPackageType.PLACE)
        mock_on_error.assert_not_called()

    @mock.patch("flumine.controls.tradingcontrols.StrategyExposure._on_error")
    def test_validate_replace(self, mock_on_error):
        """
        Check that validating a REPLACE order does not lead to double counting of exposures.

        In this test, max_selection_exposure is 10.0, and the potential liability on the order is £9.
        If exposures are double counted, the validation would fail.
        If exposures aren't double counted, then the validation will succeed
        """
        strategy = mock.Mock()
        strategy.max_order_exposure = 10
        strategy.max_selection_exposure = 10

        order1 = mock.Mock(
            market_id="market_id",
            lookup=(1, 2, 3),
            selection_id=1234,
            side="BACK",
            average_price_matched=0.0,
            size_matched=0,
            handicap=0,
            status=OrderStatus.EXECUTABLE,
            complete=False,
        )
        order1.trade.strategy = strategy
        order1.order_type.ORDER_TYPE = OrderTypes.LIMIT
        order1.order_type.price_ladder_definition = "CLASSIC"
        order1.order_type.price = 2.0
        order1.order_type.size = 9.0
        order1.size_remaining = 9.0

        self.market.blotter._strategy_selection_orders = {(strategy, 2, 3): [order1]}

        # Show that the exposures aren't double counted when REPLACE is used
        self.trading_control._validate(order1, OrderPackageType.REPLACE)
        mock_on_error.assert_not_called()

        # Just to be sure, check that the validation fails if we try to validate order1 as a PLACE
        self.trading_control._validate(order1, OrderPackageType.PLACE)
        mock_on_error.assert_called_once()
