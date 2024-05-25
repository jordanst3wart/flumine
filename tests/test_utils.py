import logging
import unittest
import datetime
from unittest import mock

from flumine import utils, FlumineException


class UtilsTest(unittest.TestCase):
    def setUp(self) -> None:
        logging.disable(logging.CRITICAL)

    def test_chunks(self):
        self.assertEqual([i for i in utils.chunks([1, 2, 3], 1)], [[1], [2], [3]])

    def test_create_cheap_hash(self):
        self.assertEqual(
            utils.create_cheap_hash("test"),
            utils.create_cheap_hash("test"),
        )
        self.assertEqual(len(utils.create_cheap_hash("test", 16)), 16)

    @mock.patch("flumine.utils.Decimal")
    def test_as_dec(self, mock_decimal):
        self.assertEqual(utils.as_dec(2.00), mock_decimal.return_value)

    # def test_arrange(self):
    #     utils.arange()

    def test_make_prices(self):
        prices = utils.make_prices(utils.MIN_PRICE, utils.CUTOFFS)
        self.assertEqual(len(prices), 350)

    def test_make_line_prices(self):
        prices = utils.make_line_prices(0.5, 9.5, 1.0)
        self.assertEqual(prices, [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5])

    def test_get_price(self):
        self.assertEqual(
            utils.get_price(
                [{"price": 12, "size": 120}, {"price": 34, "size": 120}], 0
            ),
            12,
        )
        self.assertEqual(
            utils.get_price(
                [{"price": 12, "size": 120}, {"price": 34, "size": 120}], 1
            ),
            34,
        )
        self.assertIsNone(
            utils.get_price([{"price": 12, "size": 120}, {"price": 34, "size": 120}], 3)
        )
        self.assertIsNone(utils.get_price([], 3))

    def test_get_size(self):
        self.assertEqual(
            utils.get_size([{"price": 12, "size": 12}, {"price": 34, "size": 34}], 0),
            12,
        )
        self.assertEqual(
            utils.get_size([{"price": 12, "size": 12}, {"price": 34, "size": 34}], 1),
            34,
        )
        self.assertIsNone(
            utils.get_size([{"price": 12, "size": 12}, {"price": 34, "size": 34}], 3)
        )
        self.assertIsNone(utils.get_size([], 3))

    def test_get_sp(self):
        mock_runner = mock.Mock()
        mock_runner.sp = []
        self.assertIsNone(utils.get_sp(mock_runner))
        mock_runner.sp = None
        self.assertIsNone(utils.get_sp(mock_runner))
        mock_runner = mock.Mock()
        mock_runner.sp.actual_sp = "NaN"
        self.assertIsNone(utils.get_sp(mock_runner))
        mock_runner.sp.actual_sp = "Infinity"
        self.assertIsNone(utils.get_sp(mock_runner))
        mock_runner.sp.actual_sp = 12.2345
        self.assertEqual(utils.get_sp(mock_runner), 12.2345)

    def test_calculate_matched_exposure(self):
        self.assertEqual(utils.calculate_matched_exposure([], []), (0.0, 0.0))
        self.assertEqual(
            utils.calculate_matched_exposure([(0, 0)], [(0, 0), (0, 0)]), (0.0, 0.0)
        )
        self.assertEqual(utils.calculate_matched_exposure([(5.6, 2)], []), (9.2, -2.0))
        self.assertEqual(utils.calculate_matched_exposure([], [(5.6, 2)]), (-9.2, 2.0))
        self.assertEqual(
            utils.calculate_matched_exposure([], [(5.6, 2), (5.8, 2)]), (-18.8, 4.0)
        )
        self.assertEqual(
            utils.calculate_matched_exposure([(5.6, 2)], [(5.6, 2)]), (0.0, 0.0)
        )
        self.assertEqual(
            utils.calculate_matched_exposure([(5.6, 2), (100, 20)], [(5.6, 2)]),
            (1980.0, -20.0),
        )
        self.assertEqual(
            utils.calculate_matched_exposure([(5.6, 2), (100, 20)], [(10, 1000)]),
            (-7010.80, 978.0),
        )
        self.assertEqual(
            utils.calculate_matched_exposure([(10, 2)], [(5, 2)]), (10.0, 0.0)
        )
        self.assertEqual(
            utils.calculate_matched_exposure([(10, 2)], [(5, 4)]), (2.0, 2.0)
        )
        self.assertEqual(
            utils.calculate_matched_exposure([(10, 2)], [(5, 8)]), (-14.0, 6.0)
        )

        self.assertEqual(
            utils.calculate_matched_exposure([(5.6, 200)], [(5.6, 100)]),
            (460.0, -100.0),
        )

    def test_calculate_unmatched_exposure(self):
        self.assertEqual(utils.calculate_unmatched_exposure([], []), (0.0, 0.0))
        self.assertEqual(
            utils.calculate_unmatched_exposure([(0, 0)], [(0, 0), (0, 0)]), (0.0, 0.0)
        )
        self.assertEqual(
            utils.calculate_unmatched_exposure([(5.6, 2.0)], []), (0.0, -2.0)
        )
        self.assertEqual(
            utils.calculate_unmatched_exposure([], [(5.6, 2)]), (-9.2, 0.0)
        )
        self.assertEqual(
            utils.calculate_unmatched_exposure([], [(5.6, 2), (5.8, 2)]), (-18.8, 0.0)
        )
        self.assertEqual(
            utils.calculate_unmatched_exposure([(5.6, 2)], [(5.6, 2)]), (-9.2, -2.0)
        )
        self.assertEqual(
            utils.calculate_unmatched_exposure([(5.6, 2), (100, 20)], [(5.6, 2)]),
            (-9.2, -22.0),
        )
        self.assertEqual(
            utils.calculate_unmatched_exposure(
                [(5.6, 2.0), (100.0, 20.0)], [(10, 1000)]
            ),
            (-9000.0, -22.0),
        )
        self.assertEqual(
            utils.calculate_unmatched_exposure([(10.0, 2.0)], [(5, 2)]), (-8.0, -2.0)
        )
        self.assertEqual(
            utils.calculate_unmatched_exposure([(10, 2)], [(5, 4)]), (-16.0, -2.0)
        )
        self.assertEqual(
            utils.calculate_unmatched_exposure([(10, 2)], [(5, 8)]), (-32.0, -2.0)
        )

        self.assertEqual(
            utils.calculate_unmatched_exposure([(5.6, 200.0)], [(5.6, 100)]),
            (-460.0, -200.0),
        )

    def test_wap(self):
        self.assertEqual(
            utils.wap([(123456789, 1.5, 100), (123456789, 1.6, 100)]), (200, 1.55)
        )
        self.assertEqual(utils.wap([]), (0, 0))
        self.assertEqual(utils.wap([(123456789, 1.5, 0)]), (0, 0))

    def test_call_strategy_error_handling(self):
        mock_strategy_check = mock.Mock()
        mock_market = mock.Mock()
        mock_market_book = mock.Mock()
        utils.call_strategy_error_handling(
            mock_strategy_check, mock_market, mock_market_book
        )
        mock_strategy_check.assert_called_with(mock_market, mock_market_book)

    def test_call_strategy_error_handling_flumine_error(self):
        mock_strategy_check = mock.MagicMock(side_effect=FlumineException)
        mock_strategy_check.__name__ = "mock_strategy_check"
        mock_market = mock.Mock()
        mock_market_book = mock.Mock()
        self.assertFalse(
            utils.call_strategy_error_handling(
                mock_strategy_check, mock_market, mock_market_book
            )
        )
        mock_strategy_check.assert_called_with(mock_market, mock_market_book)

    def test_call_strategy_error_handling_error(self):
        mock_strategy_check = mock.MagicMock(side_effect=ValueError)
        mock_strategy_check.__name__ = "mock_strategy_check"
        mock_market = mock.Mock()
        mock_market_book = mock.Mock()
        self.assertFalse(
            utils.call_strategy_error_handling(
                mock_strategy_check, mock_market, mock_market_book
            )
        )
        mock_strategy_check.assert_called_with(mock_market, mock_market_book)

    @mock.patch("flumine.utils.config")
    def test_call_strategy_error_handling_raise(self, mock_config):
        mock_config.raise_errors = True
        mock_strategy_check = mock.MagicMock(side_effect=ValueError)
        mock_strategy_check.__name__ = "mock_strategy_check"
        mock_market = mock.Mock()
        mock_market_book = mock.Mock()
        with self.assertRaises(ValueError):
            utils.call_strategy_error_handling(
                mock_strategy_check, mock_market, mock_market_book
            )

    def test_call_middleware_error_handling(self):
        mock_middlware = mock.Mock()
        mock_market = mock.Mock()
        utils.call_middleware_error_handling(mock_middlware, mock_market)
        mock_middlware.assert_called_with(mock_market)

    def test_call_middleware_error_handling_flumine_error(self):
        mock_middlware = mock.MagicMock(side_effect=FlumineException)
        mock_market = mock.Mock()
        utils.call_middleware_error_handling(mock_middlware, mock_market)
        mock_middlware.assert_called_with(mock_market)

    def test_call_middleware_error_handling_error(self):
        mock_middlware = mock.MagicMock(side_effect=ValueError)
        mock_market = mock.Mock()
        utils.call_middleware_error_handling(mock_middlware, mock_market)
        mock_middlware.assert_called_with(mock_market)

    @mock.patch("flumine.utils.config")
    def test_call_middleware_error_handling_raise(self, mock_config):
        mock_config.raise_errors = True
        mock_middlware = mock.MagicMock(side_effect=ValueError)
        mock_market = mock.Mock()
        with self.assertRaises(ValueError):
            utils.call_middleware_error_handling(mock_middlware, mock_market)

    def test_call_process_orders_error_handling(self):
        mock_strategy = mock.Mock()
        mock_market = mock.Mock()
        utils.call_process_orders_error_handling(mock_strategy, mock_market, [])
        mock_strategy.process_orders.assert_called_with(mock_market, [])

    def test_call_process_orders_error_handling_flumine_error(self):
        mock_strategy = mock.MagicMock()
        mock_strategy.process_orders.side_effect = FlumineException
        mock_market = mock.Mock()
        utils.call_process_orders_error_handling(mock_strategy, mock_market, [])
        mock_strategy.process_orders.assert_called_with(mock_market, [])

    def test_call_process_orders_error_handling_error(self):
        mock_strategy = mock.MagicMock()
        mock_strategy.process_orders.side_effect = ValueError
        mock_market = mock.Mock()
        utils.call_process_orders_error_handling(mock_strategy, mock_market, [])
        mock_strategy.process_orders.assert_called_with(mock_market, [])

    @mock.patch("flumine.utils.config")
    def test_call_process_orders_error_handling_raise(self, mock_config):
        mock_config.raise_errors = True
        mock_strategy = mock.MagicMock()
        mock_strategy.process_orders.side_effect = ValueError
        mock_market = mock.Mock()
        with self.assertRaises(ValueError):
            utils.call_process_orders_error_handling(mock_strategy, mock_market, [])
