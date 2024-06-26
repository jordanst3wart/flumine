import unittest
from unittest import mock

from flumine.baseflumine import (
    BaseFlumine,
    Market,
)
from flumine.clients.exchangetype import ExchangeType

from flumine.exceptions import ClientError


class BaseFlumineTest(unittest.TestCase):
    def setUp(self):
        self.mock_client = mock.Mock(EXCHANGE=ExchangeType.BETFAIR, paper_trade=False)
        self.base_flumine = BaseFlumine(self.mock_client)

    def test_run(self):
        with self.assertRaises(NotImplementedError):
            self.base_flumine.run()

    def test_add_client(self):
        mock_clients = mock.Mock()
        self.base_flumine.clients = mock_clients
        mock_streams = mock.Mock()
        self.base_flumine.streams = mock_streams
        mock_client = mock.Mock()
        self.base_flumine.add_client(mock_client)
        mock_clients.add_client.assert_called_with(mock_client)
        mock_streams.add_client.assert_called_with(mock_client)

    def test_add_worker(self):
        mock_worker = mock.Mock()
        self.base_flumine.add_worker(mock_worker)
        self.assertEqual(len(self.base_flumine._workers), 1)

    def test__add_default_workers(self):
        self.base_flumine._add_default_workers()
        self.assertEqual(len(self.base_flumine._workers), 0)

    def test__process_market_books(self):
        self.base_flumine.streams = mock.Mock()
        mock_strategy = mock.Mock(stream_ids=[1])
        self.base_flumine.add_strategy(mock_strategy)
        mock_market_book = mock.Mock(
            publish_time_epoch=123, market_id="1.123", streaming_unique_id=1, runners=[]
        )
        mock_event = mock.Mock(event=[mock_market_book])
        for call_count in range(1, 5):
            # process_new_market must be called only once, the first time
            with self.subTest(call_count=call_count):
                self.base_flumine._process_market_books(mock_event)
                mock_strategy.process_new_market.assert_called_once()
                self.assertEqual(
                    mock_strategy.process_market_book.call_count, call_count
                )
        market, market_book = mock_strategy.process_new_market.call_args[0]
        self.assertIs(market_book, mock_market_book)
        self.assertIsInstance(market, Market)
        self.assertIs(market.market_book, mock_market_book)

    def test__process_market_stream_not_subscribed(self):
        """
        Market book should only be called with objects from the streams
        it is subscribed to.
        """
        self.base_flumine.streams = mock.Mock()
        mock_strategy = mock.Mock(stream_ids=[1, 2])
        self.base_flumine.add_strategy(mock_strategy)
        mock_market_book = mock.Mock(
            publish_time_epoch=123, market_id="1.123", streaming_unique_id=5, runners=[]
        )
        mock_event = mock.Mock(event=[mock_market_book])
        self.base_flumine._process_market_books(mock_event)
        mock_strategy.process_new_market.assert_not_called()
        mock_strategy.check_market_book.assert_not_called()
        mock_strategy.process_market_book.assert_not_called()

    def test__process_market_books_check_market_books(self):
        """
        Tests base_flumine._process_market_books() with different return values
        of strategy.check_market_book().
        """
        self.base_flumine.streams = mock.Mock()
        mock_strategy = mock.Mock(stream_ids=[1])
        check_pattern = (False, True, True, False, True)
        mock_strategy.check_market_book.side_effect = check_pattern
        self.base_flumine.add_strategy(mock_strategy)
        mock_market_book = mock.Mock(
            publish_time_epoch=123, market_id="1.123", streaming_unique_id=1, runners=[]
        )
        mock_event = mock.Mock(event=[mock_market_book])
        process_call_count = 0
        for check_call_count, check_market_book_retval in enumerate(check_pattern, 1):
            self.base_flumine._process_market_books(mock_event)
            process_call_count += check_market_book_retval  # True == 1, False == 0
            self.assertEqual(
                mock_strategy.check_market_book.call_count, check_call_count
            )
            self.assertEqual(
                mock_strategy.process_market_book.call_count, process_call_count
            )

    @mock.patch("flumine.baseflumine.Market")
    def test__add_market(self, mock_market):
        mock_market_book = mock.Mock()
        self.assertEqual(
            self.base_flumine._add_market("1.234", mock_market_book), mock_market()
        )
        self.assertEqual(len(self.base_flumine.markets._markets), 1)

    @mock.patch("flumine.baseflumine.BaseFlumine.info")
    def test__remove_market(self, _):
        mock_strategy = mock.Mock()
        self.base_flumine.strategies = [mock_strategy]
        mock_markets = mock.Mock()
        self.base_flumine.markets = mock_markets
        mock_market = mock.Mock()
        self.base_flumine._remove_market(mock_market)
        mock_markets.remove_market.assert_called_with(mock_market.market_id)
        mock_strategy.remove_market.assert_called_with(mock_market.market_id)

    @mock.patch("flumine.baseflumine.BaseFlumine.info")
    def test__remove_market_no_clear(self, _):
        mock_strategy = mock.Mock()
        self.base_flumine.strategies = [mock_strategy]
        mock_markets = mock.Mock()
        self.base_flumine.markets = mock_markets
        mock_market = mock.Mock()
        self.base_flumine._remove_market(mock_market, clear=False)
        mock_markets.remove_market.assert_not_called()
        mock_strategy.remove_market.assert_called_with(mock_market.market_id)

    @mock.patch("flumine.baseflumine.events")
    def test__process_market_catalogues_missing_book(self, mock_events):
        # Matches by stream id
        mock_strategy_1 = mock.Mock(stream_ids=[1, 2])
        mock_strategy_1.market_cached.return_value = True
        # Does not match
        mock_strategy_2 = mock.Mock(stream_ids=[3, 4])
        mock_strategy_2.market_cached.return_value = False
        # Matches by market id being cached
        mock_strategy_3 = mock.Mock(stream_ids=[5, 6])
        mock_strategy_3.market_cached.return_value = True

        mock_market = mock.Mock(
            market_catalogue=None, market_id="1.23", market_book=None
        )

        self.base_flumine.strategies = [
            mock_strategy_1,
            mock_strategy_2,
            mock_strategy_3,
        ]
        self.base_flumine.markets = mock.Mock(markets={"1.23": mock_market})

        mock_market_catalogue = mock.Mock(market_id="1.23")
        mock_event = mock.Mock(event=[mock_market_catalogue])
        self.base_flumine._process_market_catalogues(mock_event)

        self.assertEqual(mock_market.market_catalogue, mock_market_catalogue)
        self.assertFalse(mock_market.update_market_catalogue)

    @mock.patch("flumine.baseflumine.process_current_orders")
    def test__process_current_orders(self, mock_process_current_orders):
        mock_order = mock.Mock(complete=True)
        mock_market = mock.Mock(closed=False)
        self.base_flumine.markets = [mock_market]
        mock_strategy = mock.Mock()
        self.base_flumine.strategies = [mock_strategy]
        mock_current_orders = mock.Mock(orders=[mock_order])
        mock_event = mock.Mock(event=[mock_current_orders])
        self.base_flumine._process_current_orders(mock_event)
        mock_process_current_orders.assert_called_with(
            self.base_flumine.markets,
            mock_event,
        )

    @mock.patch("flumine.baseflumine.process_current_orders")
    def test__process_current_orders_no_event(self, mock_process_current_orders):
        mock_event = mock.Mock(event=[])
        self.base_flumine._process_current_orders(mock_event)
        mock_process_current_orders.assert_not_called()

    @mock.patch("flumine.baseflumine.BaseFlumine.info")
    def test__process_close_market(self, mock_info):
        mock_strategy = mock.Mock()
        mock_strategy.stream_ids = [1, 2, 3]
        self.base_flumine.strategies = [mock_strategy]
        mock_market = mock.Mock(closed=False, elapsed_seconds_closed=None)
        self.base_flumine.markets._markets = {"1.23": mock_market}
        mock_event = mock.Mock()
        mock_market_book = mock.Mock(market_id="1.23", streaming_unique_id=2)
        mock_event.event = mock_market_book
        self.base_flumine._process_close_market(mock_event)
        mock_market.close_market.assert_called_with()
        mock_market.blotter.process_closed_market.assert_called_with(
            mock_market, mock_market_book
        )
        mock_strategy.process_closed_market.assert_called_with(
            mock_market, mock_market_book
        )
        mock_market.assert_called_with(mock_market_book)

    @mock.patch("flumine.baseflumine.BaseFlumine.info")
    def test__process_close_market_closed(self, mock_info):
        mock_strategy = mock.Mock()
        mock_strategy.stream_ids = [1, 2, 3]
        self.base_flumine.strategies = [mock_strategy]
        mock_market = mock.Mock(
            market_id="1.23", event_id="1", closed=False, elapsed_seconds_closed=None
        )
        mock_market.market_book.streaming_unique_id = 2
        markets = [
            mock_market,
            mock.Mock(
                market_id="4.56", event_id="1", closed=True, elapsed_seconds_closed=25
            ),
            mock.Mock(
                market_id="7.89", event_id="1", closed=True, elapsed_seconds_closed=3601
            ),
            mock.Mock(
                market_id="1.01",
                event_id="2",
                closed=False,
                elapsed_seconds_closed=3601,
            ),
        ]
        for market in markets:
            self.base_flumine.markets.add_market(market.market_id, market)
        mock_market_book = mock.Mock(market_id="1.23")
        mock_event = mock.Mock(event=mock_market_book)
        self.base_flumine._process_close_market(mock_event)

        self.assertEqual(len(self.base_flumine.markets._markets), 3)
        self.assertEqual(len(self.base_flumine.markets.events), 2)

    @mock.patch("flumine.baseflumine.BaseFlumine._process_cleared_markets")
    @mock.patch("flumine.baseflumine.events")
    @mock.patch("flumine.baseflumine.BaseFlumine._process_cleared_orders")
    @mock.patch("flumine.baseflumine.BaseFlumine.info")
    def test__process_close_market_closed_paper(
        self,
        mock_info,
        mock__process_cleared_orders,
        mock_events,
        mock__process_cleared_markets,
    ):
        self.mock_client.paper_trade = True
        mock_strategy = mock.Mock()
        mock_strategy.stream_ids = [1, 2, 3]
        self.base_flumine.strategies = [mock_strategy]
        mock_market = mock.Mock(closed=False, elapsed_seconds_closed=None)
        mock_market.market_book.streaming_unique_id = 2
        mock_market.cleared.return_value = {}
        self.base_flumine.markets._markets = {
            "1.23": mock_market,
            "4.56": mock.Mock(market_id="4.56", closed=True, elapsed_seconds_closed=25),
            "7.89": mock.Mock(
                market_id="7.89", closed=True, elapsed_seconds_closed=3601
            ),
            "1.01": mock.Mock(
                market_id="1.01", closed=False, elapsed_seconds_closed=3601
            ),
        }
        mock_event = mock.Mock()
        mock_market_book = mock.Mock(market_id="1.23")
        mock_event.event = mock_market_book
        self.base_flumine._process_close_market(mock_event)
        self.assertEqual(len(self.base_flumine.markets._markets), 4)
        mock__process_cleared_orders.assert_called_with(
            mock_events.ClearedOrdersEvent()
        )
        mock_market.cleared.assert_called_with(self.mock_client)
        mock__process_cleared_markets.assert_called_with(
            mock_events.ClearedMarketsEvent()
        )

    @mock.patch("flumine.baseflumine.events")
    @mock.patch("flumine.baseflumine.BaseFlumine.info")
    def test__process_cleared_orders(self, mock_info, mock_events):
        mock_market = mock.Mock()
        mock_market.blotter.process_cleared_orders.return_value = []
        mock_markets = mock.Mock()
        mock_markets.markets = {"1.23": mock_market}
        self.base_flumine.markets = mock_markets
        mock_event = mock.Mock()
        mock_event.event.market_id = "1.23"
        mock_event.event.orders = []
        self.base_flumine._process_cleared_orders(mock_event)
        mock_market.blotter.process_cleared_orders.assert_called_with(mock_event.event)

    def test__process_cleared_markets(self):
        mock_event = mock.Mock()
        mock_event.event.orders = []
        self.base_flumine._process_cleared_markets(mock_event)

    def test__process_end_flumine(self):
        mock_strategies = mock.Mock()
        self.base_flumine.strategies = mock_strategies
        self.base_flumine._process_end_flumine()
        mock_strategies.finish.assert_called_with(self.base_flumine)

    def test_info(self):
        self.assertTrue(self.base_flumine.info)

    def test_enter_no_clients(self):
        self.base_flumine.clients._clients = []
        with self.assertRaises(ClientError):
            with self.base_flumine:
                pass

    @mock.patch("flumine.baseflumine.BaseFlumine._process_end_flumine")
    def test_enter_exit(self, mock__process_end_flumine):
        with self.base_flumine:
            self.assertTrue(self.base_flumine._running)
            self.mock_client.login.assert_called_with()

        self.assertFalse(self.base_flumine._running)
        mock__process_end_flumine.assert_called_with()
        self.mock_client.logout.assert_called_with()
