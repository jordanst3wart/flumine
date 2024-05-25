import unittest
import datetime
from unittest import mock
from unittest.mock import call

from flumine.streams import streams, datastream
from flumine.streams.basestream import BaseStream
from flumine.streams.simulatedorderstream import CurrentOrders
from flumine.streams import orderstream
from flumine.exceptions import ListenerError


class StreamsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_flumine = mock.Mock()
        self.mock_flumine.SIMULATED = False
        self.streams = streams.Streams(self.mock_flumine)

    def test_init(self):
        self.assertEqual(self.streams.flumine, self.mock_flumine)
        self.assertEqual(self.streams._streams, [])
        self.assertEqual(self.streams._stream_id, 0)

    @mock.patch("flumine.streams.streams.Streams.add_stream")
    def test_call(self, mock_add_stream):
        mock_strategy = mock.Mock(streams=[], raw_data=False)
        self.streams(mock_strategy)
        mock_add_stream.assert_called_with(mock_strategy)

    @mock.patch("flumine.streams.streams.Streams.add_stream")
    def test_call_data_stream(self, mock_add_stream):
        mock_strategy = mock.Mock(streams=[], stream_class=datastream.DataStream)
        self.streams(mock_strategy)
        mock_add_stream.assert_called_with(mock_strategy)

    def test_call_simulated_markets_events(self):
        self.mock_flumine.SIMULATED = True
        mock_strategy = mock.Mock(
            streams=[],
            market_filter={
                "markets": ["dubs of the mad skint and british"],
                "events": ["joetry"],
            },
        )
        self.streams(mock_strategy)
        self.assertEqual(len(mock_strategy.streams), 0)

    def test_call_simulated_no_markets_no_events(self):
        self.mock_flumine.SIMULATED = True
        mock_strategy = mock.Mock(streams=[], market_filter={})
        self.streams(mock_strategy)
        self.assertEqual(len(mock_strategy.streams), 0)

    @mock.patch("flumine.streams.streams.Streams.add_order_stream")
    def test_add_client_betfair(self, mock_add_order_stream):
        mock_client = mock.Mock(order_stream=True, paper_trade=False)
        mock_client.EXCHANGE = streams.ExchangeType.BETFAIR
        self.streams.add_client(mock_client)
        mock_add_order_stream.assert_called_with(mock_client)

    @mock.patch("flumine.streams.streams.Streams.add_simulated_order_stream")
    def test_add_client_paper_trade(self, mock_add_simulated_order_stream):
        mock_client = mock.Mock(order_stream=True, paper_trade=True)
        mock_client.EXCHANGE = streams.ExchangeType.BETFAIR
        self.streams.add_client(mock_client)
        mock_add_simulated_order_stream.assert_called_with(mock_client)

    @mock.patch("flumine.streams.streams.Streams.add_order_stream")
    def test_add_client_no_order_stream(self, mock_add_order_stream):
        mock_client = mock.Mock(order_stream=False)
        mock_client.EXCHANGE = streams.ExchangeType.BETFAIR
        self.streams.add_client(mock_client)
        mock_add_order_stream.assert_not_called()

    @mock.patch("flumine.streams.streams.Streams._increment_stream_id")
    def test_add_stream_new(self, mock_increment):
        mock_strategy = mock.Mock(market_filter={}, sports_data_filter=[])
        mock_stream_class = mock.Mock()
        mock_strategy.stream_class = mock_stream_class

        self.streams.add_stream(mock_strategy)
        self.assertEqual(len(self.streams), 1)
        mock_increment.assert_called_with()
        mock_strategy.stream_class.assert_called_with(
            flumine=self.mock_flumine,
            stream_id=mock_increment(),
            market_filter=mock_strategy.market_filter,
            market_data_filter=mock_strategy.market_data_filter,
            streaming_timeout=mock_strategy.streaming_timeout,
            conflate_ms=mock_strategy.conflate_ms,
        )

    @mock.patch("flumine.streams.streams.Streams._increment_stream_id")
    def test_add_stream_none(self, mock_increment):
        mock_strategy = mock.Mock(market_filter=None, sports_data_filter=[])
        mock_stream_class = mock.Mock()
        mock_strategy.stream_class = mock_stream_class

        self.streams.add_stream(mock_strategy)
        self.assertEqual(len(self.streams), 1)
        mock_increment.assert_called_with()
        mock_strategy.stream_class.assert_called_with(
            flumine=self.mock_flumine,
            stream_id=mock_increment(),
            market_filter=mock_strategy.market_filter,
            market_data_filter=mock_strategy.market_data_filter,
            streaming_timeout=mock_strategy.streaming_timeout,
            conflate_ms=mock_strategy.conflate_ms,
        )

    @mock.patch("flumine.streams.streams.Streams._increment_stream_id")
    def test_add_stream_multi(self, mock_increment):
        mock_strategy = mock.Mock(
            market_filter=[{1: 2}, {3: 4}],
            stream_class=streams.MarketStream,
            sports_data_filter=[],
        )
        self.streams.add_stream(mock_strategy)
        self.assertEqual(len(self.streams), 2)
        mock_increment.assert_called_with()

    @mock.patch("flumine.streams.streams.Streams._increment_stream_id")
    def test_add_stream_old(self, mock_increment):
        mock_strategy = mock.Mock(
            market_data_filter=2,
            stream_class=streams.MarketStream,
            streaming_timeout=3,
            conflate_ms=4,
            market_filter={},
            sports_data_filter=[],
        )
        stream = mock.Mock(
            spec=streams.MarketStream,
            market_filter={},
            market_data_filter=2,
            streaming_timeout=3,
            conflate_ms=4,
            stream_id=1001,
        )
        self.streams._streams = [stream]

        self.streams.add_stream(mock_strategy)
        self.assertEqual(len(self.streams), 1)
        mock_increment.assert_not_called()

    @mock.patch("flumine.streams.streams.SimulatedOrderStream")
    @mock.patch("flumine.streams.streams.Streams._increment_stream_id")
    def test_add_simulated_order_stream(self, mock_increment, mock_order_stream_class):
        conflate_ms = 500
        streaming_timeout = 0.5
        mock_client = mock.Mock()
        self.streams.add_simulated_order_stream(
            mock_client, conflate_ms, streaming_timeout
        )
        self.assertEqual(len(self.streams), 1)
        mock_increment.assert_called_with()
        mock_order_stream_class.assert_called_with(
            flumine=self.mock_flumine,
            stream_id=mock_increment(),
            streaming_timeout=streaming_timeout,
            conflate_ms=conflate_ms,
            client=mock_client,
            custom=True,
        )

    @mock.patch("flumine.streams.streams.Streams._increment_stream_id")
    def test_add_custom_stream(self, mock_increment):
        mock_stream = mock.Mock()
        self.streams.add_custom_stream(mock_stream)
        mock_increment.assert_called_with()
        self.assertEqual(self.streams._streams, [mock_stream])

    def test_start(self):
        mock_stream = mock.Mock()
        self.streams._streams = [mock_stream]
        self.streams.start()
        mock_stream.start.assert_called_with()

    def test_start_simulated(self):
        self.mock_flumine.SIMULATED = True
        mock_stream = mock.Mock()
        self.streams._streams = [mock_stream]
        self.streams.start()
        mock_stream.start.assert_not_called()

    def test_stop(self):
        mock_stream = mock.Mock()
        self.streams._streams = [mock_stream]
        self.streams.stop()
        mock_stream.stop.assert_called_with()

    def test__increment_stream_id(self):
        self.assertEqual(self.streams._increment_stream_id(), 1000)

    def test_iter(self):
        for i in self.streams:
            assert i

    def test_len(self):
        self.assertEqual(len(self.streams), 0)


class TestBaseStream(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_flumine = mock.Mock()
        self.mock_client = mock.Mock()
        self.stream = BaseStream(
            self.mock_flumine,
            123,
            0.01,
            100,
            {"test": "me"},
            {"please": "now"},
            "yes",
            client=self.mock_client,
            output_queue=False,
            event_processing=True,
            event_id="123",
            operation="test",
            **{"calculate_market_tv": True},
        )

    def test_init(self):
        self.assertEqual(self.stream.flumine, self.mock_flumine)
        self.assertEqual(self.stream.stream_id, 123)
        self.assertEqual(self.stream.market_filter, {"test": "me"})
        self.assertEqual(self.stream.market_data_filter, {"please": "now"})
        self.assertEqual(self.stream.streaming_timeout, 0.01)
        self.assertEqual(self.stream.conflate_ms, 100)
        self.assertIsNone(self.stream._stream)
        self.assertEqual(self.stream._client, self.mock_client)
        self.assertEqual(self.stream.MAX_LATENCY, 0.5)
        self.assertIsNone(self.stream._output_queue)
        self.assertTrue(self.stream.event_processing)
        self.assertEqual(self.stream.event_id, "123")
        self.assertEqual(self.stream.operation, "test")
        self.assertEqual(self.stream.listener_kwargs, {"calculate_market_tv": True})

    def test_run(self):
        with self.assertRaises(NotImplementedError):
            self.stream.run()

    def test_handle_output(self):
        with self.assertRaises(NotImplementedError):
            self.stream.handle_output()

    def test_stop(self):
        mock_stream = mock.Mock()
        self.stream._stream = mock_stream
        self.stream.stop()
        mock_stream.stop.assert_called_with()

    @mock.patch("flumine.streams.basestream.BaseStream.client")
    def test_betting_client(self, mock_client):
        self.assertEqual(self.stream.betting_client, mock_client.betting_client)

    def test_client(self):
        self.stream._client = 1
        self.assertEqual(self.stream.client, 1)
        self.stream._client = None
        self.assertEqual(self.stream.client, self.mock_flumine.clients.get_default())

    def test_stream_running(self):
        self.assertFalse(self.stream.stream_running)
        mock_stream = mock.Mock(running=True)
        self.stream._stream = mock_stream
        self.assertTrue(self.stream.stream_running)


class TestMarketStream(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_flumine = mock.Mock()
        self.stream = streams.MarketStream(
            self.mock_flumine, 123, 0.01, 100, {"test": "me"}, {"please": "now"}
        )

    def test_init(self):
        self.assertEqual(self.stream.flumine, self.mock_flumine)
        self.assertEqual(self.stream.stream_id, 123)
        self.assertEqual(self.stream.market_filter, {"test": "me"})
        self.assertEqual(self.stream.market_data_filter, {"please": "now"})
        self.assertEqual(self.stream.streaming_timeout, 0.01)
        self.assertEqual(self.stream.conflate_ms, 100)
        self.assertIsNone(self.stream._stream)

    # def test_run(self):
    #     pass
    #
    # def test_handle_output(self):
    #     pass


class TestDataStream(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_flumine = mock.Mock()
        self.stream = streams.DataStream(
            self.mock_flumine, 123, 0.01, 100, {"test": "me"}, {"please": "now"}
        )

    def test_init(self):
        self.assertEqual(self.stream.flumine, self.mock_flumine)
        self.assertEqual(self.stream.stream_id, 123)
        self.assertEqual(self.stream.market_filter, {"test": "me"})
        self.assertEqual(self.stream.market_data_filter, {"please": "now"})
        self.assertEqual(self.stream.streaming_timeout, 0.01)
        self.assertEqual(self.stream.conflate_ms, 100)
        self.assertIsNone(self.stream._stream)
        self.assertEqual(self.stream.LISTENER, datastream.FlumineListener)
        self.assertEqual(
            self.stream._listener.output_queue, self.mock_flumine.handler_queue
        )

    @mock.patch("flumine.streams.marketstream.BaseStream.betting_client")
    def test_run(self, mock_betting_client):
        self.stream.run()
        mock_betting_client.streaming.create_stream.assert_called_with(
            listener=self.stream._listener, unique_id=123
        )

    @mock.patch("flumine.streams.datastream.FlumineOrderStream")
    @mock.patch("flumine.streams.datastream.FlumineMarketStream")
    def test_flumine_listener(
        self,
        mock_market_stream,
        mock_order_stream,
    ):
        listener = datastream.FlumineListener()
        self.assertEqual(
            listener._add_stream(123, "marketSubscription"), mock_market_stream()
        )
        self.assertEqual(
            listener._add_stream(123, "orderSubscription"), mock_order_stream()
        )

    def test_flumine_stream(self):
        mock_listener = mock.Mock()
        stream = datastream.FlumineStream(mock_listener, 0)
        self.assertEqual(str(stream), "FlumineStream")
        self.assertEqual(repr(stream), "<FlumineStream [0]>")

    @mock.patch("flumine.streams.datastream.RawDataEvent")
    def test_flumine_stream_on_process(self, raw_data_event):
        mock_listener = mock.Mock()
        stream = datastream.FlumineStream(mock_listener, 0)
        stream.on_process([1, 2, 3])
        raw_data_event.assert_called_with([1, 2, 3])
        mock_listener.output_queue.put.assert_called_with(raw_data_event.return_value)

    @mock.patch("flumine.streams.datastream.FlumineMarketStream.on_process")
    def test_flumine_market_stream(self, mock_on_process):
        mock_listener = mock.Mock(stream_unique_id=0)
        stream = datastream.FlumineMarketStream(mock_listener, 0)
        stream._clk = "AAA"
        market_books = [{"id": "1.123"}, {"id": "1.456"}, {"id": "1.123"}]
        stream._process(market_books, 123)

        self.assertEqual(len(stream._caches), 2)
        self.assertEqual(stream._updates_processed, 3)
        mock_on_process.assert_called_with(
            [mock_listener.stream_unique_id, "AAA", 123, market_books]
        )

    @mock.patch("flumine.streams.datastream.FlumineMarketStream.on_process")
    def test_flumine_market_stream_market_closed(self, mock_on_process):
        mock_listener = mock.Mock(stream_unique_id=0)
        stream = datastream.FlumineMarketStream(mock_listener, 0)
        stream._caches = {"1.123": object}
        stream._clk = "AAA"
        market_books = [{"id": "1.123", "marketDefinition": {"status": "CLOSED"}}]
        stream._process(market_books, 123)

        self.assertEqual(stream._lookup, "mc")
        self.assertEqual(len(stream._caches), 0)
        self.assertEqual(stream._updates_processed, 1)
        mock_on_process.assert_called_with(
            [mock_listener.stream_unique_id, "AAA", 123, market_books]
        )

    @mock.patch("flumine.streams.datastream.FlumineOrderStream.on_process")
    def test_flumine_order_stream(self, mock_on_process):
        mock_listener = mock.Mock(stream_unique_id=0)
        stream = datastream.FlumineOrderStream(mock_listener, 0)
        stream._clk = "AAA"
        order_updates = [{"id": "1.123"}, {"id": "1.456"}, {"id": "1.123"}]
        stream._process(order_updates, 123)

        self.assertEqual(stream._lookup, "oc")
        self.assertEqual(len(stream._caches), 2)
        self.assertEqual(stream._updates_processed, 3)
        mock_on_process.assert_called_with(
            [mock_listener.stream_unique_id, "AAA", 123, order_updates]
        )

class TestOrderStream(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_flumine = mock.Mock()
        self.stream = streams.OrderStream(self.mock_flumine, 123, 0.01, 100)

    def test_init(self):
        self.assertEqual(self.stream.flumine, self.mock_flumine)
        self.assertEqual(self.stream.stream_id, 123)
        self.assertIsNone(self.stream.market_filter)
        self.assertIsNone(self.stream.market_data_filter)
        self.assertEqual(self.stream.streaming_timeout, 0.01)
        self.assertEqual(self.stream.conflate_ms, 100)
        self.assertIsNone(self.stream._stream)
        self.assertEqual(orderstream.START_DELAY, 2)
        self.assertEqual(orderstream.SNAP_DELTA, 5)

    # def test_run(self):
    #     pass
    #
    # def test_handle_output(self):
    #     pass


class TestSimulatedOrderStream(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_flumine = mock.Mock()
        self.stream = streams.SimulatedOrderStream(self.mock_flumine, 123, 0.01, 100)

    def test_init(self):
        self.assertEqual(self.stream.flumine, self.mock_flumine)
        self.assertEqual(self.stream.stream_id, 123)
        self.assertIsNone(self.stream.market_filter)
        self.assertIsNone(self.stream.market_data_filter)
        self.assertEqual(self.stream.streaming_timeout, 0.01)
        self.assertEqual(self.stream.conflate_ms, 100)
        self.assertIsNone(self.stream._stream)

    def test_current_orders(self):
        mock_client = mock.Mock()
        current_orders = CurrentOrders([1], mock_client)
        self.assertEqual(current_orders.orders, [1])
        self.assertFalse(current_orders.more_available)
        self.assertEqual(current_orders.client, mock_client)

    # def test_run(self):
    #     pass

    def test__get_current_orders(self):
        mock_market = mock.Mock(closed=False)
        order_one = mock.Mock(client=self.stream.client)
        mock_market.blotter.client_orders.return_value = [order_one]
        self.stream.flumine.markets = [mock_market, mock.Mock(closed=True)]
        self.assertEqual(self.stream._get_current_orders(), [order_one])
