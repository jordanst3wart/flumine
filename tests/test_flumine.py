import unittest
from unittest import mock

from flumine import Flumine, worker
from flumine.events import events
from flumine.clients import ExchangeType


class FlumineTest(unittest.TestCase):
    def setUp(self):
        self.mock_client = mock.Mock(EXCHANGE=ExchangeType.SIMULATED)
        self.flumine = Flumine(self.mock_client)

    @mock.patch("flumine.worker.BackgroundWorker")
    @mock.patch("flumine.Flumine.add_worker")
    def test__add_default_workers(self, mock_add_worker, mock_worker):
        mock_client_one = mock.Mock(market_recording_mode=False)
        mock_client_one.betting_client.session_timeout = 1200
        mock_client_two = mock.Mock(market_recording_mode=True)
        mock_client_two.betting_client.session_timeout = 600
        self.flumine.clients = [
            mock_client_one,
            mock_client_two,
        ]
        self.flumine._add_default_workers()
        self.assertEqual(
            mock_worker.call_args_list,
            [
                mock.call(self.flumine, function=worker.keep_alive, interval=300),
                mock.call(
                    self.flumine,
                    function=worker.poll_market_catalogue,
                    interval=120,
                    start_delay=10,
                ),
                mock.call(
                    self.flumine,
                    function=worker.poll_account_balance,
                    interval=120,
                    start_delay=10,
                ),
                mock.call(
                    self.flumine,
                    function=worker.poll_market_closure,
                    interval=60,
                    start_delay=10,
                ),
            ],
        )

    @mock.patch("flumine.worker.BackgroundWorker")
    @mock.patch("flumine.Flumine.add_worker")
    def test__add_default_workers_market_record(self, mock_add_worker, mock_worker):
        mock_client_one = mock.Mock(market_recording_mode=True)
        mock_client_one.betting_client.session_timeout = 1200
        mock_client_two = mock.Mock(market_recording_mode=True)
        mock_client_two.betting_client.session_timeout = 600
        self.flumine.clients = [
            mock_client_one,
            mock_client_two,
        ]
        self.flumine._add_default_workers()
        self.assertEqual(
            mock_worker.call_args_list,
            [
                mock.call(self.flumine, function=worker.keep_alive, interval=300),
                mock.call(
                    self.flumine,
                    function=worker.poll_market_catalogue,
                    interval=120,
                    start_delay=10,
                ),
            ],
        )

    def test_str(self):
        assert str(self.flumine) == "<Flumine>"

    def test_repr(self):
        assert repr(self.flumine) == "<Flumine>"
