import unittest
from unittest import mock

from flumine import Flumine, worker
from flumine.events import events
from flumine.clients import ExchangeType


class FlumineTest(unittest.TestCase):
    def setUp(self):
        self.mock_client = mock.Mock(EXCHANGE=ExchangeType.SIMULATED)
        self.flumine = Flumine(self.mock_client)

    def test_str(self):
        assert str(self.flumine) == "<Flumine>"

    def test_repr(self):
        assert repr(self.flumine) == "<Flumine>"

    # def test_trade(self):
        # self.flumine.add_strategy()
        # self.flumine.run()

