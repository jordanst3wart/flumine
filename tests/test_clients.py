import unittest
from unittest import mock
from betfairlightweight.exceptions import BetfairError

from flumine.clients.baseclient import BaseClient
from flumine.clients.betfairclient import BetfairClient
from flumine.clients.clients import ExchangeType, Clients
from flumine import exceptions


class ClientsTest(unittest.TestCase):
    def setUp(self):
        self.clients = Clients()

    def test_init(self):
        self.assertEqual(self.clients._clients, [])
        self.assertEqual(
            self.clients._exchange_clients,
            {exchange: {} for exchange in ExchangeType},
        )

    def test_add_client(self):
        mock_client = mock.Mock(EXCHANGE=ExchangeType.BETFAIR, username="test")
        self.clients._clients = [mock_client]
        with self.assertRaises(exceptions.ClientError):
            self.clients.add_client(mock_client)

        with self.assertRaises(exceptions.ClientError):
            self.clients.add_client(mock.Mock(EXCHANGE="test", username="test"))

        self.clients._clients = []
        self.clients._exchange_clients[ExchangeType.BETFAIR][
            mock_client.username
        ] = mock_client
        with self.assertRaises(exceptions.ClientError):
            self.clients.add_client(mock_client)

        self.clients._clients = []
        self.clients._exchange_clients = {exchange: {} for exchange in ExchangeType}
        mock_client.EXCHANGE = ExchangeType.BETFAIR
        self.assertEqual(self.clients.add_client(mock_client), mock_client)
        self.assertEqual(self.clients._clients, [mock_client])
        self.assertEqual(
            self.clients._exchange_clients[mock_client.EXCHANGE],
            {mock_client.username: mock_client},
        )

    def test_get_default(self):
        self.clients._clients.append("howlandthehum")
        self.assertEqual(self.clients.get_default(), "howlandthehum")

    def test_get_betfair_default(self):
        mock_client_one = mock.Mock(EXCHANGE=ExchangeType.SIMULATED)
        mock_client_two = mock.Mock(EXCHANGE=ExchangeType.BETFAIR)
        self.clients._clients.append(mock_client_one)
        self.clients._clients.append(mock_client_two)
        self.assertEqual(self.clients.get_betfair_default(), mock_client_two)

    def test_login(self):
        mock_client = unittest.mock.Mock()
        self.clients._clients = [mock_client]
        self.clients.login()
        mock_client.login.assert_called_with()

    def test_keep_alive(self):
        mock_client = unittest.mock.Mock()
        self.clients._clients = [mock_client]
        self.clients.keep_alive()
        mock_client.keep_alive.assert_called_with()

    def test_logout(self):
        mock_client = unittest.mock.Mock()
        self.clients._clients = [mock_client]
        self.clients.logout()
        mock_client.logout.assert_called_with()

    def test_update_account_details(self):
        mock_client = unittest.mock.Mock()
        self.clients._clients = [mock_client]
        self.clients.update_account_details()
        mock_client.update_account_details.assert_called_with()

    def test_simulated(self):
        self.assertFalse(self.clients.simulated)
        self.clients._clients.append(mock.Mock(paper_trade=True))
        self.assertTrue(self.clients.simulated)

    def test_info(self):
        self.assertEqual(
            self.clients.info, {exchange.value: {} for exchange in ExchangeType}
        )
        mock_client = mock.Mock()
        self.clients._exchange_clients[ExchangeType.BETFAIR]["james"] = mock_client
        self.assertEqual(
            self.clients.info,
            {
                ExchangeType.BETFAIR.value: {"james": mock_client.info},
                ExchangeType.SIMULATED.value: {},
            },
        )

    def test_iter(self):
        for i in self.clients:
            assert i

    def test_len(self):
        self.assertEqual(len(self.clients), 0)


class BaseClientTest(unittest.TestCase):
    def setUp(self):
        self.mock_betting_client = mock.Mock(lightweight=False)
        self.base_client = BaseClient(
            self.mock_betting_client, interactive_login=True, username="test"
        )

    def test_init(self):
        self.assertEqual(self.base_client.betting_client, self.mock_betting_client)
        self.assertTrue(self.base_client.interactive_login)
        self.assertEqual(self.base_client._username, "test")
        self.assertIsNone(self.base_client.account_details)
        self.assertIsNone(self.base_client.account_funds)
        self.assertEqual(self.base_client.commission_paid, 0)
        self.assertEqual(self.base_client.trading_controls, [])
        self.assertTrue(self.base_client.order_stream)
        self.assertFalse(self.base_client.paper_trade)
        self.assertFalse(self.base_client.simulated_full_match)

    def test_login(self):
        with self.assertRaises(NotImplementedError):
            assert self.base_client.login()

    def test_keep_alive(self):
        with self.assertRaises(NotImplementedError):
            assert self.base_client.keep_alive()

    def test_logout(self):
        with self.assertRaises(NotImplementedError):
            assert self.base_client.logout()

    def test_update_account_details(self):
        with self.assertRaises(NotImplementedError):
            assert self.base_client.update_account_details()

    def test_min_bet_payout(self):
        with self.assertRaises(NotImplementedError):
            assert self.base_client.min_bet_payout

    def test_min_bsp_liability(self):
        with self.assertRaises(NotImplementedError):
            assert self.base_client.min_bsp_liability

    def test_username(self):
        self.assertEqual(
            self.base_client.username, self.base_client.betting_client.username
        )
        self.base_client.betting_client = None
        self.assertEqual(self.base_client.username, self.base_client._username)

    def test_info(self):
        self.assertTrue(self.base_client.info)


class BetfairClientTest(unittest.TestCase):
    def setUp(self):
        self.mock_betting_client = mock.Mock(lightweight=False)
        self.betfair_client = BetfairClient(self.mock_betting_client)

    def test_login(self):
        self.betfair_client.login()
        self.mock_betting_client.login.assert_called_with()

    def test_login_no_certs(self):
        self.betfair_client.interactive_login = True
        self.betfair_client.login()
        self.mock_betting_client.login_interactive.assert_called_with()

    def test_login_error(self):
        self.betfair_client.betting_client.login.side_effect = BetfairError
        self.assertIsNone(self.betfair_client.login())
        self.mock_betting_client.login.assert_called_with()

    def test_keep_alive(self):
        self.mock_betting_client.session_expired = True
        self.betfair_client.keep_alive()
        self.mock_betting_client.keep_alive.assert_called_with()

    def test_keep_alive_error(self):
        self.betfair_client.betting_client.keep_alive.side_effect = BetfairError
        self.assertIsNone(self.betfair_client.keep_alive())
        self.mock_betting_client.keep_alive.assert_called_with()

    def test_logout(self):
        self.betfair_client.logout()
        self.mock_betting_client.logout.assert_called_with()

    def test_logout_error(self):
        self.betfair_client.betting_client.logout.side_effect = BetfairError
        self.assertIsNone(self.betfair_client.logout())
        self.mock_betting_client.logout.assert_called_with()

    @mock.patch("flumine.clients.betfairclient.BetfairClient._get_account_details")
    @mock.patch("flumine.clients.betfairclient.BetfairClient._get_account_funds")
    def test_update_account_details(self, mock_get_funds, mock_get_details):
        self.betfair_client.update_account_details()
        mock_get_funds.assert_called_with()
        mock_get_details.assert_called_with()
        self.assertEqual(self.betfair_client.account_details, mock_get_details())
        self.assertEqual(self.betfair_client.account_funds, mock_get_funds())

    def test__get_account_details(self):
        self.betfair_client._get_account_details()
        self.mock_betting_client.account.get_account_details.assert_called_with()

    def test__get_account_details_error(self):
        self.betfair_client.betting_client.account.get_account_details.side_effect = (
            BetfairError
        )
        self.assertIsNone(self.betfair_client._get_account_details())
        self.mock_betting_client.account.get_account_details.assert_called_with()

    def test__get_account_funds(self):
        self.betfair_client._get_account_funds()
        self.mock_betting_client.account.get_account_funds.assert_called_with()

    def test__get_account_funds_error(self):
        self.betfair_client.betting_client.account.get_account_funds.side_effect = (
            BetfairError
        )
        self.assertIsNone(self.betfair_client._get_account_funds())
        self.mock_betting_client.account.get_account_funds.assert_called_with()
