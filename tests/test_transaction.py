import unittest
from unittest import mock
from unittest.mock import call

from flumine.execution.transaction import Transaction, OrderPackageType
from flumine.exceptions import ControlError, OrderError


class TransactionTest(unittest.TestCase):
    def setUp(self) -> None:
        mock_blotter = {}
        self.mock_market = mock.Mock(blotter=mock_blotter)
        self.mock_client = mock.Mock(trading_controls=[])
        self.transaction = Transaction(self.mock_market, 1, False, self.mock_client)

    def test_init(self):
        self.assertEqual(self.transaction.market, self.mock_market)
        self.assertEqual(self.transaction._client, self.mock_client)
        self.assertEqual(self.transaction._id, 1)
        self.assertFalse(self.transaction._async_place_orders)

    @mock.patch(
        "flumine.execution.transaction.Transaction._validate_controls",
        return_value=False,
    )
    def test_place_order_violation(self, mock__validate_controls):
        mock_order = mock.Mock()
        self.assertFalse(self.transaction.place_order(mock_order))
        mock__validate_controls.assert_called_with(mock_order, OrderPackageType.PLACE)
        self.transaction._pending_place = []

    @mock.patch(
        "flumine.execution.transaction.Transaction._validate_controls",
        return_value=False,
    )
    def test_force_place_order(self, mock__validate_controls):
        self.transaction.market.blotter = mock.MagicMock()
        self.transaction.market.blotter.has_trade.return_value = False
        mock_order = mock.Mock(id="123", lookup=(1, 2, 3))
        self.assertTrue(self.transaction.place_order(mock_order, force=True))
        mock__validate_controls.assert_not_called()
        self.transaction._pending_place = [(mock_order, None)]

    @mock.patch(
        "flumine.execution.transaction.Transaction._validate_controls",
        return_value=True,
    )
    def test_cancel_order(self, mock__validate_controls):
        mock_order = mock.Mock(client=self.mock_client)
        self.assertTrue(self.transaction.cancel_order(mock_order, 0.01))
        mock_order.cancel.assert_called_with(0.01)
        mock__validate_controls.assert_called_with(mock_order, OrderPackageType.CANCEL)
        self.transaction._pending_cancel = [(mock_order, None)]

    @mock.patch(
        "flumine.execution.transaction.Transaction._validate_controls",
        return_value=False,
    )
    def test_cancel_order_violation(self, mock__validate_controls):
        mock_order = mock.Mock(client=self.mock_client)
        self.assertFalse(self.transaction.cancel_order(mock_order))
        mock__validate_controls.assert_called_with(mock_order, OrderPackageType.CANCEL)
        self.transaction._pending_cancel = []

    @mock.patch(
        "flumine.execution.transaction.Transaction._validate_controls",
        return_value=False,
    )
    def test_force_cancel_order(self, mock__validate_controls):
        mock_order = mock.Mock(client=self.mock_client)
        self.assertTrue(self.transaction.cancel_order(mock_order, 0.01, force=True))
        mock_order.cancel.assert_called_with(0.01)
        mock__validate_controls.assert_not_called()
        self.transaction._pending_cancel = [(mock_order, None)]

    @mock.patch(
        "flumine.execution.transaction.Transaction._validate_controls",
        return_value=True,
    )
    def test_update_order(self, mock__validate_controls):
        mock_order = mock.Mock(client=self.mock_client)
        self.assertTrue(self.transaction.update_order(mock_order, "PERSIST"))
        mock_order.update.assert_called_with("PERSIST")
        mock__validate_controls.assert_called_with(mock_order, OrderPackageType.UPDATE)
        self.transaction._pending_update = [(mock_order, None)]

    @mock.patch(
        "flumine.execution.transaction.Transaction._validate_controls",
        return_value=False,
    )
    def test_update_order_violation(self, mock__validate_controls):
        mock_order = mock.Mock(client=self.mock_client)
        self.assertFalse(self.transaction.update_order(mock_order, "test"))
        mock__validate_controls.assert_called_with(mock_order, OrderPackageType.UPDATE)
        self.transaction._pending_update = []

    @mock.patch(
        "flumine.execution.transaction.Transaction._validate_controls",
        return_value=False,
    )
    def test_force_update_order(self, mock__validate_controls):
        mock_order = mock.Mock(client=self.mock_client)
        self.assertTrue(
            self.transaction.update_order(mock_order, "PERSIST", force=True)
        )
        mock_order.update.assert_called_with("PERSIST")
        mock__validate_controls.assert_not_called()
        self.transaction._pending_update = [(mock_order, None)]

    @mock.patch(
        "flumine.execution.transaction.Transaction._validate_controls",
        return_value=True,
    )
    def test_replace_order(self, mock__validate_controls):
        mock_order = mock.Mock(client=self.mock_client)
        self.assertTrue(self.transaction.replace_order(mock_order, 1.01, 321))
        mock_order.replace.assert_called_with(1.01)
        mock__validate_controls.assert_called_with(mock_order, OrderPackageType.REPLACE)
        self.transaction._pending_replace = [(mock_order, None)]

    @mock.patch(
        "flumine.execution.transaction.Transaction._validate_controls",
        return_value=False,
    )
    def test_replace_order_violation(self, mock__validate_controls):
        mock_order = mock.Mock(client=self.mock_client)
        self.assertFalse(self.transaction.replace_order(mock_order, 2.02))
        mock__validate_controls.assert_called_with(mock_order, OrderPackageType.REPLACE)
        self.transaction._pending_replace = []

    @mock.patch(
        "flumine.execution.transaction.Transaction._validate_controls",
        return_value=False,
    )
    def test_force_replace_order(self, mock__validate_controls):
        mock_order = mock.Mock(client=self.mock_client)
        self.assertTrue(
            self.transaction.replace_order(mock_order, 1.01, 321, force=True)
        )
        mock_order.replace.assert_called_with(1.01)
        mock__validate_controls.assert_not_called()
        self.transaction._pending_replace = [(mock_order, None)]

    def test__validate_controls(self):
        mock_trading_control = mock.Mock()
        mock_client_control = mock.Mock()
        self.transaction.market.flumine.trading_controls = [mock_trading_control]
        self.transaction._client.trading_controls = [mock_client_control]
        mock_order = mock.Mock()
        mock_package_type = mock.Mock()
        self.assertTrue(
            self.transaction._validate_controls(mock_order, mock_package_type)
        )
        mock_trading_control.assert_called_with(mock_order, mock_package_type)
        mock_client_control.assert_called_with(mock_order, mock_package_type)

    def test__validate_controls_violation(self):
        mock_trading_control = mock.Mock()
        mock_trading_control.side_effect = ControlError("test")
        mock_client_control = mock.Mock()
        self.transaction.market.flumine.trading_controls = [mock_trading_control]
        self.transaction._client.trading_controls = [mock_client_control]
        mock_order = mock.Mock()
        mock_package_type = mock.Mock()
        self.assertFalse(
            self.transaction._validate_controls(mock_order, mock_package_type)
        )
        mock_trading_control.assert_called_with(mock_order, mock_package_type)
        mock_client_control.assert_not_called()
