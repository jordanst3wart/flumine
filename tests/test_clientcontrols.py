import unittest

from unittest import mock

from flumine.controls.basecontrol import BaseControl
from flumine.exceptions import ControlError
from flumine.order.orderpackage import OrderPackageType


class TestBaseControl(unittest.TestCase):
    def setUp(self):
        self.mock_flumine = mock.Mock()
        self.mock_client = mock.Mock()
        self.control = BaseControl(self.mock_flumine, self.mock_client)

    def test_init(self):
        self.assertEqual(self.control.flumine, self.mock_flumine)
        self.assertIsNone(self.control.NAME)

    def test_validate(self):
        with self.assertRaises(NotImplementedError):
            self.control._validate(None, None)

    def test_on_error(self):
        order = mock.Mock()
        order.info = {"hello": "world"}
        with self.assertRaises(ControlError):
            self.control._on_error(order, "test")
        order.violation.assert_called_with("Order has violated: None Error: test")

