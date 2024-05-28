import unittest
import datetime
from unittest import mock

from flumine.controls.clientcontrols import (
    BaseControl,
    OrderPackageType,
)
from flumine.exceptions import ControlError


class TestBaseControl(unittest.TestCase):
    def setUp(self):
        self.mock_flumine = mock.Mock()
        self.mock_client = mock.Mock()
        self.control = BaseControl(self.mock_flumine, self.mock_client)

    def test_init(self):
        self.assertEqual(self.control.flumine, self.mock_flumine)
        self.assertIsNone(self.control.NAME)

    @mock.patch("flumine.controls.BaseControl._validate")
    def test_call(self, mock_validate):
        order = mock.Mock()
        self.control(order, OrderPackageType.PLACE)
        mock_validate.assert_called_with(order, OrderPackageType.PLACE)

    def test_validate(self):
        with self.assertRaises(NotImplementedError):
            self.control._validate(None, None)

    def test_on_error(self):
        order = mock.Mock()
        order.info = {"hello": "world"}
        with self.assertRaises(ControlError):
            self.control._on_error(order, "test")
        order.violation.assert_called_with("Order has violated: None Error: test")
