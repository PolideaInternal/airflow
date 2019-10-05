import unittest
import pytest
from parameterized import parameterized


class TestDupa(unittest.TestCase):
    @parameterized.expand([(1,), (2,), (3,)])
    def test_int(self, number):
        assert isinstance(number, int)
