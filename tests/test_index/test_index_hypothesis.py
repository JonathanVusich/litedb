import pytest
from hypothesis import given, settings, assume
from hypothesis.strategies import text, integers
import unittest

from autodb.index import Index


class IndexAddTest(unittest.TestCase):

    def setUp(self):
        self.index = Index(str)

    @given(
        value=text(),
        index=integers()
    )
    def test_insert(self, value, index):
        if value is None:
            self.index.add(value, index)
            assert index in self.index.none_indexes
        elif value in self.index.indexes:
            previous_value = self.index.indexes[value]
            if isinstance(previous_value, set):
                assume(index not in previous_value)
                first_len = len(previous_value)
                self.index.add(value, index)
                assert len(self.index.indexes[value]) - first_len == 1
                assert index in self.index.indexes[value]
            else:
                assume(index != previous_value)
                self.index.add(value, index)
                new_value = self.index.indexes[value]
                assert isinstance(new_value, set)
                assert len(new_value) == 2
                assert index in new_value
        else:
            self.index.add(value, index)
            new_value = self.index.indexes[value]
            assert isinstance(new_value, int)
            assert new_value == index


class IndexRetrieveTest(unittest.TestCase):

    def setUp(self) -> None:
        self.index = Index(str)

    @given(
        value=text(),
        index=integers()
    )
    @settings(max_examples=100)
    def test_retrieve(self, value, index):
        original_value = self.index.retrieve(value)
        if isinstance(original_value, set):
            original_value = original_value.copy()
            assume(index not in original_value)
        assert isinstance(original_value, set) or original_value is None
        self.index.add(value, index)
        new_value = self.index.retrieve(value)
        if original_value is None:
            assert isinstance(new_value, set)
            assert new_value == {index}
        else:
            assert len(new_value) - len(original_value) == 1
            assert index in new_value


class IndexRetrieveRangeTest(unittest.TestCase):

    def setUp(self) -> None:
        self.int_index = Index(int)
        for i in range(100):
            self.int_index.add(i, i)
        self.str_index = Index(int)
        for i in range(10):
            self.str_index.add(str(i), i)

    @given(
        low=integers(min_value=0, max_value=99),
        high=integers(min_value=0, max_value=99)
    )
    @settings(
        max_examples=100
    )
    def test_retrieve_range_int(self, low, high):
        assume(high > low)
        range = self.int_index.retrieve_range(low, high)
        if high - low == 0:
            assert range == {high}
        else:
            assert len(range) == high - low + 1

    @given(
        low=integers(min_value=0, max_value=9),
        high=integers(min_value=0, max_value=9)
    )
    @settings(
        max_examples=10
    )
    def test_retrieve_range_str(self, low, high):
        high = str(high)
        low = str(low)
        assume(high > low)
        range = self.str_index.retrieve_range(low, high)
        if high == low:
            assert range == {int(high)}
        else:
            assert len(range) == int(high) - int(low) + 1


class IndexDestroyTest(unittest.TestCase):

    def setUp(self) -> None:
        self.index = Index(str)

    @given(
        value=text(),
        index=integers()
    )
    @settings(max_examples=100)
    def test_destroy(self, value, index):
        self.index.add(value, index)
        self.index.destroy(value, index)
        new_entry = self.index.retrieve(value)
        assert new_entry == set()
        with pytest.raises(KeyError):
            self.index.destroy(value, index)
