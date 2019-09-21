import unittest

import pytest
from hypothesis import given
from hypothesis.strategies import integers

from pydb.database import MemoryDatabase
from ..test_table.table_test_objects import BadAndGoodObject, GoodObject, BadObject, GoodIndex


class TestMemoryDatabase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.database = MemoryDatabase()
        cls.length = 0
        cls.class_choices = [BadObject, BadAndGoodObject, GoodObject]

    @given(
        value=integers()
    )
    def test_database_insert(self, value):
        self.database.insert(BadAndGoodObject(value))
        self.database.insert(GoodObject(value))
        self.database.insert(BadObject(value))
        self.length += 1
        assert len(self.database._tables[BadAndGoodObject]) == self.length
        assert len(self.database._tables[GoodObject]) == self.length
        assert len(self.database._tables[BadObject]) == self.length
        assert len(self.database) == self.length * 3

    @given(
        value=integers()
    )
    def test_database_retrieve(self, value):
        class_choice = self.class_choices[value % 3]
        with pytest.raises(IndexError):
            self.database.select(class_choice).retrieve(bad_index=value)
        if class_choice == BadObject:
            with pytest.raises(IndexError):
                self.database.select(class_choice).retrieve(good_index=value)
        elif class_choice == GoodObject:
            results = self.database.select(class_choice).retrieve(good_index=GoodIndex(value))
            if results:
                for result in results:
                    assert isinstance(result, GoodObject)
                    assert result.good_index == GoodIndex(value)
        elif class_choice == BadAndGoodObject:
            results = self.database.select(class_choice).retrieve(good_index=value)
            if results:
                for result in results:
                    assert isinstance(result, BadAndGoodObject)
                    assert result.good_index == value

    @given(
        value=integers()
    )
    def test_database_zelete(self, value):
        class_choice = self.class_choices[value % 3]
        with pytest.raises(IndexError):
            self.database.select(class_choice).retrieve(bad_index=value)
        if class_choice == BadObject:
            with pytest.raises(IndexError):
                self.database.select(class_choice).delete(good_index=value)
        elif class_choice == GoodObject:
            self.database.select(class_choice).delete(good_index=GoodIndex(value))
            assert not self.database.select(class_choice).retrieve(good_index=GoodIndex(value))
        elif class_choice == BadAndGoodObject:
            self.database.select(class_choice).delete(good_index=value)
            assert not self.database.select(class_choice).retrieve(good_index=value)
