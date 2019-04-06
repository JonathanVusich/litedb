import pytest
from autodb.database import MemoryDatabase
from ..test_table.table_test_objects import BadAndGoodObject, GoodObject, BadObject, GoodIndex
from hypothesis.strategies import integers
from hypothesis import given
import unittest
import random


class TestMemoryDatabase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.database = MemoryDatabase()
        cls.length = 0
        cls.class_choices = [None, None, None, BadObject, BadAndGoodObject, GoodObject]

    @given(
        value=integers()
    )
    def test_database_insert(self, value):
        self.database.insert(BadAndGoodObject(value))
        self.database.insert(GoodObject(value))
        self.database.insert(BadObject(value))
        self.length += 1
        assert len(self.database.class_map[BadAndGoodObject]) == self.length
        assert len(self.database.class_map[GoodObject]) == self.length
        assert len(self.database.class_map[BadObject]) == self.length
        assert len(self.database) == self.length * 3

    @given(
        value=integers()
    )
    def test_database_retrieve(self, value):
        class_choice = self.class_choices[value % 6]
        if class_choice is not None:
            with pytest.raises(IndexError):
                self.database.retrieve(class_type=class_choice, bad_index=value)
        if class_choice == BadObject:
            with pytest.raises(IndexError):
                self.database.retrieve(class_type=class_choice, good_index=value)
        elif class_choice == GoodObject:
            results = self.database.retrieve(class_type=class_choice, good_index=GoodIndex(value))
            if results:
                for result in results:
                    assert isinstance(result, GoodObject)
                    assert result.good_index == GoodIndex(value)
        elif class_choice == BadAndGoodObject:
            results = self.database.retrieve(class_type=class_choice, good_index=value)
            if results:
                for result in results:
                    assert isinstance(result, BadAndGoodObject)
                    assert result.good_index == value
        else:
            results = self.database.retrieve(class_type=None, good_index=value)
            if results:
                for result in results:
                    assert isinstance(result, BadAndGoodObject)
                    assert result.good_index == value

    @given(
        value=integers()
    )
    def test_database_delete(self, value):
        class_choice = self.class_choices[value % 6]
        if class_choice is not None:
            with pytest.raises(IndexError):
                self.database.retrieve(class_type=class_choice, bad_index=value)
        else:
            if class_choice == BadObject:
                with pytest.raises(IndexError):
                    self.database.retrieve(class_type=class_choice, good_index=value)
            else:
                results = self.database.retrieve(class_type=class_choice, good_index=value)
                if results:
                    for result in results:
                        if class_choice is not None:
                            assert isinstance(result, class_choice)
                        else:
                            assert isinstance(result, (GoodObject, BadAndGoodObject))