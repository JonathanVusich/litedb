import pytest
from autodb.database import MemoryDatabase
from ..test_table.table_test_objects import BadAndGoodObject, GoodObject, BadObject
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
        class_choice = random.choice(self.class_choices)
        if value % 2 == 0:
            if class_choice is not None:
                with pytest.raises(IndexError):
                    self.database.retrieve(class_type=class_choice, bad_index=value)
        else:
            if class_choice == BadObject:
                with pytest.raises(IndexError):
                    results = self.database.retrieve(class_type=class_choice, good_index=value)
            else:
                results = self.database.retrieve(class_type=class_choice, good_index=value)
                for result in results:
                    if class_choice is not None:
                        assert isinstance(result, class_choice)
                    else:
                        assert isinstance(result, (GoodObject, BadAndGoodObject))