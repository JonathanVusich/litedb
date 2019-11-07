Installation
============

``liteDB`` is easily installed using ``pip``:

.. code-block:: bash

   $ pip install litedb


Basic Usage
===========

There are two different implementations of the ``liteDB`` interface, ``MemoryDatabase`` and ``DiskDatabase``.

As the names might suggest, ``MemoryDatabase`` is a completely volatile implementation that is designed to
be used as an in-memory caching solution.
``DiskDatabase`` instances reside on the filesystem, and can be written and read to from the same API that ``MemoryDatabase``
uses with one key difference: it requires the use of ``db.commit()`` in order to flush changes from the in-memory cache to disk.

Database creation
================================

Creating or connecting to ``liteDB`` instances is incredibly easy. ``DiskDatabase`` instances do require that a folder
path be specified for database storage.
If the folder contains an existing ``liteDB`` instance, the data from the disk will be loaded into the database upon
construction.

.. code-block:: python

   from litedb import DiskDatabase, MemoryDatabase

   memory_db = MemoryDatabase()
   disk_db = DiskDatabase("~/liteDB") # The source folder for the database

Inserting data
==============

Easy insertion of data is one of the key features of ``liteDB``. Whenever an object is inserted into a ``liteDB`` instance,
it will be checked for validity and then automatically indexed and stored in its proper table. Object attributes must be comparable
and hashable (implement ``__eq__``, ``__gt__``, ``__lt__`` and ``__hash__``) in order to qualify for indexing.
Attributes that are not indexed will **not** be able to be used in queries.

``liteDB`` excels at storing simple, lightweight value classes. For example, if we need to store weather data over time,
we might create a simple record class called ``WeatherRecord``.

.. code-block:: python

    from datetime import date

    class WeatherRecord:

    def __init__(self, day: date, temp: int, wind_speed: int):
        self.day = day
        self.temp = temp
        self.wind_speed = wind_speed

    def __repr__(self) -> str:
        return str(self.__dict__)

This object will be indexable by all three of its class attributes, and thus is an ideal candidate for ``liteDB``.
We will then proceed to insert our ``WeatherRecord`` objects into a ``liteDB`` instance.

.. code-block:: python

    from litedb import MemoryDatabase
    from datetime import date, timedelta

    day_before_yesterday = WeatherRecord(date.today() - timedelta(days=2), 45, 23)
    yesterday = WeatherRecord(date.today() - timedelta(days=1), 68, 4)
    today = WeatherRecord(date.today(), 57, 13)

    mem_db = MemoryDatabase()
    mem_db.insert(day_before_yesterday)
    mem_db.insert(yesterday)
    mem_db.insert(today)

.. warning::

    Be careful about inserting Python's builtin data types! The full list of blacklisted objects includes ``list``,
    ``dict``, ``tuple``, ``set``, ``frozenset``, ``bytes``, ``bytearray``, ``str``, ``int``, ``bool``, ``float``, ``complex``,
    ``memoryview``, and ``range``. These types are not indexable by the ``liteDB`` engine, and will cause a ``TypeError`` to be thrown!

Retrieving data
===============

Objects can be retrieved from the database by performing lookup operations on their indexable values or by filtering the
contents of the various tables. In order to check which attributes are indexable, we need to look up the table that contains
``WeatherRecord`` instances and view its indexes. We can do this by using the ``select()`` method. This method will use
a class type to look up the ``liteDB`` table that stores that type of object. Once we have selected the table that we want,
we can use the ``indexes`` attribute to view the available indexes.

.. code-block:: python

    mem_db.select(WeatherRecord).indexes
    >>> ['day', 'temp', 'wind_speed']

Now that we know that these indices are valid, we can start lookup up records. To do this, we will need to again select
the table that contains the ``WeatherRecord`` objects.

.. code-block:: python

    weather_records = mem_db.select(WeatherRecord)

We can find the weather for today by performing the following query:

.. code-block:: python

    list(weather_records.retrieve(day=date.today()))
    >>> [{'day': datetime.date(2019, 11, 6), 'temp': 57, 'wind_speed': 13}]

We can also retrieves items when their indexes fall in a certain range. To do this, we can use a tuple of two values that
specify the range.

.. note:: Ranges are inclusive in ``liteDB``!

.. code-block:: python

    list(weather_records.retrieve(wind_speed=(4, 13)))
    >>> [{'day': datetime.date(2019, 11, 5), 'temp': 68, 'wind_speed': 4}, {'day': datetime.date(2019, 11, 6), 'temp': 57, 'wind_speed': 13}]

In order to specify unbounded ranges, we can use the ``None`` keyword.

.. code-block:: python

    list(weather_records.retrieve(wind_speed=(None, 13)))
    >>> [{'day': datetime.date(2019, 11, 5), 'temp': 68, 'wind_speed': 4}, {'day': datetime.date(2019, 11, 6), 'temp': 57, 'wind_speed': 13}]

    list(weather_records.retrieve(wind_speed=(13, None)))
    >>> [{'day': datetime.date(2019, 11, 4), 'temp': 45, 'wind_speed': 23}, {'day': datetime.date(2019, 11, 6), 'temp': 57, 'wind_speed': 13}]

    list(weather_records.retrieve(wind_speed=(None, None)))
    >>> [{'day': datetime.date(2019, 11, 4), 'temp': 45, 'wind_speed': 23}, {'day': datetime.date(2019, 11, 5), 'temp': 68, 'wind_speed': 4}, {'day': datetime.date(2019, 11, 6), 'temp': 57, 'wind_speed': 13}]

There isn't much point to using a range of ``(None, None)`` (as it simply retrieves all of the items), but it is still valid syntax.

Queries can also be narrowed by adding more parameters to the ``retrieve()`` call.

.. code-block:: python

    list(weather_records.retrieve(wind_speed=(None, 13), day=date.today()))
    >>> [{'day': datetime.date(2019, 11, 6), 'temp': 57, 'wind_speed': 13}]

    list(weather_records.retrieve(wind_speed=(None, 13), day=date.today(), temp=12))
    >>> []

You might notice that each query is wrapped in a ``list()`` call. This is because ``liteDB`` returns a generator of entries
with each query, reducing the number of objects that have to be loaded into memory at once.

Deleting data
=============

Removing entries from a ``liteDB`` instance is as easy as performing a ``delete()`` query instead of a ``retrieve()`` query.

.. code-block:: python

    weather_records.delete(wind_speed=(None, 13))
    list(weather_records.retrieve_all())
    >>> [{'day': datetime.date(2019, 11, 4), 'temp': 45, 'wind_speed': 23}]