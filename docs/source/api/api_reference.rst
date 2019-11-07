This is the full documentation for the entire ``liteDB`` API.

Database
========

.. py:class:: litedb.Database(abc.ABC)

    :class:`Database` provides a standard base class that all ``liteDB``
    instances should implement.

    .. py:method:: __len__()

        The number of items in this database.

    .. py:method:: __iter__()

        Allows iteration over all of the class types in this database.

    .. py:method:: __repr__()

        Print a summary of all the tables in this database.

    .. py:attribute:: tables

        Returns a ``ValuesView`` of the tables in this database.

    .. py:method:: insert(item: object)

        :param object item: A Python object.

        Inserts and indexes the given item.

    .. py:method:: select(cls)

        :param class cls: A Python class definition, such as ``Decimal``.

        Returns a :class:`Table` that corresponds to the given class type.


MemoryDatabase
==============


.. py:class:: litedb.MemoryDatabase(litedb.Database)

    :class:`MemoryDatabase` is an in-memory implementation of :class:`Database`. All items
    are stored in memory, and are discarded when the database is no longer referenced in the script.


DiskDatabase
============


.. py:class:: litedb.DiskDatabase(litedb.Database)

    :class:`DiskDatabase` is a persistent implementation of :class:`Database` that resides on the filesystem.

    .. py:method:: __init__(directory: str[, config=Config()])

        :param str directory: Valid filesystem path.

        The path on disk where :class:`DiskDatabase` will write to. If data is already present
        in this location, it will be loaded into this database.

        :param Config config: A :class:`Config` instance.

        Allows the user to specify a custom paging configuration in order to maximize performance.

    .. py:attribute:: modified

        Returns ``True`` if there are changes to this database that have not yet been synchronized.

    .. py:method:: commit()

        Synchronizes all cached items to disk for the entire database.


Table
=====


.. py:class:: litedb.Table(abc.ABC)

    :class:`Table` provides a standard base class which all ``liteDB`` tables
    should implement.

     .. py:method:: __len__()

        The number of items in this table.

    .. py:method:: __iter__()

        Allows iteration over all of the items in this table.

    .. py:method:: __repr__()

        Prints out useful information about this table.

    .. py:attribute:: indexes

        Returns a list of valid index names for this class type.

    .. py:method:: retrieve(**kwargs)

        :param kwargs: Keyword arguments that describe item indexes.

        Returns a generator of items that conform to the index constraints laid
        out by the keyword arguments. See the data retrieval section for more information
        on this.

    .. py:method:: delete(**kwargs)

        :param kwargs: Keyword arguments that describe item indexes.

        Removes all items in this table that conform to the index constraints laid
        out by the keyword arguments. See the data removal section for more information
        on this.

    .. py:method:: clear()

        Removes all items from this table.


MemoryTable
===========

.. py:class:: litedb.MemoryTable(litedb.Table)

    :class:`MemoryTable` is an in-memory implementation of :class:`Table`.

    .. note:: You will not ever need to instantiate this class!


PersistentTable
===============

.. py:class:: litedb.PersistentTable(litedb.Table)

    :class:`PersistentTable` is a persistent implementation of :class:`Table`.

    .. note:: You will not ever need to instantiate this class!

    .. py:attribute:: modified

    Returns ``True`` if there are changes to this table that have not yet been synchronized.

    .. py:method:: commit()

    Synchronizes this table's in-memory item/index cache to disk.


Config
======

.. py:class:: litedb.Config

    :class:`Config` allows customization of the :class:`DiskDatabase` paging system.

    .. warning:: Once you have defined a custom configuration for a :class:`DiskDatabase` instance, it is recommended that you do not change it!

    .. py:method:: __init__(page_size: int = 512, page_cache: int = 512)

    :param int page_size: Number of items to store in each page

    A higher page size means there is less overhead manipulating pages, but large sizes can also result in more memory usage.
    The recommended default is 512.

    :param in page_cache: Number of item pages to retain in memory.

    A higher item page count means fewer I/O calls, but also more memory usage.
    The recommended default is 512.