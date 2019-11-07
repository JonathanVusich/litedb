liteDB
======

.. image:: https://travis-ci.org/JonathanVusich/litedb.svg?branch=master&kill_cache=1
   :target: https://travis-ci.org/JonathanVusich/litedb
   :alt: Build Status

.. image:: https://coveralls.io/repos/github/JonathanVusich/litedb/badge.svg?branch=master
   :target: https://coveralls.io/github/JonathanVusich/litedb?branch=master
   :alt: Coverage
   
.. image:: https://readthedocs.org/projects/litedb/badge/?version=latest
   :target: https://litedb.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status
   
liteDB is a Python NoSQL database that was created to make databases easy. It is designed with clean and simple APIs, and comes in a couple of different implementations for specific use cases.

When should I use liteDB?
=========================

liteDB is the perfect tool for small projects where performance is of less concern than ease of use and clean, Pythonic APIs. 
When you don't want to go through the hassle of setting up an SQL database but don't want to rely on JSON data storage, liteDB is the solution. It works by using `pickle` to serialize arbitrary Python classes, and allows users to perform index-based searches on stored objects. It also is written using no platform-specific APIs so that it is completely cross-platform.

Future planned features
=======================

- Database compression using gzip
- Encryption
- Better docs
- Useful examples

Current performance/optimization problems
=========================================
Note: This section is aimed mostly at developers who need the highest levels of performance. For most tasks, LiteDB will be quite performant.

- The main performance cost in autoDB is the serialization and deserialization of Python objects. liteDB uses the ``pickle`` library for this task since it is able to serialize arbitrary Python types, which removes the need for the user to define custom classes for serialization. ``pickle`` has a couple of downsides though, including slow performance and security issues when unencrypted. An obvious solution to this problem is to use JSON for object serialization. However, this would require all stored objects to follow a predefined format and only contain data that is easy to serialize. This is a fairly large downside, as it would involve the user having to build their system for liteDB rather than just using it as a plug-and-play solution. The serialization engine will probably have to be implemented with JSON and tested extensively before this design decision can be fully resolved.

- The index map can take up a considerable amount of memory when there are millions of unique objects in the database. This is due to the automatic indexing of any suitable object attribute. Perhaps the user should be able to select their desired indexes in order to save memory. Also, indexes may be able to be more efficiently implemented using lists instead of sets.

Can I contribute to liteDB?
===========================
Yes, you absolutely can! Any and all help with future functionality and the above issues is very welcome. Just fork the repository, and begin experimenting. If you come up with something that you believe is really good, be sure to share it by opening a PR!

Development status
==================
This project has exited the implementation phase and has graduated to a beta release.
