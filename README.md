# autoDB
[![Build Status](https://travis-ci.com/JonathanVusich/autodb.svg?branch=master)](https://travis-ci.com/JonathanVusich/autodb)
[![Coverage Status](https://coveralls.io/repos/github/JonathanVusich/autodb/badge.svg?branch=master&kill_cache=1)](https://coveralls.io/github/JonathanVusich/autodb?branch=master&kill_cache=1)

autoDB is a pure Python NoSQL database that was created to make databases easy. It is designed with clean and simple APIs, and comes in a variety of implementations for specific use cases.

# When should I use autoDB?
autoDB is the perfect tool for small projects where speed is of less concern than ease of use and clean, Pythonic APIs. 
When you don't want to go through the hassle of setting up an SQL database but don't want to rely on JSON data storage, autoDB is the solution. It can serialize arbitrary Python classes, and allows users to perform index-based searches on stored objects. It also is written using no platform-specific APIs so that it is completely cross-platform.

# Planned features
- Multithreaded support
- Database compression
- Encryption

# Current performance/optimization problems
The main performance cost in autoDB is the serialization and deserialization of Python objects. autoDB uses the `pickle` library for this task since it is able to serialize arbitrary Python types, which removes the need for the user to define custom classes for serialization. `pickle` has a number of downsides though, including slow performance and security issues when unencrypted. An obvious solution to this problem is to use JSON for object serialization. However, this would require all stored objects to follow a predefined format and only contain data that is easy to serialize. This is a fairly large downside, as it would involve the user having to build their system for autoDB rather than just using it as a plug-and-play solution. The serialization engine will probably have to be implemented with JSON and tested extensively before this design decision can be fully resolved. Perhaps the user should have the option of choosing?

The index map can take up a considerable amount of memory when there are millions of unique objects in the database. This is due to the automatic indexing of any suitable object attribute. Perhaps the user should be able to select their desired indexes in order to save memory. Also, indexes may be able to be more efficiently implemented using lists instead of sets.

# Can I contribute to autoDB?
Yes, you absolutely can! Any and all help with the above issues is very welcome. Just fork the repository, and begin experimenting. If you come up with something that you believe is really good, be sure to share it by opening a PR!

# Development status
This project is still in the early development stages. An alpha build can be expected by the end of June 2019.
