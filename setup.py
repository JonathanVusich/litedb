import os

from setuptools import setup, find_packages


def read(file_name: str):
    return open(os.path.join(os.path.dirname(__file__), file_name)).read()


setup(
    name="litedb",
    version="1.0.0",
    author="Jonathan Vusich",
    author_email="jonathanvusich@gmail.com",
    description="A pure Python NoSQL database",
    long_description=read("README.rst"),
    long_description_content_type="text/markdown",
    license="MIT",
    keywords="nosql database python",
    url="https://github.com/JonathanVusich/litedb",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=read("requirements.txt"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
    ],
)
