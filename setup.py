#!/usr/bin/env python3
from setuptools import setup, find_packages
with open("./README.md", "r") as f:
    long_desription = f.read()

setup(
    name="WebMonitor",
    version=0.0,
    packages=find_packages(),
    scripts=[
        "./web_checker.py",
        "./kafka_pg_transfer.py"
    ],
    install_requires=[
        "requests",
        "pyyaml",
        "kafka-python",
        "psycopg2",
    ],
    description="",
    long_description=long_desription,
    long_description_content_type="text/markdown",
    author="Evgeny Petrov",
    author_email="golovasteek@gmail.com",
    url="https://github.com/golovasteek/web_monitor"
)
