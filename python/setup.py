from setuptools import setup, find_packages
import os
import re

# determine version
VERSION_FILE="sparkts/_version.py"
verstrline = open(VERSION_FILE, "rt").read()
VERSION_REGEX = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VERSION_REGEX, verstrline, re.M)
if mo:
    version_string = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

JAR_FILE = 'sparkts-' + version_string + '-SNAPSHOT-jar-with-dependencies.jar'

setup(
    name='sparkts',
    description = 'A library for analyzing large time series data with Apache Spark',
    author = 'Sandy Ryza',
    author_email = 'sandy@cloudera.com',
    url = 'https://github.com/sryza/spark-timeseries',
    version=version_string,
    packages=find_packages(),
    include_package_data = True,
    classifiers = [],
    keywords = ['spark', 'time', 'series', 'data', 'analysis'],
    install_requires = [
        'pandas >= 0.13',
        'numpy >= 1.9.2'
    ],
    test_requires = [
        'nose == 1.3.7',
        'unittest2 >= 1.0.0'
    ]
)
