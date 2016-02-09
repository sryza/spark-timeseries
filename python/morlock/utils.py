import sys
import os
import logging
import pandas as pd

from glob import glob

def add_pyspark_path():
    """Add PySpark to the library path based on the value of SPARK_HOME. """

    try:
        spark_home = os.environ['SPARK_HOME']

        sys.path.append(os.path.join(spark_home, 'python'))
        py4j_src_zip = glob(os.path.join(spark_home, 'python',
                                         'lib', 'py4j-*-src.zip'))
        if len(py4j_src_zip) == 0:
            raise ValueError('py4j source archive not found in %s'
                             % os.path.join(spark_home, 'python', 'lib'))
        else:
            py4j_src_zip = sorted(py4j_src_zip)[::-1]
            sys.path.append(py4j_src_zip[0])
    except KeyError:
        logging.error("""SPARK_HOME was not set. please set it. e.g.
          SPARK_HOME='/home/...' ./bin/pyspark [program]""")
        exit(-1)
    except ValueError as e:
        logging.error(str(e))
        exit(-1)


def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)

def datetime_to_nanos(dt):
    """
    Accepts a string, Pandas Timestamp, or long, and returns nanos since the epoch.
    """
    if isinstance(dt, pd.Timestamp):
        return dt.value
    elif isinstance(dt, str):
        return pd.Timestamp(dt).value
    elif isinstance(dt, long):
        return dt

    raise ValueError
