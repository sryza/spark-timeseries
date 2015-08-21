from py4j.java_gateway import java_import
from pyspark import RDD
from pyspark.serializers import FramedSerializer, SpecialLengths, write_int, read_int
from utils import datetime_to_millis
from datetimeindex import UniformDateTimeIndex
import struct
import numpy as np
import pandas as pd
from io import BytesIO

class TimeSeriesRDD(RDD):
    """
    A lazy distributed collection of univariate series with a conformed time dimension. Lazy in the
    sense that it is an RDD: it encapsulates all the information needed to generate its elements,
    but doesn't materialize them upon instantiation. Distributed in the sense that different
    univariate series within the collection can be stored and processed on different nodes. Within
    each univariate series, observations are not distributed. The time dimension is conformed in the
    sense that a single DateTimeIndex applies to all the univariate series. Each univariate series
    within the RDD has a String key to identify it.
    """

    def __init__(self, dt_index, rdd, jtsrdd = None, sc = None):
        if jtsrdd == None:
            # Construct from a Python RDD object and a Python DateTimeIndex
            jvm = rdd.ctx._jvm
            jrdd = rdd._reserialize(TimeSeriesSerializer())._jrdd.map( \
                jvm.com.cloudera.sparkts.BytesToKeyAndSeries())
            self._jtsrdd = jvm.com.cloudera.sparkts.TimeSeriesRDD( \
                dt_index._jdt_index, jrdd.rdd())
            RDD.__init__(self, rdd._jrdd, rdd.ctx)
        else:
            # Construct from a py4j.JavaObject pointing to a TimeSeriesRDD and a Python SparkContext
            jvm = sc._jvm
            jrdd = jvm.org.apache.spark.api.java.JavaRDD(jtsrdd, None).map( \
                jvm.com.cloudera.sparkts.KeyAndSeriesToBytes())
            RDD.__init__(self, jrdd, sc, TimeSeriesSerializer())
            self._jtsrdd = jtsrdd

    def collect_as_timeseries(self):
        jts = self._jtsrdd.collectAsTimeSeries()
        return TimeSeries(jts)

    def __getitem__(self, val):
        start = datetime_to_millis(val.start)
        stop = datetime_to_millis(val.stop)
        return TimeSeriesRDD(None, None, self._jtsrdd.slice(start, stop), self.ctx)

    def differences(self, n):
        return TimeSeriesRDD(None, None, self._jtsrdd.differences(n), self.ctx)

    def fill(self, method):
        return TimeSeriesRDD(None, None, self._jtsrdd.fill(method), self.ctx)

    def map_series(self, dn, dt_index = None):
        if dt_index == None:
          dt_index = self.index()
        return TimeSeriesRDD(dt_index, self.map(fn))

    def to_instants(self):
        jrdd = self._jtsrdd.toInstants(-1).toJavaRDD().map( \
            self.ctx._jvm.com.cloudera.sparkts.InstantToBytes())
        return RDD(jrdd, self.ctx, InstantDeserializer())

    def index(self):
         jindex = self._jtsrdd.index()
         if jindex.getClass().getName() == 'com.cloudera.sparkts.UniformDateTimeIndex':
             return UniformDateTimeIndex(None, None, None, None, jindex)

class TimeSeriesSerializer(FramedSerializer):
    """
    Serializes (key, vector) pairs.
    TODO: documentation
    """

    def dumps(self, obj):
        stream = BytesIO()
        (key, vector) = obj
        key_bytes = key.encode('utf-8')
        write_int(len(key_bytes), stream)
        stream.write(key_bytes)

        write_int(len(vector), stream)
        # TODO: maybe some optimized way to write this all at once?
        for value in vector:
            stream.write(struct.pack('!d', value))
        stream.seek(0)
        return stream.read()

    def loads(self, obj):
        stream = BytesIO(obj)
        key_length = read_int(stream)
        key = stream.read(key_length).decode('utf-8')

        return (key, _read_vec(stream))

    def __repr__(self):
        return 'TimeSeriesSerializer'

class InstantDeserializer(FramedSerializer):
    def loads(self, obj):
        stream = BytesIO(obj)
        timestamp_ms = struct.unpack('!q', stream.read(8))[0]

        return (pd.Timestamp(timestamp_ms * 1000000), _read_vec(stream))

    def __repr__(self):
        return "InstantDeserializer"

def _read_vec(stream):
    vector_length = read_int(stream)
    vector = np.empty(vector_length)
    # TODO: maybe some optimized way to read this all at once?
    for i in xrange(vector_length):
        vector[i] = struct.unpack('!d', stream.read(8))[0]
    
    return vector


