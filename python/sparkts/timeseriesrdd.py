from py4j.java_gateway import java_import
from pyspark import RDD
from pyspark.serializers import FramedSerializer, SpecialLengths, write_int, read_int
from pyspark.sql import DataFrame
from utils import datetime_to_millis
from datetimeindex import DateTimeIndex
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
            jrdd = rdd._reserialize(_TimeSeriesSerializer())._jrdd.map( \
                jvm.com.cloudera.sparkts.BytesToKeyAndSeries())
            self._jtsrdd = jvm.com.cloudera.sparkts.TimeSeriesRDD( \
                dt_index._jdt_index, jrdd.rdd())
            RDD.__init__(self, rdd._jrdd, rdd.ctx)
        else:
            # Construct from a py4j.JavaObject pointing to a TimeSeriesRDD and a Python SparkContext
            jvm = sc._jvm
            jrdd = jvm.org.apache.spark.api.java.JavaRDD(jtsrdd, None).map( \
                jvm.com.cloudera.sparkts.KeyAndSeriesToBytes())
            RDD.__init__(self, jrdd, sc, _TimeSeriesSerializer())
            self._jtsrdd = jtsrdd

    def __getitem__(self, val):
        """
        Returns a TimeSeriesRDD representing a subslice of this TimeSeriesRDD, containing only
        values for a sub-range of the time it covers.
        """
        start = datetime_to_millis(val.start)
        stop = datetime_to_millis(val.stop)
        return TimeSeriesRDD(None, None, self._jtsrdd.slice(start, stop), self.ctx)

    def differences(self, n):
        """
        Returns a TimeSeriesRDD where each time series is differenced with the given order.
        
        The new RDD will be missing the first n date-times.
        
        Parameters
        ----------
        n : int
            The order of differencing to perform.
        """
        return TimeSeriesRDD(None, None, self._jtsrdd.differences(n), self.ctx)

    def fill(self, method):
        """
        Returns a TimeSeriesRDD with missing values imputed using the given method.
        
        Parameters
        ----------
        method : string
            "nearest" fills in NaNs with the closest non-NaN value, using the closest previous value
            in the case of a tie.  "linear" does a linear interpolation from the closest filled-in
            values.  "next" uses the closest value that is in the future of the missing value.
            "previous" uses the closest value from the past of the missing value.  "spline"
            interpolates using a cubic spline.
        """
        return TimeSeriesRDD(None, None, self._jtsrdd.fill(method), self.ctx)

    def map_series(self, fn, dt_index = None):
        """
        Returns a TimeSeriesRDD, with a transformation applied to all the series in this RDD.

        Either the series produced by the given function should conform to this TimeSeriesRDD's
        index, or a new DateTimeIndex should be given that they conform to.
        
        Parameters
        ----------
        fn : function
            A function that maps arrays of floats to arrays of floats.
        dt_index : DateTimeIndex
            A DateTimeIndex for the produced TimeseriesRDD.
        """
        if dt_index == None:
          dt_index = self.index()
        return TimeSeriesRDD(dt_index, self.map(fn))

    def to_instants(self):
        """
        Returns an RDD of instants, each a horizontal slice of this TimeSeriesRDD at a time.

        This essentially transposes the TimeSeriesRDD, producing an RDD of tuples of datetime and
        a numpy array containing all the observations that occurred at that time.
        """
        jrdd = self._jtsrdd.toInstants(-1).toJavaRDD().map( \
            self.ctx._jvm.com.cloudera.sparkts.InstantToBytes())
        return RDD(jrdd, self.ctx, _InstantDeserializer())

    def index(self):
        """Returns the index describing the times referred to by the elements of this TimeSeriesRDD
        """
        jindex = self._jtsrdd.index()
        return DateTimeIndex(jindex)

    def to_observations_dataframe(self, sql_ctx, ts_col='timestamp', key_col='key', val_col='value'):
        """
        Returns a DataFrame of observations, each containing a timestamp, a key, and a value.

        Parameters
        ----------
        sql_ctx : SQLContext
        ts_col : string
            The name for the timestamp column.
        key_col : string
            The name for the key column.
        val_col : string
            The name for the value column.
        """
        ssql_ctx = sql_ctx._ssql_ctx
        jdf = self._jtsrdd.toObservationsDataFrame(ssql_ctx, ts_col, key_col, val_col)
        return DataFrame(jdf, sql_ctx)

    def remove_instants_with_nans(self):
        """
        Returns a TimeSeriesRDD with instants containing NaNs cut out.
        
        The resulting TimeSeriesRDD has a slimmed down DateTimeIndex, missing all the instants
        for which any series in the RDD contained a NaN.
        """
        return TimeSeriesRDD(None, None, self._jtsrdd.removeInstantsWithNaNs(), self.ctx)

    def filter(self, predicate):
        return TimeSeriesRDD(self.index(), super(TimeSeriesRDD, self).filter(predicate))

    def find_series(self, key):
        """
        Finds a series in the TimeSeriesRDD by its key.
        
        Parameters
        ----------
        key : string
            The key of the series to find.
        """
        # TODO: this could be more efficient if we pushed it down into Java
        return self.filter(lambda x: x[0] == key).first()[1]

    def return_rates(self):
        """
        Returns a TimeSeriesRDD where each series is a return rate series for a series in this RDD.
        
        Assumes periodic (as opposed to continuously compounded) returns.
        """
        return TimeSeriesRDD(None, None, self._jtsrdd.returnRates(), self.ctx)

def time_series_rdd_from_observations(dt_index, df, ts_col, key_col, val_col):
    """Instantiates a TimeSeriesRDD from a DataFrame of observations.

    An observation is a row containing a timestamp, a string key, and float value.

    Parameters
    ----------
    dt_index : DateTimeIndex
        The index of the RDD to create. Observations not contained in this index will be ignored.
    df : DataFrame
    ts_col : string
        The name of the column in the DataFrame containing the timestamps.
    key_col : string
        The name of the column in the DataFrame containing the keys.
    val_col : string
        The name of the column in the DataFrame containing the values.
    """
    jvm = df._sc._jvm
    jtsrdd = jvm.com.cloudera.sparkts.TimeSeriesRDD.timeSeriesRDDFromObservations( \
      dt_index._jdt_index, df._jdf, ts_col, key_col, val_col)
    return TimeSeriesRDD(None, None, jtsrdd, df._sc)

class _TimeSeriesSerializer(FramedSerializer):
    """Serializes (key, vector) pairs to and from bytes.  Must be compatible with the Scala
    implementation in com.cloudera.sparkts.{BytesToKeyAndSeries, KeyAndSeriesToBytes}
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
        return '_TimeSeriesSerializer'

class _InstantDeserializer(FramedSerializer):
    """
    Serializes (timestamp, vector) pairs to an from bytes.  Must be compatible with the Scala
    implementation in com.cloudera.sparkts.InstantToBytes
    """
    
    def loads(self, obj):
        stream = BytesIO(obj)
        timestamp_ms = struct.unpack('!q', stream.read(8))[0]

        return (pd.Timestamp(timestamp_ms * 1000000), _read_vec(stream))

    def __repr__(self):
        return "_InstantDeserializer"

def _read_vec(stream):
    vector_length = read_int(stream)
    vector = np.empty(vector_length)
    # TODO: maybe some optimized way to read this all at once?
    for i in xrange(vector_length):
        vector[i] = struct.unpack('!d', stream.read(8))[0]
    
    return vector


