from test_utils import PySparkTestCase
from sparkts.timeseriesrdd import *
from sparkts.timeseriesrdd import _TimeSeriesSerializer
from sparkts.datetimeindex import *
import pandas as pd
import numpy as np
from unittest import TestCase
from io import BytesIO
from pyspark.sql import SQLContext

class TimeSeriesSerializerTestCase(TestCase):
    def test_times_series_serializer(self):
        serializer = _TimeSeriesSerializer()
        stream = BytesIO()
        series = [('abc', np.array([4.0, 4.0, 5.0])), ('123', np.array([1.0, 2.0, 3.0]))]
        serializer.dump_stream(iter(series), stream)
        stream.seek(0)
        reconstituted = list(serializer.load_stream(stream))
        self.assertEquals(reconstituted[0][0], series[0][0])
        self.assertEquals(reconstituted[1][0], series[1][0])
        self.assertTrue((reconstituted[0][1] == series[0][1]).all())
        self.assertTrue((reconstituted[1][1] == series[1][1]).all())

class TimeSeriesRDDTestCase(PySparkTestCase):
    def test_time_series_rdd(self):
        freq = DayFrequency(1, self.sc)
        start = '2015-04-09'
        dt_index = uniform(start, periods=10, freq=freq, sc=self.sc)
        vecs = [np.arange(0, 10), np.arange(10, 20), np.arange(20, 30)]
        rdd = self.sc.parallelize(vecs).map(lambda x: (str(x[0]), x))
        tsrdd = TimeSeriesRDD(dt_index, rdd)
        self.assertEquals(tsrdd.count(), 3)

        contents = tsrdd.collectAsMap()
        self.assertEquals(len(contents), 3)
        self.assertTrue((contents["0"] == np.arange(0, 10)).all())
        self.assertTrue((contents["10"] == np.arange(10, 20)).all())
        self.assertTrue((contents["20"] == np.arange(20, 30)).all())

        subslice = tsrdd['2015-04-10':'2015-04-15']
        self.assertEquals(subslice.index(), uniform('2015-04-10', periods=6, freq=freq, sc=self.sc))
        contents = subslice.collectAsMap()
        self.assertEquals(len(contents), 3)
        self.assertTrue((contents["0"] == np.arange(1, 7)).all())
        self.assertTrue((contents["10"] == np.arange(11, 17)).all())
        self.assertTrue((contents["20"] == np.arange(21, 27)).all())

    def test_to_instants(self):
        vecs = [np.arange(x, x + 4) for x in np.arange(0, 20, 4)]
        labels = ['a', 'b', 'c', 'd', 'e']
        start = '2015-4-9'
        dt_index = uniform(start, periods=4, freq=DayFrequency(1, self.sc), sc=self.sc)
        rdd = self.sc.parallelize(zip(labels, vecs), 3)
        tsrdd = TimeSeriesRDD(dt_index, rdd)
        samples = tsrdd.to_instants().collect()
        target_dates = ['2015-4-9', '2015-4-10', '2015-4-11', '2015-4-12']
        self.assertEquals([x[0] for x in samples], [pd.Timestamp(x) for x in target_dates])
        self.assertTrue((samples[0][1] == np.arange(0, 20, 4)).all())
        self.assertTrue((samples[1][1] == np.arange(1, 20, 4)).all())
        self.assertTrue((samples[2][1] == np.arange(2, 20, 4)).all())
        self.assertTrue((samples[3][1] == np.arange(3, 20, 4)).all())

    def test_to_observations(self):
        sql_ctx = SQLContext(self.sc)
        vecs = [np.arange(x, x + 4) for x in np.arange(0, 20, 4)]
        labels = ['a', 'b', 'c', 'd', 'e']
        start = '2015-4-9'
        dt_index = uniform(start, periods=4, freq=DayFrequency(1, self.sc), sc=self.sc)
        rdd = self.sc.parallelize(zip(labels, vecs), 3)
        tsrdd = TimeSeriesRDD(dt_index, rdd)

        obsdf = tsrdd.to_observations_dataframe(sql_ctx)
        tsrdd_from_df = time_series_rdd_from_observations( \
            dt_index, obsdf, 'timestamp', 'key', 'value')
        
        ts1 = tsrdd.collect()
        ts1.sort(key = lambda x: x[0])
        ts2 = tsrdd_from_df.collect()
        ts2.sort(key = lambda x: x[0])
        self.assertTrue(all([pair[0][0] == pair[1][0] and (pair[0][1] == pair[1][1]).all() \
            for pair in zip(ts1, ts2)]))
        
        df1 = obsdf.collect()
        df1.sort(key = lambda x: x.value)
        df2 = tsrdd_from_df.to_observations_dataframe(sql_ctx).collect()
        df2.sort(key = lambda x: x.value)
        self.assertEquals(df1, df2)

    def test_filter(self):
        vecs = [np.arange(x, x + 4) for x in np.arange(0, 20, 4)]
        labels = ['a', 'b', 'c', 'd', 'e']
        start = '2015-4-9'
        dt_index = uniform(start, periods=4, freq=DayFrequency(1, self.sc), sc=self.sc)
        rdd = self.sc.parallelize(zip(labels, vecs), 3)
        tsrdd = TimeSeriesRDD(dt_index, rdd)
        filtered = tsrdd.filter(lambda x: x[0] == 'a' or x[0] == 'b')
        self.assertEquals(filtered.count(), 2)
        # assert it has TimeSeriesRDD functionality:
        filtered['2015-04-10':'2015-04-15'].count()

    def test_to_pandas_series_rdd(self):
        vecs = [np.arange(x, x + 4) for x in np.arange(0, 20, 4)]
        labels = ['a', 'b', 'c', 'd', 'e']
        start = '2015-4-9'
        dt_index = uniform(start, periods=4, freq=DayFrequency(1, self.sc), sc=self.sc)
        rdd = self.sc.parallelize(zip(labels, vecs), 3)
        tsrdd = TimeSeriesRDD(dt_index, rdd)

        series_arr = tsrdd.to_pandas_series_rdd().collect()

        pd_index = dt_index.to_pandas_index()
        self.assertEquals(len(vecs), len(series_arr))
        for i in xrange(len(vecs)):
            self.assertEquals(series_arr[i][0], labels[i])
            self.assertTrue(pd.Series(vecs[i], pd_index).equals(series_arr[i][1]))

