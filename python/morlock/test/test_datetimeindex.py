from test_utils import PySparkTestCase
from morlock.datetimeindex import *
import pandas as pd

class DateTimeIndexTestCase(PySparkTestCase):
    def test_uniform(self):
        freq = DayFrequency(3, self.sc)
        self.assertEqual(freq.days(), 3)
        start = '2015-04-10'
        index = uniform(start, periods=5, freq=freq, sc=self.sc)
        index2 = uniform(start, end='2015-04-22', freq=freq, sc=self.sc)
        self.assertEqual(index, index2)
        
        self.assertEqual(len(index), 5)
        self.assertEqual(index.first(), pd.to_datetime('2015-04-10'))
        self.assertEqual(index.last(), pd.to_datetime('2015-04-22'))
        subbydate = index[pd.to_datetime('2015-04-13'):pd.to_datetime('2015-04-19')]
        subbyloc = index.islice(1, 4)
        self.assertEqual(subbydate, subbyloc)
        self.assertEqual(subbydate.first(), pd.to_datetime('2015-04-13'))
        self.assertEqual(subbydate.last(), pd.to_datetime('2015-04-19'))
        self.assertEqual(subbydate.datetime_at_loc(0), pd.to_datetime('2015-04-13'))
        self.assertEqual(subbydate[pd.to_datetime('2015-04-13')], 0)

    def test_irregular(self):
        pd_index = pd.date_range('2015-04-10', periods=5, freq='3D')
        dt_index = irregular(pd_index, self.sc)

        self.assertEqual(len(dt_index), 5)
        self.assertEqual(dt_index.first(), pd.to_datetime('2015-04-10'))
        self.assertEqual(dt_index.last(), pd.to_datetime('2015-04-22'))
        subbydate = dt_index[pd.to_datetime('2015-04-13'):pd.to_datetime('2015-04-19')]
        subbyloc = dt_index.islice(1, 4)
        self.assertEqual(subbydate, subbyloc)
        self.assertEqual(subbydate.first(), pd.to_datetime('2015-04-13'))
        self.assertEqual(subbydate.last(), pd.to_datetime('2015-04-19'))
        self.assertEqual(subbydate.datetime_at_loc(0), pd.to_datetime('2015-04-13'))
        self.assertEqual(subbydate[pd.to_datetime('2015-04-13')], 0)

        pd_index2 = dt_index.to_pandas_index()
        self.assertTrue(pd_index.equals(pd_index2), str(pd_index) + "!=" + str(pd_index2))

