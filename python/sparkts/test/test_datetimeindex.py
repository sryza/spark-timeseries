from test_utils import PySparkTestCase
from datetimeindex import *
import pandas as pd

class UniformDateTimeIndexTestCase(PySparkTestCase):
    def test_uniform(self):
        freq = DayFrequency(3, self.sc)
        self.assertEqual(freq.days(), 3)
        start = '2015-04-10'
        index = uniform(start, 5, freq, self.sc)

        self.assertEqual(index.size(), 5)
        self.assertEqual(index.first(), pd.to_datetime('2015-04-10'))
        self.assertEqual(index.last(), pd.to_datetime('2015-04-22'))
        subbydate = index[pd.to_datetime('2015-04-13'):pd.to_datetime('2015-04-19')]
        subbyloc = index.islice(1, 4)
        print(subbydate)
        print(subbyloc)
        self.assertEqual(subbydate, subbyloc)
        self.assertEqual(subbydate.first(), pd.to_datetime('2015-04-13'))
        self.assertEqual(subbydate.last(), pd.to_datetime('2015-04-19'))
        self.assertEqual(subbydate.datetime_at_loc(0), pd.to_datetime('2015-04-13'))
        self.assertEqual(subbydate[pd.to_datetime('2015-04-13')], 0)

