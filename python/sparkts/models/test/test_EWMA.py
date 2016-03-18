from sparkts.test.test_utils import PySparkTestCase

from pyspark.mllib.linalg import Vectors
from sparkts.models import EWMA
from sparkts.models.EWMA import EWMAModel

class FitEWMAModelTestCase(PySparkTestCase):
    def test_add_time_dependent_effects(self):
        orig = Vectors.dense([float(i) for i in range(1, 11)])
        m1 = EWMAModel(0.2, sc=self.sc)
        smoothed1 = m1.add_time_dependent_effects(orig)
        
        self.assertAlmostEqual(orig[0], smoothed1[0])
        self.assertAlmostEqual(smoothed1[1], m1.smoothing * orig[1] + (1 - m1.smoothing) * smoothed1[0])
        self.assertAlmostEqual(smoothed1[smoothed1.size - 1], 6.54, delta=0.01)
        
        m2 = EWMAModel(0.6, sc=self.sc)
        smoothed2 = m2.add_time_dependent_effects(orig)
        
        self.assertAlmostEqual(orig[0], smoothed2[0])
        self.assertAlmostEqual(smoothed2[1], m2.smoothing * orig[1] + (1 - m2.smoothing) * smoothed2[0])
        self.assertAlmostEqual(smoothed2[smoothed2.size - 1], 9.33, delta=0.01)
    
    def test_remove_time_dependent_effects(self):
        smoothed = Vectors.dense([1.0, 1.2, 1.56, 2.05, 2.64, 3.31, 4.05, 4.84, 5.67, 6.54])
        m1 = EWMAModel(0.2, sc=self.sc)
        orig1 = m1.remove_time_dependent_effects(smoothed)
        self.assertAlmostEqual(orig1[0], 1.0, delta=0.1)
        self.assertAlmostEqual(orig1[orig1.size - 1], 10.0, delta=0.1)

    def test_fit_EWMA_model(self):
        # We reproduce the example in ch 7.1 from https://www.otexts.org/fpp/7/1
        oil = Vectors.dense([446.7, 454.5, 455.7, 423.6, 456.3, 440.6, 425.3, 485.1, 506.0, 526.8, 514.3, 494.2])
        model = EWMA.fit_model(oil, sc=self.sc)
        self.assertAlmostEqual(0.89, model.smoothing, delta=0.01)
