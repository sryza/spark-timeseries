import numpy as np

from sparkts.test.test_utils import PySparkTestCase

from pyspark.mllib.linalg import Vectors
from sparkts.models import Autoregression
from sparkts.models.Autoregression import ARModel

class FitAutoregressionModelTestCase(PySparkTestCase):
    def test_fit_ar1_model(self):
        model = ARModel(1.5, [0.2], sc=self.sc)
        ts = model.sample(5000)
        fittedModel = Autoregression.fit_model(ts, 1, sc=self.sc)
        self.assertTrue(len(fittedModel.coefficients) == 1)
        self.assertAlmostEqual(fittedModel.c, 1.5, delta=0.07)
        self.assertAlmostEqual(fittedModel.coefficients[0], 0.2, delta=0.03)

    def test_fit_ar2_model(self):
        model = ARModel(1.5, [0.2, 0.3], sc=self.sc)
        ts = model.sample(5000)
        fittedModel = Autoregression.fit_model(ts, 2, sc=self.sc)
        self.assertTrue(len(fittedModel.coefficients) == 2)
        self.assertAlmostEqual(fittedModel.c, 1.5, delta=0.15)
        self.assertAlmostEqual(fittedModel.coefficients[0], 0.2, delta=0.03)
        self.assertAlmostEqual(fittedModel.coefficients[1], 0.3, delta=0.03)
    
    def test_add_and_remove_time_dependent_effects(self):
        ts = Vectors.dense(np.random.normal(size=1000))
        model = ARModel(1.5, [0.2, 0.3], sc=self.sc)
        added = model.add_time_dependent_effects(ts)
        removed = model.remove_time_dependent_effects(added)
        for i in range(len(added)):
            self.assertAlmostEqual(ts[i], removed[i], delta=0.001, msg=("failed at index %d: %f != %f" % (i, added[i], removed[i])))
        