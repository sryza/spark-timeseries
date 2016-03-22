import unittest
import os

import numpy as np

from sparkts.test.test_utils import PySparkTestCase

from sparkts.models import ARIMA
from sparkts.models.ARIMA import ARIMAModel, fit_model

def data_file_as_nparray(fn):
    dn = os.path.dirname(os.path.realpath(__file__))
    with open(dn + '/' + fn) as f:
        lines = f.readlines()
    values = [float(l) for l in lines]
    return np.array(values)


class FitARIMAModelTestCase(PySparkTestCase):
    def test_compare_with_r(self):
        data = data_file_as_nparray('resources/R_ARIMA_DataSet1.csv')
        model = fit_model(1, 0, 1, data, sc=self.sc)
        (c, ar, ma) = model.coefficients
        self.assertAlmostEqual(ar, 0.3, delta=0.01)
        self.assertAlmostEqual(ma, 0.7, delta=0.01)
    
    def test_compare_with_r_with_userparams(self):
        data = data_file_as_nparray('resources/R_ARIMA_DataSet1.csv')
        model = fit_model(1, 0, 1, data, userInitParams=[0.0, 0.2, 1.0], sc=self.sc)
        (c, ar, ma) = model.coefficients
        self.assertAlmostEqual(ar, 0.55, delta=0.01)
        self.assertAlmostEqual(ma, 1.03, delta=0.01)
    
    @unittest.skip("""
    Test isn't working at the moment. model.sample(1000) results in a TooManyEvaluationsException
    and sampling with fewer values results in a newModel that doesn't match the original model.
    """)
    def test_remodel_sample_data(self):
        """
        Data sampled from a given model should result in a similar model if fit again.
        """
        model = ARIMAModel(2, 1, 2, [8.2, 0.2, 0.5, 0.3, 0.1], sc=self.sc)
        sampled = model.sample(1000)
        newModel = fit_model(2, 1, 2, sampled, sc=self.sc)
        (c, ar1, ar2, ma1, ma2) = model.coefficients
        (cTest, ar1Test, ar2Test, ma1Test, ma2Test) = newModel.coefficients
        self.assertAlmostEqual(c, cTest, delta=1)
        self.assertAlmostEqual(ar1, ar1Test, delta=0.1)
        self.assertAlmostEqual(ar2, ar2Test, delta=0.1)
        self.assertAlmostEqual(ma1, ma1Test, delta=0.1)
        self.assertAlmostEqual(ma2, ma2Test, delta=0.1)
        
    def test_stationarity_and_invertability(self):
        model1 = ARIMAModel(1, 0, 0, [0.2, 1.5], hasIntercept = True, sc=self.sc)
        self.assertFalse(model1.is_stationary())
        self.assertTrue(model1.is_invertible())

        model2 = ARIMAModel(0, 0, 1, [0.13, 1.8], hasIntercept = True, sc=self.sc)
        self.assertTrue(model2.is_stationary())
        self.assertFalse(model2.is_invertible())
        
        model3 = ARIMAModel(2, 0, 0, [0.003359, 1.545, -0.5646], hasIntercept = True, sc=self.sc)
        self.assertTrue(model3.is_stationary())
        self.assertTrue(model3.is_invertible())
        
        model4 = ARIMAModel(1, 0, 1, [-0.09341,  0.857361, -0.300821], hasIntercept = True, sc=self.sc)
        self.assertTrue(model4.is_stationary())
        self.assertTrue(model4.is_invertible())
