import unittest
import numpy as np

from sparkts.test.test_utils import PySparkTestCase

from sparkts.models import RegressionARIMA
from sparkts.models.RegressionARIMA import RegressionARIMAModel

class FitRegressionARIMAModelTestCase(PySparkTestCase):
    def test_cochrane_orcutt_empdata_with_maxiter(self):
        metal = [
            44.2, 44.3, 44.4, 43.4, 42.8, 44.3, 44.4, 44.8, 44.4, 43.1, 42.6, 42.4, 42.2,
            41.8, 40.1, 42, 42.4, 43.1, 42.4, 43.1, 43.2, 42.8, 43, 42.8, 42.5, 42.6, 42.3, 42.9, 43.6,
            44.7, 44.5, 45, 44.8, 44.9, 45.2, 45.2, 45, 45.5, 46.2, 46.8, 47.5, 48.3, 48.3, 49.1, 48.9,
            49.4, 50, 50, 49.6, 49.9, 49.6, 50.7, 50.7, 50.9, 50.5, 51.2, 50.7, 50.3, 49.2, 48.1
        ]
        
        vendor = [
            322.0, 317, 319, 323, 327, 328, 325, 326, 330, 334, 337, 341, 322, 318, 320,
            326, 332, 334, 335, 336, 335, 338, 342, 348, 330, 326, 329, 337, 345, 350, 351, 354, 355, 357,
            362, 368, 348, 345, 349, 355, 362, 367, 366, 370, 371, 375, 380, 385, 361, 354, 357, 367, 376,
            381, 381, 383, 384, 387, 392, 396
        ]
        
        Y = np.array(metal)
        regressors = np.array(vendor).reshape(len(vendor), 1)
        
        regARIMA = RegressionARIMA.fit_model(Y, regressors, method="cochrane-orcutt", optimizationArgs=[1], sc=self.sc)
        beta = regARIMA.regressionCoeff
        self.assertAlmostEqual(beta[0], 28.918, delta=0.01)
        self.assertAlmostEqual(beta[1], 0.0479, delta=0.001)

    def test_cochrane_orcutt_stock_data(self):
        """
        ref : http://www.ats.ucla.edu/stat/stata/examples/chp/chpstata8.htm
        data : http://www.ats.ucla.edu/stat/examples/chp/p203.txt
        """
        expenditure = [
            214.6, 217.7, 219.6, 227.2, 230.9, 233.3, 234.1, 232.3, 233.7, 236.5,
            238.7, 243.2, 249.4, 254.3, 260.9, 263.3, 265.6, 268.2, 270.4, 275.6
        ]
        
        stock = [
            159.3, 161.2, 162.8, 164.6, 165.9, 167.9, 168.3, 169.7, 170.5, 171.6,
            173.9, 176.1, 178.0, 179.1, 180.2, 181.2, 181.6, 182.5, 183.3, 184.3
        ]
        
        Y = np.array(expenditure)
        regressors = np.array(stock).reshape(len(stock), 1)
        
        regARIMA = RegressionARIMA.fit_cochrane_orcutt(Y, regressors, 11, sc=self.sc)
        beta = regARIMA.regressionCoeff
        rho = regARIMA.arimaCoeff[0]
        
        self.assertAlmostEqual(rho, 0.8241, delta=0.001)
        self.assertAlmostEqual(beta[0], -235.4889, delta=0.1)
        self.assertAlmostEqual(beta[1], 2.75306, delta=0.001)
