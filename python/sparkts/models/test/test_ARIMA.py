from sparkts.test.test_utils import PySparkTestCase

from pyspark.mllib.linalg import Vectors
from sparkts.models import ARIMA


class FitModelTestCase(PySparkTestCase):
    def compare_with_r(self):
        data = data_file_as_dense_vector('resources/R_ARIMA_DataSet1.csv')
        model = ARIMA.fitModel(1, 0, 1, data, sc=sc)
        (c, ar, ma) = model.coefficients
        self.assertAlmostEqual(ar, 0.3)
        self.assertAlmostEqual(ma, 0.7)
    
    def remodel_sample_data(self):
        """
        Data sampled from a given model should result in a similar model if fit again.
        """
        model = ARIMAModel(2, 1, 2, Vectors.dense([8.2, 0.2, 0.5, 0.3, 0.1]))
        sampled = model.sample(1000)
        newModel = ARIMA.fitModel(2, 1, 2, sampled)
        (c, ar1, ar2, ma1, ma2) = model.coefficients
        (cTest, ar1Test, ar2Test, ma1Test, ma2Test) = newModel.coefficients
        self.assertAlmostEqual(c, cTest, delta=1)
        self.assertAlmostEqual(ar1, ar1Test, delta=0.1)
        self.assertAlmostEqual(ar2, ar2Test, delta=0.1)
        self.assertAlmostEqual(ma1, ma1Test, delta=0.1)
        self.assertAlmostEqual(ma2, ma2Test, delta=0.1)
        
    def stationarity_and_invertability(self):
        model1 = ARIMAModel(1, 0, 0, Array(0.2, 1.5), hasIntercept = true)
        self.assertFalse(model1.is_stationary())
        self.assertTrue(model1.is_invertable())

        model2 = ARIMAModel(0, 0, 1, Array(0.13, 1.8), hasIntercept = true)
        self.assertTrue(model2.is_stationary())
        self.assertFalse(model2.is_invertable())
        
        model3 = ARIMAModel(2, 0, 0, Array(0.003359, 1.545, -0.5646), hasIntercept = true)
        self.assertTrue(model3.is_stationary())
        self.assertTrue(model3.is_invertable())
        
        model4 = ARIMAModel(1, 0, 1, Array(-0.09341,  0.857361, -0.300821), hasIntercept = true)
        self.assertTrue(model4.is_stationary())
        self.assertTrue(model4.is_invertable())
