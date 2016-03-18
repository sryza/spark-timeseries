import unittest
import numpy as np

from sparkts.test.test_utils import PySparkTestCase

from pyspark.mllib.linalg import Vectors, Matrices
from sparkts.models import AutoregressionX
from sparkts.models.AutoregressionX import ARXModel

nRows = 1000
nCols = 2
X = np.random.normal(size=(nRows, nCols))
intercept = np.random.normal() * 10

class FitAutoregressionXModelTestCase(PySparkTestCase):
    @unittest.skip("results don't match Scala right now")
    def test_predict(self):
        c = 0
        xCoeffs = [-1.136026484226831e-08, 8.637677568908233e-07,
          15238.143039368977, -7.993535860373772e-09, -5.198597570089805e-07,
          1.5691547009557947e-08, 7.409621376205488e-08]
        yMaxLag = 0
        xMaxLag = 0
        arxModel = ARXModel(c, xCoeffs, yMaxLag, xMaxLag, includesOriginalX=True, sc=self.sc)

        y = Vectors.dense([100])
        x = Matrices.dense(1, 7, [465,1,0.006562479,24,1,0,51])

        results = arxModel.predict(y, x)
        self.assertEqual(len(results), 1)
        self.assertAlmostEqual(results[0], 465, delta=0.0001)