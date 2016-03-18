import numpy as np

from sparkts.test.test_utils import PySparkTestCase

from pyspark.mllib.linalg import Vectors
from sparkts.models import GARCH, ARGARCH
from sparkts.models.GARCH import GARCHModel
from sparkts.models.ARGARCH import ARGARCHModel

class FitGARCHModelTestCase(PySparkTestCase):
    def test_log_likelihood(self):
        model = GARCHModel(0.2, 0.3, 0.4, sc=self.sc)
        n = 10000
        
        ts = Vectors.dense(model.sample(n))
        logLikelihoodWithRightModel = model.log_likelihood(ts)
        
        logLikelihoodWithWrongModel1 = GARCHModel(.3, .4, .5, sc=self.sc).log_likelihood(ts)
        logLikelihoodWithWrongModel2 = GARCHModel(.25, .35, .45, sc=self.sc).log_likelihood(ts)
        logLikelihoodWithWrongModel3 = GARCHModel(.1, .2, .3, sc=self.sc).log_likelihood(ts)

        self.assertTrue(logLikelihoodWithRightModel > logLikelihoodWithWrongModel1)
        self.assertTrue(logLikelihoodWithRightModel > logLikelihoodWithWrongModel2)
        self.assertTrue(logLikelihoodWithRightModel > logLikelihoodWithWrongModel3)
        self.assertTrue(logLikelihoodWithWrongModel2 > logLikelihoodWithWrongModel1)
    
    def test_gradient(self):
        alpha = 0.3
        beta = 0.4
        omega = 0.2
        genModel = GARCHModel(omega, alpha, beta, sc=self.sc)
        n = 10000
    
        ts = Vectors.dense(genModel.sample(n))
    
        gradient1 = GARCHModel(omega + .1, alpha + .05, beta + .1, sc=self.sc).gradient(ts)
        for g in gradient1:
            self.assertTrue(g < 0.0)
        
        gradient2 = GARCHModel(omega - .1, alpha - .05, beta - .1, sc=self.sc).gradient(ts)
        for g in gradient2:
            self.assertTrue(g > 0.0)
    
    def test_fit_model(self):
        omega = 0.2
        alpha = 0.3
        beta = 0.5
        genModel = ARGARCHModel(0.0, 0.0, alpha, beta, omega, sc=self.sc)
        
        n = 10000
        ts = Vectors.dense(genModel.sample(n))
        
        model = GARCH.fit_model(ts, sc=self.sc)
        # tolerances are a little bit larger than Scala; not certain why... -pame
        self.assertAlmostEqual(model.omega, omega, delta=0.1)
        self.assertAlmostEqual(model.alpha, alpha, delta=0.1)
        self.assertAlmostEqual(model.beta, beta, delta=0.2)

    def test_fit_model(self):
        ts = Vectors.dense([
            0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,
            0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,
            -0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,
            -0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,
            -0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,
            0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,
            0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,
            0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,
            -0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,
            0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,
            -0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,
            -0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,
            -0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,
            0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,
            0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,
            -0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,
            -0.1,0.1,0.0,-0.01,0.00,-0.1,0.1,-0.2,-0.1,0.1,0.0,-0.01,0.00,-0.1
        ])
        model = ARGARCH.fit_model(ts, sc=self.sc)
        print("alpha: %f" % (model.alpha))
        print("beta: %f" % (model.beta))
        print("omega: %f" % (model.omega))
        print("c: %f" % (model.c))
        print("phi: %f" % (model.phi))

    def test_standardize_and_filter(self):
        model = ARGARCHModel(40.0, 0.4, 0.2, 0.3, 0.4, sc=self.sc)
        n = 10000
        ts = Vectors.dense(model.sample(n))
        
        # de-heteroskedasticize
        standardized = model.remove_time_dependent_effects(ts)
        filtered = model.add_time_dependent_effects(standardized)
        for i in range(len(filtered)):
            self.assertAlmostEquals(filtered[i], ts[i], msg="%f != %f at index %d" % (filtered[i], ts[i], i))
