from _model import PyModel

from pyspark.mllib.common import _py2java, _java2py
from pyspark.mllib.linalg import Vectors

"""
"""

def fit_model(ts, sc=None):
    """
    Fits a GARCH(1, 1) model to the given time series.
    
    Parameters
    ----------
    ts:
        the time series to which we want to fit a GARCH model as a Numpy array
        
    Returns a GARCH model
    """
    assert sc != None, "Missing SparkContext"
    
    jvm = sc._jvm
    jmodel = jvm.com.cloudera.sparkts.models.GARCH.fitModel(_py2java(sc, Vectors.dense(ts)))
    return GARCHModel(jmodel=jmodel, sc=sc)

class GARCHModel(PyModel):
    def __init__(self, omega=0.0, alpha=0.0, beta=0.0, jmodel=None, sc=None):
        assert sc != None, "Missing SparkContext"
        
        self._ctx = sc
        if jmodel == None:
            self._jmodel = self._ctx._jvm.com.cloudera.sparkts.models.GARCHModel(omega, alpha, beta)
        else:
            self._jmodel = jmodel
        
        self.omega = self._jmodel.omega()
        self.alpha = self._jmodel.alpha()
        self.beta = self._jmodel.beta()

    def gradient(self, ts):
        """
        Find the gradient of the log likelihood with respect to the given time series.
        
        Based on http://www.unc.edu/~jbhill/Bollerslev_GARCH_1986.pdf
        
        Returns an 3-element array containing the gradient for the alpha, beta, and omega parameters.
        """
        gradient = self._jmodel.gradient(_py2java(self._ctx, Vectors.dense(ts)))
        return _java2py(self._ctx, gradient)
    
    def log_likelihood(self, ts):
        """
        Returns the log likelihood of the parameters on the given time series.
        
        Based on http://www.unc.edu/~jbhill/Bollerslev_GARCH_1986.pdf
        """
        likelihood = self._jmodel.logLikelihood(_py2java(self._ctx, Vectors.dense(ts)))
        return _java2py(self._ctx, likelihood)
    
    def sample(self, n):
        """
        Samples a random time series of a given length with the properties of the model.
        
        Parameters
        ----------
        n:
            The length of the time series to sample.
        
        Returns the sampled time series.
        """
        rg = self._ctx._jvm.org.apache.commons.math3.random.JDKRandomGenerator()
        return _java2py(self._ctx, self._jmodel.sample(n, rg))
