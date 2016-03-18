from _model import PyModel

from pyspark.mllib.common import _py2java, _java2py
from pyspark.mllib.linalg import Vectors

"""
"""

def fit_model(ts, sc=None):
    """
    Fits an AR(1) + GARCH(1, 1) model to the given time series.
    
    Parameters
    ----------
    ts:
        the time series to which we want to fit a AR+GARCH model
        
    Returns an ARGARCH model
    """
    assert sc != None, "Missing SparkContext"
    
    jvm = sc._jvm
    jmodel = jvm.com.cloudera.sparkts.models.ARGARCH.fitModel(_py2java(sc, ts))
    return ARGARCHModel(jmodel=jmodel, sc=sc)

class ARGARCHModel(PyModel):
    def __init__(self, c=0.0, phi=0.0, omega=0.0, alpha=0.0, beta=0.0, jmodel=None, sc=None):
        assert sc != None, "Missing SparkContext"
        
        self._ctx = sc
        if jmodel == None:
            self._jmodel = self._ctx._jvm.com.cloudera.sparkts.models.ARGARCHModel(c, phi, omega, alpha, beta)
        else:
            self._jmodel = jmodel
        
        self.c = self._jmodel.c()
        self.phi = self._jmodel.phi()
        self.omega = self._jmodel.omega()
        self.alpha = self._jmodel.alpha()
        self.beta = self._jmodel.beta()
    
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
    
    def sample_with_variances(self):
        rg = self._ctx._jvm.org.apache.commons.math3.random.JDKRandomGenerator()
        return _java2py(self._ctx, self._jmodel.sampleWithVariances(n, rg))
        
