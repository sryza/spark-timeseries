from . import _py2java_double_array
from _model import PyModel

from pyspark.mllib.common import _py2java, _java2py

def fit_model(ts, maxLag=1, noIntercept=False, sc=None):
    """
    Fits an AR(1) model to the given time series
    
    Parameters
    ----------
    ts:
        the time series to which we want to fit an autoregression model
    """
    assert sc != None, "Missing SparkContext"
    
    jvm = sc._jvm
    jmodel = jvm.com.cloudera.sparkts.models.Autoregression.fitModel(_py2java(sc, ts), maxLag, noIntercept)
    return ARModel(jmodel=jmodel, sc=sc)


class ARModel(PyModel):
    def __init__(self, c=0, coefficients=None, jmodel=None, sc=None):
        assert sc != None, "Missing SparkContext"
        
        self._ctx = sc
        if jmodel == None:
            self._jmodel = self._ctx._jvm.com.cloudera.sparkts.models.ARModel(c, _py2java_double_array(coefficients, self._ctx._gateway))
        else:
            self._jmodel = jmodel
        
        self.c = self._jmodel.c()
        self.coefficients = _java2py(self._ctx, self._jmodel.coefficients())

    def sample(self, n):
        rg = self._ctx._jvm.org.apache.commons.math3.random.JDKRandomGenerator()
        return _java2py(self._ctx, self._jmodel.sample(n, rg))
