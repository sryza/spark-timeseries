from _model import PyModel

from pyspark.mllib.common import _py2java, _java2py
from pyspark.mllib.linalg import Vectors

"""
Fits an Exponentially Weight Moving Average model (EWMA) (aka. Simple Exponential Smoothing) to
a time series. The model is defined as S_t = (1 - a) * X_t + a * S_{t - 1}, where a is the
smoothing parameter, X is the original series, and S is the smoothed series. For more
information, please see https://en.wikipedia.org/wiki/Exponential_smoothing.
"""

def fit_model(ts, sc=None):
    """
    Fits an EWMA model to a time series. Uses the first point in the time series as a starting
    value. Uses sum squared error as an objective function to optimize to find smoothing parameter
    The model for EWMA is recursively defined as S_t = (1 - a) * X_t + a * S_{t-1}, where
    a is the smoothing parameter, X is the original series, and S is the smoothed series
    Note that the optimization is performed as unbounded optimization, although in its formal
    definition the smoothing parameter is <= 1, which corresponds to an inequality bounded
    optimization. Given this, the resulting smoothing parameter should always be sanity checked
    https://en.wikipedia.org/wiki/Exponential_smoothing
    
    Parameters
    ----------
    ts:
        the time series to which we want to fit an EWMA model
        
    Returns an EWMA model
    """
    assert sc != None, "Missing SparkContext"

    jvm = sc._jvm
    jmodel = jvm.com.cloudera.sparkts.models.EWMA.fitModel(_py2java(sc, ts))
    return EWMAModel(jmodel=jmodel, sc=sc)

class EWMAModel(PyModel):
    def __init__(self, smoothing=0.0, jmodel=None, sc=None):
        assert sc != None, "Missing SparkContext"
        
        self._ctx = sc
        if jmodel == None:
            self._jmodel = self._ctx._jvm.com.cloudera.sparkts.models.EWMAModel(smoothing)
        else:
            self._jmodel = jmodel
        
        self.smoothing = self._jmodel.smoothing()
