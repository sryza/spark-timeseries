from pyspark.mllib.linalg import Vectors
from pyspark.mllib.common import _py2java, _java2py

class PyModel(object):
    def remove_time_dependent_effects(self, ts):
        """
        Given a timeseries, assume that it is the result of an ARIMA(p, d, q) process, and apply
        inverse operations to obtain the original series of underlying errors.
        To do so, we assume prior MA terms are 0.0, and prior AR are equal to the model's intercept or
        0.0 if fit without an intercept
        
        Parameters
        ----------
        ts:
            Time series of observations with this model's characteristics as a DenseVector
        
        returns the time series with removed time-dependent effects as a DenseVector
        """
        destts = Vectors.dense([0] * len(ts))
        result =  self._jmodel.removeTimeDependentEffects(_py2java(self._ctx, ts), _py2java(self._ctx, destts))
        return Vectors.dense(_java2py(self._ctx, result))
    
    def add_time_dependent_effects(self, ts):
        """
        Given a timeseries, apply an ARIMA(p, d, q) model to it.
        We assume that prior MA terms are 0.0 and prior AR terms are equal to the intercept or 0.0 if
        fit without an intercept
        
        Parameters
        ----------
        ts:
            Time series of i.i.d. observations as a DenseVector
        
        returns the time series with added time-dependent effects as a DenseVector.
        """
        destts = Vectors.dense([0] * len(ts))
        result =  self._jmodel.addTimeDependentEffects(_py2java(self._ctx, ts), _py2java(self._ctx, destts))
        return Vectors.dense(_java2py(self._ctx, result))
