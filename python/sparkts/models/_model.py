import numpy as np

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.common import _py2java, _java2py

class PyModel(object):
    def remove_time_dependent_effects(self, ts):
        """
        Given a timeseries, apply inverse operations to obtain the original series of underlying errors.
        Parameters
        ----------
        ts:
            Time series of observations with this model's characteristics as a Numpy array
        
        returns the time series with removed time-dependent effects as a Numpy array
        """
        destts = Vectors.dense(np.array([0] * len(ts)))
        result =  self._jmodel.removeTimeDependentEffects(_py2java(self._ctx, Vectors.dense(ts)), _py2java(self._ctx, destts))
        return _java2py(self._ctx, result.toArray())
    
    def add_time_dependent_effects(self, ts):
        """
        Given a timeseries, apply a model to it.
        
        Parameters
        ----------
        ts:
            Time series of i.i.d. observations as a Numpy array
        
        returns the time series with added time-dependent effects as a Numpy array.
        """
        destts = Vectors.dense([0] * len(ts))
        result =  self._jmodel.addTimeDependentEffects(_py2java(self._ctx, Vectors.dense(ts)), _py2java(self._ctx, destts))
        return _java2py(self._ctx, result.toArray())
