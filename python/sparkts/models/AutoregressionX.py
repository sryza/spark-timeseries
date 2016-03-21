from . import _py2java_double_array, _nparray2breezevector, _nparray2breezematrix
from _model import PyModel

from pyspark.mllib.common import _py2java, _java2py

"""
Models a time series as a function of itself (autoregressive terms) and exogenous variables, which
are lagged up to degree xMaxLag.
"""

def fit_model(y, x, yMaxLag, xMaxLag, includesOriginalX=True, noIntercept=False, sc=None):
    """
    Fit an autoregressive model with additional exogenous variables. The model predicts a value
    at time t of a dependent variable, Y, as a function of previous values of Y, and a combination
    of previous values of exogenous regressors X_i, and current values of exogenous regressors X_i.
    This is a generalization of an AR model, which is simply an ARX with no exogenous regressors.
    The fitting procedure here is the same, using least squares. Note that all lags up to the
    maxlag are included. In the case of the dependent variable the max lag is 'yMaxLag', while
    for the exogenous variables the max lag is 'xMaxLag', with which each column in the original
    matrix provided is lagged accordingly.

    Parameters
    ----------
    y:
            the dependent variable, time series
    x:
            a matrix of exogenous variables
    yMaxLag:
            the maximum lag order for the dependent variable
    xMaxLag:
            the maximum lag order for exogenous variables
    includesOriginalX:
            a boolean flag indicating if the non-lagged exogenous variables should
            be included. Default is true
    noIntercept:
            a boolean flag indicating if the intercept should be dropped. Default is
            false
        
    Returns an ARXModel, which is an autoregressive model with exogenous variables.
    """
    assert sc != None, "Missing SparkContext"
    
    jvm = sc._jvm
    jmodel = jvm.com.cloudera.sparkts.models.AutoregressionX.fitModel(_nparray2breezevector(sc, y.toArray()), _nparray2breezematrix(sc, x.toArray()), yMaxLag, xMaxLag, includesOriginalX, noIntercept)
    return ARXModel(jmodel=jmodel, sc=sc)
    

class ARXModel(PyModel):
    """
     An autoregressive model with exogenous variables.
     
     Parameters
     ----------
     c:
        An intercept term, zero if none desired.
     coefficients:
        The coefficients for the various terms. The order of coefficients is as follows:
            - Autoregressive terms for the dependent variable, in increasing order of lag
            - For each column in the exogenous matrix (in their original order), the
              lagged terms in increasing order of lag (excluding the non-lagged versions).
            - The coefficients associated with the non-lagged exogenous matrix
     yMaxLag:
        The maximum lag order for the dependent variable.
     xMaxLag:
        The maximum lag order for exogenous variables.
     includesOriginalX:
        A boolean flag indicating if the non-lagged exogenous variables should be included.
    """
    def __init__(self, c=0.0, coefficients=[], yMaxLag=0, xMaxLag=0, includesOriginalX=True, jmodel=None, sc=None):
        assert sc != None, "Missing SparkContext"
        
        self._ctx = sc
        if jmodel == None:
            self._jmodel = self._ctx._jvm.com.cloudera.sparkts.models.ARXModel(float(c), _py2java_double_array(self._ctx, coefficients), yMaxLag, xMaxLag, includesOriginalX)
        else:
            self._jmodel = jmodel
        
        self.c = self._jmodel.c()
        self.coefficients = _java2py(self._ctx, self._jmodel.coefficients())
        self.yMaxLag = self._jmodel.yMaxLag()
        self.xMaxLag = self._jmodel.xMaxLag()
    
    def predict(self, y, x):
        prediction = self._jmodel.predict(_nparray2breezevector(self._ctx, y.toArray()), _nparray2breezematrix(self._ctx, x.toArray()))
        return _java2py(self._ctx, prediction.toArray(None))
