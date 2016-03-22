from . import _py2java_int_array, _py2java_double_array, _nparray2breezevector, _nparray2breezematrix, _py2scala_seq
from _model import PyModel

from pyspark.mllib.common import _py2java, _java2py
from pyspark.mllib.linalg import Vectors


"""
This model is basically a regression with ARIMA error structure
see
[[https://onlinecourses.science.psu.edu/stat510/node/53]]
[[https://www.otexts.org/fpp/9/1]]
[[http://robjhyndman.com/talks/RevolutionR/11-Dynamic-Regression.pdf]]
The basic idea is that for usual regression models
  Y = B*X + e
e should be IID ~ N(0,sigma^2^), but in time series problems, e tends to have time series
characteristics.
"""

def fit_model(ts, regressors, method="cochrane-orcutt", optimizationArgs=None, sc=None):
    """
    Parameters
    ----------
    ts:
        time series to which to fit an ARIMA model as a Numpy array
    regressors:
        regression matrix as a Numpy array
    method:
        Regression method. Currently, only "cochrane-orcutt" is supported.
    optimizationArgs:
        
    sc:
        The SparkContext, required.
    
    returns an RegressionARIMAModel
    """
    assert sc != None, "Missing SparkContext"
    
    jvm = sc._jvm
    
    jmodel = jvm.com.cloudera.sparkts.models.RegressionARIMA.fitModel(_nparray2breezevector(sc, ts), _nparray2breezematrix(sc, regressors), method, _py2scala_seq(sc, optimizationArgs))
    return RegressionARIMAModel(jmodel=jmodel, sc=sc)

def fit_cochrane_orcutt(ts, regressors, maxIter=10, sc=None):
    """
    Fit linear regression model with AR(1) errors , for references on Cochrane Orcutt model:
    See [[https://onlinecourses.science.psu.edu/stat501/node/357]]
    See : Applied Linear Statistical Models - Fifth Edition - Michael H. Kutner , page 492
    The method assumes the time series to have the following model
    
    Y_t = B.X_t + e_t
    e_t = rho*e_t-1+w_t
    e_t has autoregressive structure , where w_t is iid ~ N(0,&sigma 2)
    
    Outline of the method :
    1) OLS Regression for Y (timeseries) over regressors (X)
    2) Apply auto correlation test (Durbin-Watson test) over residuals , to test whether e_t still
       have auto-regressive structure
    3) if test fails stop , else update update coefficients (B's) accordingly and go back to step 1)
    
    Parameters
    ----------
    ts:
        Vector of size N for time series data to create the model for as a Numpy array
    regressors:
        Matrix N X K for the timed values for K regressors over N time points as a Numpy array
    maxIter:
        maximum number of iterations in iterative cochrane-orchutt estimation
    
    Returns instance of class [[RegressionARIMAModel]]
    """
    
    assert sc != None, "Missing SparkContext"
    
    jvm = sc._jvm
    
    fnord = _nparray2breezematrix(sc, regressors)
    print fnord
    
    jmodel = jvm.com.cloudera.sparkts.models.RegressionARIMA.fitCochraneOrcutt(_nparray2breezevector(sc, ts), _nparray2breezematrix(sc, regressors), maxIter)
    return RegressionARIMAModel(jmodel=jmodel, sc=sc)
    

class RegressionARIMAModel(PyModel):
    def __init__(self, regressionCoeff=None, d=0, q=0, coefficients=None, hasIntercept=False, jmodel=None, sc=None):
        """
        Parameters
        ----------
        regressionCoeff:
            coefficients for regression , including intercept , for example. if model has 3 regressors
            then length of [[regressionCoeff]] is 4
        arimaOrders:
            p,d,q for the arima error structure, length of [[arimaOrders]] must be 3
        arimaCoeff:
            AR, d, and MA terms, length of arimaCoeff = p+d+q
        """
        assert sc != None, "Missing SparkContext"

        self._ctx = sc
        if jmodel == None:
            self._jmodel = self._ctx._jvm.com.cloudera.sparkts.models.RegressionARIMAModel(_py2java_double_array(self._ctx, regressionCoeff), _py2java_int_array(self._ctx, arimaOrders), _py2scala_arraybuffer(self._ctx, arimaCoeff))
        else:
            self._jmodel = jmodel
            
        self.regressionCoeff = _java2py(sc, self._jmodel.regressionCoeff())
        self.arimaOrders = _java2py(sc, self._jmodel.arimaOrders())
        self.arimaCoeff = _java2py(sc, self._jmodel.arimaCoeff())
    
