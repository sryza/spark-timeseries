from . import _py2java_double_array
from _model import PyModel

from pyspark.mllib.common import _py2java, _java2py
from pyspark.mllib.linalg import Vectors


"""
ARIMA models allow modeling timeseries as a function of prior values of the series
(i.e. autoregressive terms) and a moving average of prior error terms. ARIMA models
are traditionally specified as ARIMA(p, d, q), where p is the autoregressive order,
d is the differencing order, and q is the moving average order. Using the backshift (aka
lag operator) B, which when applied to a Y returns the prior value, the
ARIMA model can be specified as
Y_t = c + \sum_{i=1}^p \phi_i*B^i*Y_t + \sum_{i=1}^q \theta_i*B^i*\epsilon_t + \epsilon_t
where Y_i has been differenced as appropriate according to order `d`
See [[https://en.wikipedia.org/wiki/Autoregressive_integrated_moving_average]] for more
information on ARIMA.
See [[https://en.wikipedia.org/wiki/Order_of_integration]] for more information on differencing
integrated time series.


"""

def autofit(ts, maxp=5, maxd=2, maxq=5, sc=None):
    """
    Utility function to help in fitting an automatically selected ARIMA model based on approximate
    Akaike Information Criterion (AIC) values. The model search is based on the heuristic
    developed by Hyndman and Khandakar (2008) and described in [[http://www.jstatsoft
    .org/v27/i03/paper]]. In contrast to the algorithm in the paper, we use an approximation to
    the AIC, rather than an exact value. Note that if the maximum differencing order provided
    does not suffice to induce stationarity, the function returns a failure, with the appropriate
    message. Additionally, note that the heuristic only considers models that have parameters
    satisfying the stationarity/invertibility constraints. Finally, note that our algorithm is
    slightly more lenient than the original heuristic. For example, the original heuristic
    rejects models with parameters "close" to violating stationarity/invertibility. We only
    reject those that actually violate it.
   
    This functionality is even less mature than some of the other model fitting functions here, so
    use it with caution.
   
    Parameters
    ----------
    ts:
        time series to which to automatically fit an ARIMA model as a Numpy array
    maxP:
        limit for the AR order
    maxD:
        limit for differencing order
    maxQ:
        limit for the MA order
    sc:
        The SparkContext, required.
    
    returns an ARIMAModel
    """
    assert sc != None, "Missing SparkContext"
    
    jmodel = sc._jvm.com.cloudera.sparkts.models.ARIMA.autoFit(_py2java(sc, Vectors.dense(ts)), maxp, maxd, maxq)
    return ARIMAModel(jmodel=jmodel, sc=sc)

def fit_model(p, d, q, ts, includeIntercept=True, method="css-cgd", userInitParams=None, sc=None):
    """
    Given a time series, fit a non-seasonal ARIMA model of order (p, d, q), where p represents
    the autoregression terms, d represents the order of differencing, and q moving average error
    terms. If includeIntercept is true, the model is fitted with an intercept. In order to select
    the appropriate order of the model, users are advised to inspect ACF and PACF plots, or compare
    the values of the objective function. Finally, while the current implementation of
    `fitModel` verifies that parameters fit stationarity and invertibility requirements,
    there is currently no function to transform them if they do not. It is up to the user
    to make these changes as appropriate (or select a different model specification)

    Parameters
    ----------
    p:
        autoregressive order
    d:
        differencing order
    q:
        moving average order
    ts:
        time series to which to fit an ARIMA(p, d, q) model as a Numpy array.
    includeIntercept:
        if true the model is fit with an intercept term. Default is true
    method:
        objective function and optimization method, current options are 'css-bobyqa',
        and 'css-cgd'. Both optimize the log likelihood in terms of the
        conditional sum of squares. The first uses BOBYQA for optimization, while
        the second uses conjugate gradient descent. Default is 'css-cgd'
    userInitParams:
        A set of user provided initial parameters for optimization as a float list.
        If null (default), initialized using Hannan-Rissanen algorithm. If provided,
        order of parameter should be: intercept term, AR parameters (in
        increasing order of lag), MA parameters (in increasing order of lag).
    sc:
        The SparkContext, required.
    
    returns an ARIMAModel
    """
    assert sc != None, "Missing SparkContext"
    
    jvm = sc._jvm
    jmodel = jvm.com.cloudera.sparkts.models.ARIMA.fitModel(p, d, q, _py2java(sc, Vectors.dense(ts)), includeIntercept, method, _py2java_double_array(sc, userInitParams))
    return ARIMAModel(jmodel=jmodel, sc=sc)

class ARIMAModel(PyModel):
    def __init__(self, p=0, d=0, q=0, coefficients=None, hasIntercept=False, jmodel=None, sc=None):
        assert sc != None, "Missing SparkContext"

        self._ctx = sc
        if jmodel == None:
            self._jmodel = self._ctx._jvm.com.cloudera.sparkts.models.ARIMAModel(p, d, q, _py2java_double_array(self._ctx, coefficients), hasIntercept)
        else:
            self._jmodel = jmodel
            
        self.p = _java2py(sc, self._jmodel.p())
        self.d = _java2py(sc, self._jmodel.d())
        self.q = _java2py(sc, self._jmodel.q())
        self.coefficients = _java2py(sc, self._jmodel.coefficients())
        self.has_intercept = _java2py(sc, self._jmodel.hasIntercept())
    
    def log_likelihood_css(self, y):
        """
        log likelihood based on conditional sum of squares
        
        Source: http://www.nuffield.ox.ac.uk/economics/papers/1997/w6/ma.pdf

        Parameters
        ----------
        y:
            time series as a DenseVector

        returns log likelihood as a double
        """
        likelihood = self._jmodel.logLikelihoodCSS(_py2java(self._ctx, y))
        return _java2py(self._ctx, likelihood)

    def log_likelihood_css_arma(self, diffedy):
        """
        log likelihood based on conditional sum of squares. In contrast to logLikelihoodCSS the array
        provided should correspond to an already differenced array, so that the function below
        corresponds to the log likelihood for the ARMA rather than the ARIMA process
        
        Parameters
        ----------
        diffedY:
            differenced array as a numpy array
        
        returns log likelihood of ARMA as a double
        """
        # need to copy diffedy to a double[] for Java
        likelihood =  self._jmodel.logLikelihoodCSSARMA(_py2java_double_array(self._ctx, diffedy))
        return _java2py(self._ctx, likelihood)
        
    def gradient_log_likelihood_css_arma(self, diffedy):
        """
        Calculates the gradient for the log likelihood function using CSS
        Derivation:
            L(y | \theta) = -\frac{n}{2}log(2\pi\sigma^2) - \frac{1}{2\pi}\sum_{i=1}^n \epsilon_t^2 \\
            \sigma^2 = \frac{\sum_{i = 1}^n \epsilon_t^2}{n} \\
            \frac{\partial L}{\partial \theta} = -\frac{1}{\sigma^2}
            \sum_{i = 1}^n \epsilon_t \frac{\partial \epsilon_t}{\partial \theta} \\
            \frac{\partial \epsilon_t}{\partial \theta} = -\frac{\partial \hat{y}}{\partial \theta} \\
            \frac{\partial\hat{y}}{\partial c} = 1 +
            \phi_{t-q}^{t-1}*\frac{\partial \epsilon_{t-q}^{t-1}}{\partial c} \\
            \frac{\partial\hat{y}}{\partial \theta_{ar_i}} =  y_{t - i} +
            \phi_{t-q}^{t-1}*\frac{\partial \epsilon_{t-q}^{t-1}}{\partial \theta_{ar_i}} \\
            \frac{\partial\hat{y}}{\partial \theta_{ma_i}} =  \epsilon_{t - i} +
            \phi_{t-q}^{t-1}*\frac{\partial \epsilon_{t-q}^{t-1}}{\partial \theta_{ma_i}} \\
        
        Parameters
        ----------
        diffedY:
            array of differenced values
        
        returns the gradient log likelihood as an array of double
        """
        # need to copy diffedy to a double[] for Java
        result =  self._jmodel.gradientlogLikelihoodCSSARMA(_py2java_double_array(self._ctx, diffedy))
        return _java2py(self._ctx, result)
    
    def sample(self, n):
        """
        Sample a series of size n assuming an ARIMA(p, d, q) process.
        
        Parameters
        ----------
        n:
            size of sample
            
        Returns a series reflecting ARIMA(p, d, q) process as a DenseVector
        """
        rg = self._ctx._jvm.org.apache.commons.math3.random.JDKRandomGenerator()
        return _java2py(self._ctx, self._jmodel.sample(n, rg))
    
    def forecast(self, ts, nfuture):
        """
        Provided fitted values for timeseries ts as 1-step ahead forecasts, based on current
        model parameters, and then provide `nFuture` periods of forecast. We assume AR terms
        prior to the start of the series are equal to the model's intercept term (or 0.0, if fit
        without and intercept term).Meanwhile, MA terms prior to the start are assumed to be 0.0. If
        there is differencing, the first d terms come from the original series.
       
        Parameters
        ----------
        ts:
            Timeseries to use as gold-standard. Each value (i) in the returning series
            is a 1-step ahead forecast of ts(i). We use the difference between ts(i) -
            estimate(i) to calculate the error at time i, which is used for the moving
            average terms.
        nFuture:
            Periods in the future to forecast (beyond length of ts)
            
        Returns a series consisting of fitted 1-step ahead forecasts for historicals and then
        `nFuture` periods of forecasts. Note that in the future values error terms become
        zero and prior predictions are used for any AR terms.
        
        """
        jts = _py2java(self._ctx, ts)
        jfore = self._jmodel.forecast(jts, nfuture)
        return _java2py(self._ctx, jfore)
    
    def is_stationary(self):
        """
        Check if the AR parameters result in a stationary model. This is done by obtaining the roots
        to the polynomial 1 - phi_1 * x - phi_2 * x^2 - ... - phi_p * x^p = 0, where phi_i is the
        corresponding AR parameter. Note that we check the roots, not the inverse of the roots, so
        we check that they lie outside of the unit circle.
        Always returns true for models with no AR terms.
        See http://www.econ.ku.dk/metrics/Econometrics2_05_II/Slides/07_univariatetimeseries_2pp.pdf
        for more information (specifically slides 23 - 25)
        
        Returns true if the model's AR parameters are stationary
        """
        return self._jmodel.isStationary()
    
    def is_invertible(self):
        """
        Checks if MA parameters result in an invertible model. Checks this by solving the roots for
        1 + theta_1 * x + theta_2 * x + ... + theta_q * x&#94;q = 0. Please see
        [[ARIMAModel.isStationary]] for more details.
        Always returns true for models with no MA terms.
        
        Returns true if the model's MA parameters are invertible
        """
        return self._jmodel.isInvertible()
    
    def approx_aic(self, ts):
        """
        Calculates an approximation to the Akaike Information Criterion (AIC). This is an approximation
        as we use the conditional likelihood, rather than the exact likelihood. Please see
        [[https://en.wikipedia.org/wiki/Akaike_information_criterion]] for more information on this
        measure.
        
        Parameters
        ----------
        ts:
            the timeseries to evaluate under current model
            
        Returns an approximation to the AIC under the current model as a double
        """
        return self._jmodel.approxAIC(_py2java(self._ctx, ts))
