/**
  * Copyright (c) 2016, Cloudera, Inc. All Rights Reserved.
  *
  * Cloudera, Inc. licenses this file to you under the Apache License,
  * Version 2.0 (the "License"). You may not use this file except in
  * compliance with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
  * CONDITIONS OF ANY KIND, either express or implied. See the License for
  * the specific language governing permissions and limitations under the
  * License.
  */

package com.cloudera.sparkts.models

import breeze.linalg.{Matrix, diag, sum, DenseMatrix => BreezeDenseMatrix, DenseVector => BreezeDenseVector}
import com.cloudera.sparkts.MatrixUtil
import com.cloudera.sparkts.MatrixUtil.{dvBreezeToSpark, mBreezeToSpark, toBreeze}
import com.cloudera.sparkts.UnivariateTimeSeries.{differencesOfOrderD, inverseDifferencesOfOrderD}
import org.apache.commons.math3.analysis.{MultivariateFunction, MultivariateVectorFunction}
import org.apache.commons.math3.optim.{InitialGuess, MaxEval, MaxIter, SimpleValueChecker}
import org.apache.commons.math3.optim.nonlinear.scalar.{GoalType, ObjectiveFunction, ObjectiveFunctionGradient}
import org.apache.commons.math3.optim.nonlinear.scalar.gradient.NonLinearConjugateGradientOptimizer
import org.apache.spark.mllib.linalg.{DenseVector, Vector}

/**
  * The autoregressive integrated moving average with explanatory variables (ARIMAX)
  * Y_{t} = Beta*X_{t} + ARIMA_MODEL
  * Link [[http://support.sas.com/resources/papers/proceedings16/11823-2016.pdf]]
  */

object ARIMAX {

  /**
    * Given a time series, fit a non-seasonal ARIMAX model of order (p, d, q), where p represents
    * the autoregression terms, d represents the order of differencing, q moving average error
    * terms, xMaxLag the maximum lag order for exogenous variables. If includeOriginalXreg is true, the
    * model is fitted with the non-lagged exogenous variables. If includeIntercept is true, the model is
    * fitted with an intercept. In order to select the appropriate order of the model, users are advised
    * to inspect ACF and PACF plots, or compare the values of the objective function.
    *
    * @param p                   Autoregressive order
    * @param d                   Differencing order
    * @param q                   Moving average order
    * @param ts                  Time series to which to fit an ARIMAX(p, d, q) model
    * @param xreg                A matrix of exogenous variables
    * @param xregMaxLag          The maximum lag order for the dependent variable
    * @param includeOriginalXreg A boolean flag indicating if the non-lagged exogenous variables should
    *                            be included. Default is true.
    * @param includeIntercept    If true the model is fit with an intercept term. Default is true
    * @param userInitParams      A set of user provided initial parameters for optimization. If null
    *                            (default), initialized using Hannan-Rissanen algorithm. If provided,
    *                            order of parameter should be: intercept term, AR parameters (in
    *                            increasing order of lag), MA parameters (in increasing order of lag) and
    *                            coefficients for exogenous variables (xregMaxLag + 1) for each column.
    */
  def fitModel(
      p: Int,
      d: Int,
      q: Int,
      ts: Vector,
      xreg: Matrix[Double],
      xregMaxLag: Int,
      includeOriginalXreg: Boolean = true,
      includeIntercept: Boolean = true,
      userInitParams: Option[Array[Double]] = None): ARIMAXModel = {

    val differentialTs = differencesOfOrderD(ts, d).toArray.drop(d)

    val initParams = userInitParams match {
      case Some(defined) => defined
      case None => {
        val arx = estimateARXCoefficients(ts, xreg, p, d, xregMaxLag, includeOriginalXreg, includeIntercept)
        var ma = estimateMACoefficients(p, q, differentialTs, includeIntercept)

        arx.take(p + 1) ++ ma ++ arx.drop(p + 1)
      }
    }
    // Fit/smooth params using CGD
    val params = fitWithCSSCGD(
      p, d, q, xregMaxLag, differentialTs, includeOriginalXreg, includeIntercept, initParams)

    val model = new ARIMAXModel(p, d, q, xregMaxLag, params, includeOriginalXreg, includeIntercept)
    model
  }

  private def estimateARXCoefficients(
              ts: Vector,
              xreg: Matrix[Double],
              p: Int,
              d: Int,
              xregMaxLag: Int,
              includeOriginalXreg: Boolean,
              includeIntercept: Boolean): Array[Double] = {

    // [Time series data and exogenous values differentiate (http://robjhyndman.com/hyndsight/arimax/)
    val tsVector = new BreezeDenseVector(ts.toArray)
    val xregVector = new BreezeDenseVector(xreg.toArray)
    val differentialXregVector = new BreezeDenseVector(differencesOfOrderD(xregVector, d).toArray)
    val differentialXregMatrix = new BreezeDenseMatrix(xreg.rows, xreg.cols, differentialXregVector.toArray)

    // Use AutoregressionX model to get covariances and AR(n) coefficients
    val arxModel = AutoregressionX.fitModel(tsVector, differentialXregMatrix, p, xregMaxLag, includeOriginalXreg)
    val c = if (includeIntercept) Array(arxModel.c) else Array(0.0)

    c ++ arxModel.coefficients
  }

  private def estimateMACoefficients(
              p: Int,
              q: Int,
              differentialTs: Array[Double],
              includeIntercept: Boolean): Array[Double] = {
    ARIMA.hannanRissanenInit(p, q, differentialTs, includeIntercept).takeRight(q)
  }

  /**
    * Fit an ARIMAX model using conditional sum of squares estimator, optimized using conjugate
    * gradient descent. Most of the code is copied from ARIMA fitWithCSSCGD method.
    *
    * @param p                   Autoregressive order
    * @param d                   Differencing order
    * @param q                   Moving average order
    * @param xregMaxLag          The maximum lag order for exogenous variables
    * @param diffedY             Differenced time series, as appropriate
    * @param includeOriginalXreg A boolean flag indicating if the non-lagged exogenous variables should be included. Default is true.
    * @param includeIntercept    A boolean to indicate if the regression should be run without an intercept
    * @param initParams          Initial parameter guess for optimization
    * @return Parameters optimized using CSS estimator, with method conjugate gradient descent
    */
  private def fitWithCSSCGD(
                             p: Int,
                             d: Int,
                             q: Int,
                             xregMaxLag: Int,
                             diffedY: Array[Double],
                             includeOriginalXreg: Boolean,
                             includeIntercept: Boolean,
                             initParams: Array[Double]): Array[Double] = {

    val optimizer = new NonLinearConjugateGradientOptimizer(
      NonLinearConjugateGradientOptimizer.Formula.FLETCHER_REEVES,
      new SimpleValueChecker(1e-7, 1e-7))

    val objFunction = new ObjectiveFunction(new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        new ARIMAXModel(p, d, q, xregMaxLag, params, includeOriginalXreg, includeIntercept).logLikelihoodCSSARMA(diffedY)
      }
    })

    val gradient = new ObjectiveFunctionGradient(new MultivariateVectorFunction() {
      def value(params: Array[Double]): Array[Double] = {
        new ARIMAXModel(p, d, q, xregMaxLag, params, includeOriginalXreg, includeIntercept).gradientlogLikelihoodCSSARMA(diffedY)
      }
    })
    val (initialGuess, maxIter, maxEval, goal) = (new InitialGuess(initParams), new MaxIter(10000), new MaxEval(10000), GoalType.MAXIMIZE)
    val optimal = optimizer.optimize(objFunction, gradient, goal, initialGuess, maxIter, maxEval)
    optimal.getPoint
  }
}

/**
  * An autoregressive moving average model with exogenous variables.
  *
  * @param p                   Autoregressive order
  * @param d                   Differencing order
  * @param q                   Moving average order
  * @param xregMaxLag          The maximum lag order for exogenous variables.
  * @param coefficients        The coefficients for the various terms. The order of coefficients is as
  *                            follows:
  *                            - Intercept
  *                            - Autoregressive terms for the dependent variable, in increasing order of lag
  *                            - Moving-average terms for the dependent variable, in increasing order of lag
  *                            - For each column in the exogenous matrix (in their original order), the
  *                            lagged terms in increasing order of lag (excluding the non-lagged versions).
  * @param includeOriginalXreg A boolean flag indicating if the non-lagged exogenous variables should be included.
  * @param includeIntercept    A boolean to indicate if the regression should be run without an intercept
  */
class ARIMAXModel(
                   val p: Int,
                   val d: Int,
                   val q: Int,
                   val xregMaxLag: Int,
                   val coefficients: Array[Double],
                   val includeOriginalXreg: Boolean = true,
                   val includeIntercept: Boolean = true) extends TimeSeriesModel {

  /**
    * Provide fitted values for timeseries ts as 1-step ahead forecasts, based on current
    * model parameters, and then provide `nFuture` periods of forecast. We assume AR terms
    * prior to the start of the series are equal to the model's intercept term (or 0.0, if fit
    * without and intercept term).Meanwhile, MA terms prior to the start are assumed to be 0.0. If
    * there is differencing, the first d terms come from the original series.
    *
    * @param timeSeries The time series
    * @param xreg       A matrix of exogenous variables
    * @return A series consisting of fitted forecasts for timeseries value using exogenous variables.
    */
  def forecast(timeSeries: BreezeDenseVector[Double], xreg: Matrix[Double]): BreezeDenseVector[Double] = {
    val ts = new DenseVector(timeSeries.toArray)
    val nFuture: Int = xreg.rows
    val maxLag = math.max(p, q)
    val intercept = if (includeIntercept) coefficients(0) else 0.0

    // [1] Differentiate exogenous variables and time series
    val differentialXreg = differenceXreg(xreg)
    val differentialTs = new DenseVector(differencesOfOrderD(ts, d).toArray.drop(d))
    val differentialTsExtended = new DenseVector(Array.fill(maxLag)(intercept) ++ differentialTs.toArray)

    // [2] Create a vector for history values and calculate them
    val historyLen = differentialTsExtended.size
    val history = new BreezeDenseVector[Double](Array.fill(historyLen)(0.0))
    iterateARMAX(differentialTsExtended, history, _ + _, goldStandard = differentialTsExtended, exogenousVar = differentialXreg)

    // [3] Take last history value as moving-average term
    val maTerms = (for (i <- historyLen - maxLag until historyLen) yield {
      differentialTsExtended(i) - history(i)
    }).toArray

    // [4] Create a vector for predicted values and calculate predictors value
    val forward = new BreezeDenseVector[Double](Array.fill(nFuture + maxLag)(0.0))

    forward(0 until maxLag) := history(-maxLag to -1)
    iterateARMAX(forward, forward, _ + _, goldStandard = forward, initMATerms = maTerms, exogenousVar = differentialXreg)

    // [5] Create a vector for results - predicted time-series values
    val results = new BreezeDenseVector[Double](Array.fill(ts.size + nFuture)(0.0))
    results(0 until d) := toBreeze(ts)(0 until d)

    // [6] Fill up the results by history and future predictor's values
    results(d until d + historyLen - maxLag) := history(maxLag to -1)
    results(ts.size to -1) := forward(maxLag to -1)

    // [7] Differentiate predictor's values
    if (d != 0) {
      val differentialMatrix = new BreezeDenseMatrix[Double](d + 1, ts.size)
      differentialMatrix(0, ::) := toBreeze(ts).t
      for (i <- 1 to d) {
        differentialMatrix(i, i to -1) := toBreeze(differencesOfOrderD(differentialMatrix(i - 1, i to -1).t, 1)).t
      }
      for (i <- d until historyLen - maxLag) {
        results(i) = sum(differentialMatrix(0 until d, i - 1)) + history(maxLag + i)
      }
      val prevTermsForForwardInverse = diag(differentialMatrix(0 until d, -d to -1))
      val forwardIntegrated = inverseDifferencesOfOrderD(
        BreezeDenseVector.vertcat(prevTermsForForwardInverse, forward(maxLag to -1)),
        d
      )
      results(-(d + nFuture) to -1) := toBreeze(forwardIntegrated)
    }

    val prediction = results.toArray.drop(nFuture)
    new BreezeDenseVector(prediction)
  }

  /**
    * log likelihood based on conditional sum of squares. In contrast to logLikelihoodCSS the array
    * provided should correspond to an already differenced array, so that the function below
    * corresponds to the log likelihood for the ARMA rather than the ARIMA process
    *
    * @param diffedY differenced array
    * @return log likelihood of ARMA
    */
  def logLikelihoodCSSARMA(diffedY: Array[Double]): Double = {
    val n = diffedY.length
    val yHat = new BreezeDenseVector(Array.fill(n)(0.0))
    val yVec = new DenseVector(diffedY)

    iterateARMA(yVec, yHat, _ + _, goldStandard = yVec)

    val maxLag = math.max(p, q)
    // drop first maxLag terms, since we can't estimate residuals there, since no
    // AR(n) terms available
    val css = diffedY.zip(yHat.toArray).drop(maxLag).map { case (obs, pred) =>
      math.pow(obs - pred, 2)
    }.sum
    val sigma2 = css / n
    (-n / 2) * math.log(2 * math.Pi * sigma2) - css / (2 * sigma2)
  }

  /**
    * Calculates the gradient for the log likelihood function using CSS
    * Derivation:
    * L(y | \theta) = -\frac{n}{2}log(2\pi\sigma^2) - \frac{1}{2\pi}\sum_{i=1}^n \epsilon_t^2 \\
    * \sigma^2 = \frac{\sum_{i = 1}^n \epsilon_t^2}{n} \\
    * \frac{\partial L}{\partial \theta} = -\frac{1}{\sigma^2}
    * \sum_{i = 1}^n \epsilon_t \frac{\partial \epsilon_t}{\partial \theta} \\
    * \frac{\partial \epsilon_t}{\partial \theta} = -\frac{\partial \hat{y}}{\partial \theta} \\
    * \frac{\partial\hat{y}}{\partial c} = 1 +
    * \phi_{t-q}^{t-1}*\frac{\partial \epsilon_{t-q}^{t-1}}{\partial c} \\
    * \frac{\partial\hat{y}}{\partial \theta_{ar_i}} =  y_{t - i} +
    * \phi_{t-q}^{t-1}*\frac{\partial \epsilon_{t-q}^{t-1}}{\partial \theta_{ar_i}} \\
    * \frac{\partial\hat{y}}{\partial \theta_{ma_i}} =  \epsilon_{t - i} +
    * \phi_{t-q}^{t-1}*\frac{\partial \epsilon_{t-q}^{t-1}}{\partial \theta_{ma_i}} \\
    *
    * @param diffedY array of differenced values
    * @return
    */
  def gradientlogLikelihoodCSSARMA(diffedY: Array[Double]): Array[Double] = {
    val n = diffedY.length
    // fitted
    val yHat = new BreezeDenseVector[Double](Array.fill(n)(0.0))
    // reference values (i.e. gold standard)
    val yRef = new DenseVector(diffedY)
    val maxLag = math.max(p, q)
    val intercept = if (includeIntercept) 1 else 0
    val maTerms = Array.fill(q)(0.0)

    // matrix of error derivatives at time t - 0, t - 1, ... t - q
    val dEdTheta = new BreezeDenseMatrix[Double](q + 1, coefficients.length)
    val gradient = new BreezeDenseVector[Double](Array.fill(coefficients.length)(0.0))

    // error-related
    var error = 0.0
    var sigma2 = 0.0

    // iteration-related
    var i = maxLag
    var j = 0
    var k = 0

    while (i < n) {
      j = 0
      // initialize partial error derivatives in each iteration to weighted average of
      // prior partial error derivatives, using moving average coefficients
      while (j < coefficients.length) {
        k = 0
        while (k < q) {
          dEdTheta(0, j) -= coefficients(intercept + p + k) * dEdTheta(k + 1, j)
          k += 1
        }
        j += 1
      }
      // intercept
      j = 0
      yHat(i) += intercept * coefficients(j)
      dEdTheta(0, 0) -= intercept

      // autoregressive terms
      while (j < p && i - j - 1 >= 0) {
        yHat(i) += yRef(i - j - 1) * coefficients(intercept + j)
        dEdTheta(0, intercept + j) -= yRef(i - j - 1)
        j += 1
      }

      // moving average terms
      j = 0
      while (j < q) {
        yHat(i) += maTerms(j) * coefficients(intercept + p + j)
        dEdTheta(0, intercept + p + j) -= maTerms(j)
        j += 1
      }

      error = yRef(i) - yHat(i)
      sigma2 += math.pow(error, 2) / n
      updateMAErrors(maTerms, error)
      // update gradient
      gradient :+= (dEdTheta(0, ::) * error).t
      // shift back error derivatives to make room for next period
      dEdTheta(1 to -1, ::) := dEdTheta(0 to -2, ::)
      // reset latest partial error derivatives to 0
      dEdTheta(0, ::) := 0.0
      i += 1
    }

    gradient := gradient :/ -sigma2
    gradient.toArray
  }

  /**
    * Updates the error vector in place with a new (more recent) error
    * The newest error is placed in position 0, while older errors "fall off the end"
    *
    * @param maTerms  array of errors of length q in ARIMAX(p, d, q), holds errors for t-1 through t-q
    * @param newError the error at time t
    * @return a modified array with the latest error placed into index 0
    */
  private def updateMAErrors(maTerms: Array[Double], newError: Double): Unit = {
    val n = maTerms.length
    var i = 0
    while (i < n - 1) {
      maTerms(i + 1) = maTerms(i)
      i += 1
    }
    if (n > 0) {
      maTerms(0) = newError
    }
  }

  /**
    * Perform operations with the AR and MA terms, based on the time series `ts` and the errors
    * based off of `goldStandard` or `errors`, combined with elements from the series `history`.
    * Weights for terms are taken from the current model configuration.
    * So for example: iterateARMA(series1, series_of_zeros,  _ + _ , goldStandard = series1,
    * initErrors = null)
    * calculates the 1-step ahead ARMA forecasts for series1 assuming current coefficients, and
    * initial MA errors of 0.
    *
    * @param ts           Time series to use for AR terms
    * @param history      Time series holding initial values at each index
    * @param op           Operation to perform between values in dest, and various combinations of ts, errors
    *                     and intercept terms
    * @param goldStandard The time series to which to compare 1-step ahead forecasts to obtain
    *                     moving average errors. Default set to null. In which case, we check if
    *                     errors are directly provided. Either goldStandard or errors can be null,
    *                     but not both
    * @param errors       The time series of errors to be used as error at time i, which is then used
    *                     for moving average terms in future indices. Either goldStandard or errors can
    *                     be null, but not both
    * @param initMATerms  Initialization for first q terms of moving average. If none provided (i.e.
    *                     remains null, as per default), then initializes to all zeros
    * @return the time series resulting from the interaction of the parameters with the model's
    *         coefficients
    */
  private def iterateARMA(
                           ts: Vector,
                           history: BreezeDenseVector[Double],
                           op: (Double, Double) => Double,
                           goldStandard: Vector = null,
                           errors: Vector = null,
                           initMATerms: Array[Double] = null): BreezeDenseVector[Double] = {
    require(goldStandard != null || errors != null, "goldStandard or errors must be passed in")

    val maTerms = if (initMATerms == null) Array.fill(q)(0.0) else initMATerms
    val intercept = if (includeIntercept) 1 else 0
    // maximum lag
    var i = math.max(p, q)
    var j = 0
    val n = ts.size
    var error = 0.0

    while (i < n) {
      j = 0
      // intercept
      history(i) = op(history(i), intercept * coefficients(j))
      // autoregressive terms
      while (j < p && i - j - 1 >= 0) {
        history(i) = op(history(i), ts(i - j - 1) * coefficients(intercept + j))
        j += 1
      }
      // moving average terms
      j = 0
      while (j < q) {
        history(i) = op(history(i), maTerms(j) * coefficients(intercept + p + j))
        j += 1
      }

      error = if (goldStandard == null) errors(i) else goldStandard(i) - history(i)
      updateMAErrors(maTerms, error)
      i += 1
    }
    history
  }

  /**
    * Perform operations with the ARX and MA terms, based on the time series `ts` and the errors
    * based off of `goldStandard` or `errors`, combined with elements from the series `history`.
    * Weights for terms are taken from the current model configuration.
    *
    * @param ts           Time series to use for ARX terms
    * @param history      Time series holding initial values at each index
    * @param op           Operation to perform between values in dest, and various combinations of ts, errors
    *                     and intercept terms
    * @param goldStandard The time series to which to compare 1-step ahead forecasts to obtain
    *                     moving average errors. Default set to null. In which case, we check if
    *                     errors are directly provided. Either goldStandard or errors can be null,
    *                     but not both
    * @param errors       The time series of errors to be used as error at time i, which is then used
    *                     for moving average terms in future indices. Either goldStandard or errors can
    *                     be null, but not both
    * @param initMATerms  Initialization for first q terms of moving average. If none provided (i.e.
    *                     remains null, as per default), then initializes to all zeros
    * @param exogenousVar An array of array for differenced exogenous variables
    * @return The time series resulting from the interaction of the parameters with the model's
    *         coefficients
    */
  private def iterateARMAX(
                            ts: Vector,
                            history: BreezeDenseVector[Double],
                            op: (Double, Double) => Double,
                            goldStandard: Vector = null,
                            errors: Vector = null,
                            initMATerms: Array[Double] = null,
                            exogenousVar: Array[Array[Double]]): BreezeDenseVector[Double] = {
    require(goldStandard != null || errors != null, "goldStandard or errors must be passed in")
    val maTerms = if (initMATerms == null) Array.fill(q)(0.0) else initMATerms
    val intercept = if (includeIntercept) 1 else 0
    var maxPQ = math.max(p, q)
    var j = 0
    val tsSize = ts.size
    var error = 0.0

    val xregMatrix = new BreezeDenseMatrix(rows = exogenousVar.flatten.length / exogenousVar.size, cols = exogenousVar.size, data = exogenousVar.flatten)
    val xregPredictors = AutoregressionX.assemblePredictors(Array.fill(xregMaxLag)(0.0) ++ ts.toArray, MatrixUtil.matToRowArrs(xregMatrix), 0, xregMaxLag, includeOriginalXreg)

    while (maxPQ < tsSize) {
      j = 0
      // intercept (c)
      history(maxPQ) = op(history(maxPQ), intercept * coefficients(j))

      // autoregressive terms
      while (j < p && maxPQ - j - 1 >= 0) {
        history(maxPQ) = op(history(maxPQ), ts(maxPQ - j - 1) * coefficients(1 + j))
        j += 1
      }

      j = 0
      // exogenous variables
      var xregImpact = 0.0
      var psi = 0
      xregImpact = {
        var sum = 0.0
        for ((xregVal, index) <- xregPredictors(maxPQ - 1).zipWithIndex) {
          val counter = if (psi == exogenousVar.size) {
            psi = 0
            0
          } else psi

          psi += 1
          sum = xregVal * coefficients(1 + p + q + counter)
        }
        sum
      }
      history(maxPQ) = op(history(maxPQ), xregImpact)


      // moving average terms
      j = 0
      while (j < q) {
        history(maxPQ) = op(history(maxPQ), maTerms(j) * coefficients(1 + p + j))
        j += 1
      }

      error = if (goldStandard == null) errors(maxPQ) else goldStandard(maxPQ) - history(maxPQ)
      updateMAErrors(maTerms, error)
      maxPQ += 1
    }
    history
  }

  /**
    * Performs differencing of order `d` for given a matrix of exogenous variables. This means we recursively
    * difference a vector a total of d-times. So that d = 2 is a vector of the differences of differences.
    * Note that for each difference level, d_i, the element at ts(d_i - 1) corresponds to the value in the prior
    * iteration.
    *
    * @param xreg A matrix of exogenous variables to difference
    * @return An array of array for each column of xreg values differenced to order d
    */
  def differenceXreg(xreg: Matrix[Double]): Array[Array[Double]] = {
    val xregColumns = xreg.cols
    val xregArrays = Array.fill[Array[Double]](xregColumns)(Array.empty)
    for (i <- 0 until xregColumns) {
      val xregCol = new BreezeDenseVector(xreg.toArray.slice(xreg.rows * i, (i + 1) * xreg.rows))
      val diffXregCol = (new DenseVector(Array.fill(math.max(p, q))(0.0) ++ differencesOfOrderD(xregCol, d).toArray.drop(d))).toArray
      xregArrays(i) = diffXregCol
    }
    xregArrays
  }

  /**
    * Given a timeseries, assume that it is the result of an ARIMAX(p, d, q) process, and apply
    * inverse operations to obtain the original series of underlying errors.
    * To do so, we assume prior MA terms are 0.0, and prior AR are equal to the model's intercept or
    * 0.0 if fit without an intercept
    *
    * @param ts Time series of observations with this model's characteristics.
    * @return The dest series, representing remaining errors, for convenience.
    */
  def removeTimeDependentEffects(ts: Vector, destTs: Vector): Vector = {
    // difference as necessary
    val diffed = differencesOfOrderD(ts, d)
    val maxLag = math.max(p, q)
    val interceptAmt = if (includeIntercept) coefficients(0) else 0.0
    // extend vector so that initial AR(maxLag) are equal to intercept value
    val diffedExtended = new BreezeDenseVector[Double](
      Array.fill(maxLag)(interceptAmt) ++ diffed.toArray)
    val changes = diffedExtended.copy
    // changes(i) corresponds to the error term at index i, since when it is used we will have
    // removed AR and MA terms at index i
    iterateARMA(diffedExtended, changes, _ - _, errors = changes)
    val breezeDestTs = toBreeze(destTs)
    toBreeze(destTs) := changes(maxLag to -1)
    destTs
  }


  /**
    * Given a timeseries, apply an ARIMAX(p, d, q) model to it.
    * We assume that prior MA terms are 0.0 and prior AR terms are equal to the intercept or 0.0 if
    * fit without an intercept
    *
    * @param ts Time series of i.i.d. observations.
    * @return The dest series, representing the application of the model to provided error
    *         terms, for convenience.
    */
  def addTimeDependentEffects(ts: Vector, destTs: Vector): Vector = {
    val maxLag = math.max(p, q)
    val interceptAmt = if (includeIntercept) coefficients(0) else 0.0
    // extend vector so that initial AR(maxLag) are equal to intercept value
    // Recall iterateARMA begins at index maxLag, and uses previous values as AR terms
    val changes = new BreezeDenseVector[Double](Array.fill(maxLag)(interceptAmt) ++ ts.toArray)
    // ts corresponded to errors, and we want to use them
    val errorsProvided = changes.copy
    iterateARMA(changes, changes, _ + _, errors = errorsProvided)
    // drop assumed AR terms at start and perform any inverse differencing required
    toBreeze(destTs) := toBreeze(inverseDifferencesOfOrderD(changes(maxLag to -1), d))
    destTs
  }

}
