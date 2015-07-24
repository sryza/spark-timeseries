/**
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.sparkts

import breeze.linalg._

import org.apache.commons.math3.analysis.MultivariateFunction
import org.apache.commons.math3.optim.{SimpleBounds, MaxEval, MaxIter, InitialGuess}
import org.apache.commons.math3.optim.nonlinear.scalar.{GoalType, ObjectiveFunction}
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer
import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression


object ARIMA {
  /**
   * Given a time series, fit a non-seasonal ARIMA model of order (p, d, q), where p represents
   * the autoregression terms, d represents the order of differencing, and q moving average error
   * terms. If includeIntercept is true, the model is fitted with a mean. The user can specify
   * the method used to fit the parameters. The current implementation only supports "css",
   * meaning conditional sum of squares. In order to select the appropriate order of the model,
   * users are advised to visualize inspect ACF and PACF plots. Finally, current implementation
   * does not verify if the parameters fitted are stationary/invertible. Given this, it is suggested
   * that users inspect the resulting model. However, if parameters are not sane, it is likely
   * that the order of the model fit is inappropriate.
   */
  def fitModel(
      order:(Int, Int, Int),
      ts: Vector[Double],
      includeIntercept: Boolean = true,
      method: String = "css"): ARIMAModel = {
    val (p, d, q) = order
    val diffedTs = differences(ts, d).toArray.drop(d)

    if (p > 0 && q == 0) {
      val arModel = Autoregression.fitModel(new DenseVector(diffedTs), p)
      return new ARIMAModel(order, Array(arModel.c) ++ arModel.coefficients, includeIntercept)
    }

    val initParams = HannanRisannenInit(diffedTs, order, includeIntercept)
    val initCoeffs = fitWithCSS(diffedTs, order, includeIntercept, initParams)

    method match {
      case "css" => new ARIMAModel(order,initCoeffs, includeIntercept)
      case "ml" => throw new UnsupportedOperationException()
    }
  }

  /**
   * Fit an ARIMA model using conditional sum of squares, currently optimized using unbounded
   * BOBYQA.
   * @param diffedY time series we wish to fit to, already differenced if necessary
   * @param includeIntercept does the model include an intercept
   * @param initParams initial parameter guesses
   * @return
   */
  private def fitWithCSS(
      diffedY: Array[Double],
      order: (Int, Int, Int),
      includeIntercept: Boolean,
      initParams: Array[Double]): Array[Double]= {
    // We set up starting/ending trust radius using default suggested in
    // http://cran.r-project.org/web/packages/minqa/minqa.pdf
    // While # of interpolation points as mentioned common in
    // Source: http://www.damtp.cam.ac.uk/user/na/NA_papers/NA2009_06.pdf
    val (p, d, q) = order
    val radiusStart = math.min(0.96, 0.2 * initParams.map(math.abs).max)
    val radiusEnd = radiusStart * 1e-6
    val dimension = p + q + (if (includeIntercept) 1 else 0)
    val interpPoints = dimension * 2 + 1

    val optimizer = new BOBYQAOptimizer(interpPoints, radiusStart, radiusEnd)
    val objFunction = new ObjectiveFunction(new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        new ARIMAModel(order, params, includeIntercept).logLikelihoodCSSARMA(diffedY)
      }
    })

    val initialGuess = new InitialGuess(initParams)
    val maxIter = new MaxIter(10000)
    val maxEval = new MaxEval(10000)
    // TODO: Enforce stationarity and invertibility for AR and MA terms
    val bounds = SimpleBounds.unbounded(dimension)
    val goal = GoalType.MAXIMIZE
    val optimal = optimizer.optimize(objFunction, goal, bounds, maxIter, maxEval, initialGuess)
    optimal.getPoint
  }

  // TODO: implement MLE parameter estimates with Kalman filter.
  private def fitWithML(
      diffedY: Array[Double],
      arTerms: Array[Array[Double]],
      maTerms: Array[Double],
      initCoeffs: Array[Double]): Array[Double] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Initializes ARMA parameter estimates using the Hannan-Risannen algorithm. The process is:
   * fit an AR(m) model of a higher order (i.e. m > max(p, q)), use this to estimate errors,
   * then fit an OLS model of AR(p) terms and MA(q) terms. The coefficients estimated by
   * the OLS model are returned as initial parameter estimates
   * @param diffedY differenced time series, as appropriate
   * @param order model order (p, d, q)
   * @param includeIntercept boolean indicating if an intercept should be fit as well
   * @return initial ARMA(p, d, q) parameter estimates
   */
  private def HannanRisannenInit(
      diffedY: Array[Double],
      order:(Int, Int, Int),
      includeIntercept: Boolean): Array[Double] = {
    val (p, d, q) = order
    val addToLag = 1
    val m = math.max(p, q) + addToLag // m > max(p, q)
    // higher order AR(m) model
    val arModel = Autoregression.fitModel(new DenseVector(diffedY), m)
    val arTerms1 = Lag.lagMatTrimBoth(diffedY, m, includeOriginal = false)
    val yTrunc = diffedY.drop(m)
    val estimated = arTerms1.zip(
      Array.fill(yTrunc.length)(arModel.coefficients)
      ).map { case (v, b) => v.zip(b).map { case (yi, bi) => yi * bi}.sum + arModel.c }
    // errors estimated from AR(m)
    val errors = yTrunc.zip(estimated).map { case (y, yhat) => y - yhat }
    // secondary regression, regresses X_t on AR and MA terms
    val arTerms2 = Lag.lagMatTrimBoth(yTrunc, p, includeOriginal = false).drop(math.max(q - p, 0))
    val errorTerms = Lag.lagMatTrimBoth(errors, q, includeOriginal = false).drop(math.max(p - q, 0))
    val allTerms = arTerms2.zip(errorTerms).map { case (ar, ma) => ar ++ ma }
    val regression = new OLSMultipleLinearRegression()
    regression.setNoIntercept(!includeIntercept)
    regression.newSampleData(yTrunc.drop(m - addToLag), allTerms)
    val params = regression.estimateRegressionParameters()
    params
  }

  /**
   * Calculate a differenced vector of a given order. Size-preserving by leaving first `order`
   * elements intact
   * @param ts Series to difference
   * @param order The difference order (e.g. x means y(i) = ts(i) - ts(i - x), etc)
   * @return a new differenced vector
   */
  def differences(ts: Vector[Double], order: Int): Vector[Double] = {
    if (order == 0) {
      // for consistency, since we create a new vector in else-branch
      ts.copy
    } else {
      val n = ts.length
      val diffedTs = new DenseVector(Array.fill(n)(0.0))
      var i = 0

      while (i < n) {
        // elements prior to `order` are copied over without modification
        diffedTs(i) = if (i < order) ts(i) else ts(i) - ts(i - order)
        i += 1
      }
      diffedTs
    }
  }

  /**
   * Calculate a "inverse-differenced" vector of a given order. Size-preserving by leaving first
   * `order` elements intact
   * @param ts Series to add up
   * @param order The difference order to add (e.g. x means y(i) = ts(i) + y(i -
   *              x), etc)
   * @return a new vector where the difference operation as been inverted
   */
  def invDifferences(ts: Vector[Double], order: Int): Vector[Double] = {

    if (order == 0) {
      // for consistency, since we create a new vector in else-branch
      ts.copy
    } else {
      val n = ts.length
      val addedTs = new DenseVector(Array.fill(n)(0.0))
      var i = 0

      while (i < n) {
        // elements prior to `order` are copied over without modification
        addedTs(i) = if (i < order) ts(i) else ts(i) + addedTs(i - order)
        i += 1
      }
      addedTs
    }
  }
}

class ARIMAModel(
    val order:(Int, Int, Int), // AR(p), differencing(d), MA(q)
    val coefficients: Array[Double], // coefficients: intercept, AR, MA, with increasing degrees
    val hasIntercept: Boolean = true) extends TimeSeriesModel {
  /**
   * loglikelihood based on conditional sum of squares
   * Source: http://www.nuffield.ox.ac.uk/economics/papers/1997/w6/ma.pdf
   * @param y time series
   * @return loglikehood
   */
  def logLikelihoodCSS(y: Vector[Double]): Double = {
    val d = order._2
    val diffedY = ARIMA.differences(y, d).toArray.drop(d)
    logLikelihoodCSSARMA(diffedY)
  }

  /**
   * loglikelihood based on conditional sum of squares. In contrast to
   * [[com.cloudera.sparkts.ARIMAModel.logLikelihoodCSS]] the array provided should correspond
   * to an already differenced array, so that the function below corresponds to the loglikelihood
   * for the ARMA rather than the ARIMA process
   * @param diffedY differenced array
   * @return loglikelihood of ARMA
   */
  def logLikelihoodCSSARMA(diffedY: Array[Double]): Double = {
    val n = diffedY.length
    val yHat = new DenseVector(Array.fill(n)(0.0))
    val yVec = new DenseVector(diffedY)
    iterateARMA(yVec,  yHat, _ + _, goldStandard = yVec)

    val (p, d, q) = order
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
   * Updates the error vector in place with a new (more recent) error
   * The newest error is placed in position 0, while older errors "fall off the end"
   * @param errs array of errors of length q in ARIMA(p, d, q), holds errors for t-1 through t-q
   * @param newError the error at time t
   * @return a modified array with the latest error placed into index 0
   */
  def updateMAErrors(errs: Array[Double], newError: Double): Unit= {
    val n = errs.length
    var i = 0
    while (i < n - 1) {
      errs(i + 1) = errs(i)
      i += 1
    }
    if (n > 0) {
      errs(0) = newError
    }
  }

  /**
   * Perform operations with the AR and MA terms, based on the time series `ts` and the errors
   * based off of `goldStandard` or `errors`, combined with elements from the series  `dest`.
   * Weights for terms are taken from the current model configuration.
   * So for example: iterateARMA(series1, series_of_zeros,  _ + _ , goldStandard = series1,
   * initErrors = null)
   * calculates the 1-step ahead forecasts for series1 assuming current coefficients, and initial
   * MA errors of 0.
   * @param ts Time series to use for AR terms
   * @param dest Time series holding initial values at each index
   * @param op Operation to perform between values in dest, and various combinations of ts, errors
   *           and intercept terms
   * @param goldStandard The time series to which to compare 1-step ahead forecasts to obtain
   *                     moving average errors. Default set to null. In which case, we check if
   *                     errors are directly provided. Either goldStandard or errors can be null,
   *                     but not both
   * @param errors The time series of errors to be used as error at time i, which is then used
   *               for moving average terms in future indices. Either goldStandard or errors can
   *               be null, but not both
   * @param initMATerms Initialization for first q terms of moving average. If none provided (i.e.
   *                    remains null, as per default), then initializes to all zeros
   * @return the time series resulting from the interaction of the parameters with the model's
   *         coefficients.
   */
  private def iterateARMA(
      ts: Vector[Double],
      dest: Vector[Double],
      op: (Double, Double) => Double,
      goldStandard: Vector[Double] = null,
      errors: Vector[Double] = null,
      initMATerms: Array[Double] = null): Vector[Double] = {
    require(goldStandard != null || errors != null, "goldStandard or errors must be passed in")
    val (p, d, q) = order
    val maTerms = if (initMATerms == null) Array.fill(q)(0.0) else initMATerms
    val intercept = if (hasIntercept) 1 else 0
    var i = math.max(p, q) // maximum lag
    var j = 0
    val n = ts.length
    var error = 0.0

    while (i < n) {
      j = 0
      // intercept
      dest(i) = op(dest(i), intercept * coefficients(j))
      // autoregressive terms
      while (j < p && i - j - 1 >= 0) {
        dest(i) = op(dest(i), ts(i - j - 1) * coefficients(intercept + j))
        j += 1
      }
      // moving average terms
      j = 0
      while (j < q) {
        dest(i) = op(dest(i), maTerms(j) * coefficients(intercept + p + j))
        j += 1
      }

      error = if (goldStandard == null) errors(i) else goldStandard(i) - dest(i)
      updateMAErrors(maTerms, error)
      i += 1
    }
    dest
  }

  /**
   * Given a timeseries, assume that it is the result of an ARIMA(p, d, q) process, and apply
   * inverse operations to obtain the original series of underlying errors.
   * To do so, we assume prior MA terms are 0.0, and prior AR are equal to the model's intercept
   * @param ts Time series of observations with this model's characteristics.
   * @param destTs
   * @return The dest series, representing remaining errors, for convenience.
   */
  def removeTimeDependentEffects(ts: Vector[Double], destTs: Vector[Double]): Vector[Double] = {
    val (p, d, q) = order
    // difference as necessary
    val diffed = ARIMA.differences(ts, d)
    val maxLag = math.max(p, q)
    val interceptAmt = if (hasIntercept) coefficients(0) else 0.0
    // extend vector so that initial AR(maxLag) are equal to intercept value
    val diffedExtended = new DenseVector(Array.fill(maxLag)(interceptAmt) ++ diffed.toArray)
    val changes = diffedExtended.copy
    // changes(i) corresponds to the error term at index i, since when it is used we will have
    // removed AR and MA terms at index i
    iterateARMA(diffedExtended, changes, _ - _, errors = changes)
    destTs := changes(maxLag to -1)
    destTs
  }

  /**
   * Given a timeseries, apply an ARIMA(p, d, q) model to it.
   * We assume that prior MA terms are 0.0 and prior AR terms are equal to the intercept
   * @param ts Time series of i.i.d. observations.
   * @param destTs
   * @return The dest series, representing the application of the model to provided error
   *         terms, for convenience.
   */
  def addTimeDependentEffects(ts: Vector[Double], destTs: Vector[Double]): Vector[Double] = {
    val (p, d, q) = order
    val maxLag = math.max(p, q)
    val interceptAmt = if (hasIntercept) coefficients(0) else 0.0
    // extend vector so that initial AR(maxLag) are equal to intercept value
    // Recall iterateARMA begins at index maxLag, and uses previous values as AR terms
    val changes = new DenseVector(Array.fill(maxLag)(interceptAmt) ++ ts.toArray)
    // ts corresponded to errors, and we want to use them
    val errorsProvided = changes.copy
    iterateARMA(changes, changes, _ + _, errors = errorsProvided)
    // perform any inverse differencing required
    destTs := ARIMA.invDifferences(changes(maxLag to -1), d)
    destTs
  }

  /**
   * Sample a series of size n assuming an ARIMA(p, d, q) process.
   * @param n size of sample
   * @return series reflecting ARIMA(p, d, q) process
   */
  def sample(n: Int, rand: RandomGenerator): Vector[Double] = {
    val vec = new DenseVector(Array.fill[Double](n)(rand.nextGaussian))
    addTimeDependentEffects(vec, vec)
  }

  /**
   * Provided fitted values for timeseries ts as 1-step ahead forecasts, based on current
   * model parameters, and then provide `nFuture` periods of forecast. We assume AR terms
   * prior to the start of the series are equal to the model's intercept term. Meanwhile, MA
   * terms prior to the start are assumed to be 0.0
   * @param ts Timeseries to use as gold-standard. Each value (i) in the returning series
   *           is a 1-step ahead forecast of ts(i). We use the difference between ts(i) -
   *           estimated(i) to calculate the error at time i, which is used for the moving
   *           average terms
   * @param nFuture Periods in the future to forecast (beyond length of ts)
   * @return a series consisting of fitted 1-step ahead forecasts for historicals and then
   *         `nFuture` periods of forecasts. Note that in the future values error terms become
   *         zero and prior predictions are used for any AR terms.
   */
  def forecast(ts: Vector[Double], nFuture: Int): Vector[Double] = {
    val (p, d, q) = order
    val maxLag = math.max(p, q)
    // difference timeseries as necessary for model
    val diffedTs = new DenseVector(ARIMA.differences(ts, d).toArray.drop(d))

    // Assumes prior AR terms are equal to model intercept
    val interceptAmt =  if (hasIntercept) coefficients(0) else 0.0
    val diffedTsExtended = new DenseVector(Array.fill(maxLag)(interceptAmt) ++ diffedTs.toArray)
    val histLen = diffedTsExtended.length
    val hist = new DenseVector(Array.fill(histLen)(0.0))

    // fit historical values
    iterateARMA(diffedTsExtended, hist, _ + _,  goldStandard = diffedTsExtended)

    // Last set of errors, to be used in forecast if MA terms included
    val maTerms = (for (i <- histLen - maxLag until histLen) yield {
      diffedTsExtended(i) - hist(i)
    }).toArray

    // copy over last maxLag values, to use in iterateARMA for forward curve
    val forward = new DenseVector(Array.fill(nFuture + maxLag)(0.0))
    forward(0 until maxLag) := hist(-maxLag to -1)
    // use self as ts to take AR from same series, use self as goldStandard to induce future errors
    // of zero, and use prior moving average errors as initial error terms for MA
    iterateARMA(forward, forward, _ + _, goldStandard = forward, initMATerms = maTerms)

    val results = new DenseVector(Array.fill(ts.length + nFuture)(0.0))
    // drop first maxLag terms from historicals, since these are our assumed AR terms
    results(0 until ts.length) := hist(maxLag to -1)
    // drop first maxLag terms from forward curve, these are part of hist already
    results(ts.length to -1) :=  forward(maxLag to -1)
    // add up if there was any differencing required
    ARIMA.invDifferences(results, d)
  }
}
