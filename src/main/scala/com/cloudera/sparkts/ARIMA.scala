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
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.commons.math3.random.MersenneTwister


object ARIMA {
  /**
   * Given a time series, fit a non-seasonal ARIMA model of order (p, d, q), where p represents
   * the autoregression terms, d represents the order of differencing, and q moving average error
   * terms. If includeIntercept is true, the model is fitted with a mean. The user can specify
   * the method used to fit the parameters. The currently implementation only supports "css",
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
      method: String = "css")
    : ARIMAModel = {
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
   * Fit an ARIMA model using conditional sum of squares, currently optimizef using unbounded
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
      initParams: Array[Double]
      )
    : Array[Double]= {

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
    val optimal = optimizer.optimize(objFunction, goal, bounds, maxIter, maxEval,
      initialGuess)
    optimal.getPoint
  }

  // TODO: implement MLE parameter estimates with Kalman filter.
  private def fitWithML(diffedY: Array[Double],
      arTerms: Array[Array[Double]],
      maTerms: Array[Double],
      initCoeffs: Array[Double])
    : Array[Double] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Initializes ARMA paramater estimates using the Hannan-Risannen algorithm. The process is:
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
      includeIntercept: Boolean)
    : Array[Double] = {
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
   * @return a differenced vector
   */
  def differences(ts: Vector[Double], order: Int): Vector[Double] = {
    if (order == 0) {
      ts
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
   * @return a vector where the difference operation as been inverted
   */
  def invDifferences(ts: Vector[Double], order: Int): Vector[Double] = {

    if (order == 0) {
      ts
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
    val order:(Int, Int, Int), // order of autoregression, differencing, and moving average
    val coefficients: Array[Double], // coefficients: intercept, AR coeffs, ma coeffs
    val hasIntercept: Boolean = true,
    val seed: Long = 10L // seed for random number generation
    ) extends TimeSeriesModel {

  val rand = new MersenneTwister(seed)
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

  def logLikelihoodCSSARMA(diffedY: Array[Double]): Double = {
    val n = diffedY.length
    val yHat = new DenseVector(Array.fill(n)(0.0))
    val yVect = new DenseVector(diffedY)
    iterateARMA(yVect,  yHat, _ + _, goldStandard = yVect, initErrors = null)

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
   * Updates the error vector in place for a new (more recent) error
   * The newest error is placed in position 0, while older errors "fall off the end"
   * @param errs array of errors of length q in ARIMA(p, d, q), holds errors for t-1 through t-q
   * @param newError the error at time t
   * @return
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
   *                     errors are directly provided, otherwise drawn from gaussian distribution
   *
   * @param initErrors Initialization for first q errors. If none provided (i.e. remains null, as
   *                   per default), then initializes to all zeros
   *
   * @return dest series
   */
  private def iterateARMA(
      ts: Vector[Double],
      dest: Vector[Double],
      op: (Double, Double) => Double,
      goldStandard: Vector[Double] = null,
      errors: Vector[Double] = null,
      initErrors: Array[Double] = null)
    : Vector[Double] = {
    require(goldStandard != null || errors != null, "goldStandard or errors must be passed in")
    val (p, d, q) = order
    val maTerms = if (initErrors == null) Array.fill(q)(0.0) else initErrors
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
   * Given a timeseries, assume that is the result of an ARIMA(p, d, q) process, and apply
   * inverse operations to obtain the original series. Meaning:
   * 1 - difference if necessary
   * 2 - For each term subtract out AR and MA values
   * First d elements are taken directly from ts
   * @param ts Time series of observations with this model's characteristics.
   * @param destTs
   * @return The dest series, for convenience.
   */
  def removeTimeDependentEffects(ts: Vector[Double], destTs: Vector[Double]): Vector[Double] = {
    /*
    // jose: I don't quite think this is right... but not sure exactly what the operation should
    // look like. I've left it here for your reference
    val (p, d, q) = order
    val maxLag = math.max(p, q)
    val diffedTs = new DenseVector(ARIMA.differences(ts, d).toArray.drop(d))
    val changes =  new DenseVector(Array.fill(diffedTs.length)(0.0))
    //copy values in ts into destTs first
    changes := diffedTs
    val errs = new DenseVector(Array.fill(diffedTs.length)(rand.nextGaussian))
    // diffedTs corresponds to the ARMA process, changes corresponds to error at period i
    // diffedTs(i) - AR_terms - MA_terms = error_i = changes(i)
    // Note that for the first term diffedTs(0), there are 0 AR terms, and 0
    iterateARMA(diffedTs, changes, _ - _ , errors = changes)
    //copy first d elements into destTs directly from time series
    destTs(0 until d) := ts(0 until d)
    //copy remainder changes
    destTs(d until destTs.length) := changes
    destTs
    */
    throw new UnsupportedOperationException()
  }

  /**
   * Given a timeseries, apply an ARIMA(p, d, q) to it. Steps are:
   * 1 - add gaussian distributed errors if model has q term
   * 2 - For each term add in AR and MA values, where MA values are taken from (1) if needed
   * 3 - Sum values (i.e. inverse difference) to get final series, as appropriate
   * @param ts Time series of i.i.d. observations.
   * @param destTs
   * @return The dest series, for convenience.
   */
  def addTimeDependentEffects(ts: Vector[Double], destTs: Vector[Double]): Vector[Double] = {
    val (p, d, q) = order
    val maxLag = math.max(p, q)
    val changes = new DenseVector(Array.fill(ts.length)(0.0))
    // copy values
    changes := ts
    val errs = new DenseVector(Array.fill(ts.length)(rand.nextGaussian))
    changes :+= errs.map(x => x * (if (q > 0) 1.0 else 0.0)) // add in gaussian errors if needed
    // Note that changes goes in both ts and dest in call below, so that AR recursion is done
    // correctly. And we provide the errors, in case needed for MA terms
    iterateARMA(changes, changes, _ + _ , errors = errs)
    destTs :=  ARIMA.invDifferences(changes, d) // add up as necessary
  }

  /**
   * Sample a series of size n assuming an ARIMA(p, d, q) process. Note that first d + max(p, q)
   * elements stem from the originally sampled values.
   * @param n size of sample
   * @return series reflecting ARIMA(p, d, q) process
   */
  def sample(n: Int): Vector[Double] = {
    val zeros = new DenseVector(Array.fill(n + 1)(0.0))
    val result = addTimeDependentEffects(zeros, zeros)
    new DenseVector(result.toArray.drop(1))
  }

  /**
   * Provided fitted values for timeseries ts as 1-step ahead forecasts, based on current
   * model parameters, and then provide `nFuture` periods of forecast
   * @param ts Timeseries to use a gold-standard. Each value (i) in the returning series
   *           is a 1-step ahead forecast of ts(i). We use the difference between ts(i) -
   *           estimated(i) to calculate the error, which is used as MA terms
   * @param nFuture Periods in the future to forecast
   * @return a series consisting of fitted 1-step ahead forecasts for historicals and then
   *         `nFuture` periods of forecasts. Note that in the future values error terms become
   *         zero and prior predictions are used for any AR terms.
   */
  def forecast(ts: Vector[Double], nFuture: Int): Vector[Double] = {
    val (p, d, q) = order
    val maxLag = math.max(p, q)
    // difference timeseries as necessary for model
    val diffedTs = new DenseVector(ARIMA.differences(ts, d).toArray.drop(d))
    val nDiffed = diffedTs.length

    val hist = new DenseVector(Array.fill(nDiffed)(0.0))

    // fit historical values
    iterateARMA(diffedTs, hist, _ + _,  goldStandard = diffedTs)

    // Last set of errors, to be used in forecast if MA terms included
    val maTerms = (for (i <- nDiffed - maxLag until nDiffed) yield diffedTs(i) - hist(i)).toArray

    // include maxLag to copy over last maxLag values, to use in iterateARMA
    val forward = new DenseVector(Array.fill(nFuture + maxLag)(0.0))
    forward(0 until maxLag) := hist(nDiffed- maxLag until nDiffed)
    // use self as ts to take AR from same series, use self as goldStandard to induce future errors
    // of zero
    iterateARMA(forward, forward, _ + _, goldStandard = forward, initErrors = maTerms)

    val results = new DenseVector(Array.fill(ts.length + nFuture)(0.0))
    //copy first d elements prior to differencing
    results(0 until d) := ts(0 until d)
    // copy max of p/q element posts differencing, those values in hist are 0'ed out to correctly
    // calculate 1-step ahead forecasts
    results(d until d + maxLag) := diffedTs(0 until maxLag)
    // copy historical values, after first p/q
    results(d + maxLag until nDiffed + d) := hist(maxLag until nDiffed)
    // copy forward, after dropping first maxLag terms, since these were just repeated
    // for convenience in iterateARMA
    results(nDiffed + d to -1) := forward(maxLag to -1)
    // add up if there was any differencing
    ARIMA.invDifferences(results, d)
  }
}
