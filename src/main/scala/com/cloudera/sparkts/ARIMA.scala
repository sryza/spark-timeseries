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
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.commons.math3.optim.{SimpleBounds, MaxEval, MaxIter, InitialGuess}
import org.apache.commons.math3.optim.nonlinear.scalar.{GoalType, ObjectiveFunction}
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer
import org.apache.commons.math3.random.RandomGenerator

object ARIMA {
  /**
   * Fits an a non seasonal ARIMA model to the given time series.
   * http://sfb649.wiwi.hu-berlin.de/fedc_homepage/xplore/tutorials/xegbohtmlnode39.html
   */
  def fitModel(
      order:(Int, Int, Int),
      ts: Vector[Double],
      includeIntercept: Boolean = true,
      method: String = "css")
    : ARIMAModel = {
    val (p, d, q) = order
    val maxLag = math.max(p, q)
    // Y-related terms
    val y =  ts.toArray
    val terms = makeTerms(y, p, d, includeIntercept)
    val truncY = y.drop(maxLag + d) // drop as necessary

    val initParams = HannanRisannenInit(ts, p, q)
    val initCoeffs = fitWithCSS(truncY, terms, q, initParams)

    method match {
      case "css" => {
        new ARIMAModel(order, includeIntercept, initCoeffs)
      }
      case "ml" => {
        //new ARIMAModel(order, 0.0, Array(1.0, 2.0))
        throw new UnsupportedOperationException()
      }
    }
  }


  /**
   * Fit an ARIMA model using conditional sum of squares, currently fit using unbounded
   * BOBYQA.
   * @param y time series we wish to fit to
   * @param iARTerms an array of arrays containing intercept, autoregressive y, and any exogenous terms
   *              (original and lagged)
   * @param nMATerms number of moving average error terms
   * @return optimal parameters
   */
  def fitWithCSS(y: Array[Double],
      iARTerms: Array[Array[Double]],
      nMATerms: Int,
      initParams: Array[Double]
      )
    : Array[Double]= {

    // We set up starting/ending trust radius using default suggested in
    // http://cran.r-project.org/web/packages/minqa/minqa.pdf
    // While # of interpolation points as mentioned common in
    // Source: http://www.damtp.cam.ac.uk/user/na/NA_papers/NA2009_06.pdf
    val radiusStart = math.min(0.96, 0.2 * initParams.map(math.abs).max)
    val radiusEnd = radiusStart * 1e-6
    val dimension = iARTerms(0).length + nMATerms
    val interpPoints = dimension * 2 + 1

    val optimizer = new BOBYQAOptimizer(interpPoints, radiusStart, radiusEnd)
    val objFunction = new ObjectiveFunction(new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        val dummyOrder = (0, 0, 0)
        val maTerms = Array.fill(nMATerms)(0.0) // Moving average of errors, initialized to 0
        new ARIMAModel(dummyOrder, true, params).logLikelihoodCSS(y, iARTerms, maTerms)
      }
    })

    val initialGuess = new InitialGuess(initParams)
    val maxIter = new MaxIter(10000)
    val maxEval = new MaxEval(10000)
    // TODO: Enforce stationarity and invertibility for AR and MA terms, respectively?
    val bounds = SimpleBounds.unbounded(dimension)
    val goal = GoalType.MAXIMIZE
    val optimal = optimizer.optimize(objFunction, goal, bounds, maxIter, maxEval,
      initialGuess)
    optimal.getPoint
  }

  // TODO: implement MLE parameter estimates with Kalman filter.
  def fitWithML(y: Array[Double],
      arTerms: Array[Array[Double]],
      maTerms: Array[Double],
      initCoeffs: Array[Double])
    : Array[Double] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Calculate a differenced array of a given order
   * @param ts Array of doubles to difference
   * @param order The difference order (e.g. x means y(0) = ts(x) - ts(0), etc)
   * @return A differenced array of appropriate length
   */
  def differences(ts: Array[Double], order: Int): Array[Double] = {
    if (order == 0) {
      ts
    } else {
      val lenTs = ts.length
      val diffedTs = Array.fill(lenTs - order)(0.0)
      var i = order

      while (i < lenTs) {
        diffedTs(i - order) = ts(i) - ts(i - order)
        i += 1
      }
      diffedTs
    }
  }


  def makeTerms(ts: Array[Double], p: Int, d: Int, includeIntercept: Boolean)
    : Array[Array[Double]] = {
    // differencing
    val tsDiffed = differences(ts, d)
    // Auto regressive terms
    val arTerms = Lag.lagMatTrimBoth(tsDiffed, p, includeOriginal = false)
    // adding intercept
    val withIntercept = if (includeIntercept) arTerms.map(Array(1.0) ++ _) else arTerms
    withIntercept
  }

  /**
   * initialize ARMA estimates using the Hannan Risannen algorithm
   * Source: http://personal-homepages.mis.mpg.de/olbrich/script_chapter2.pdf
   */
  def HannanRisannenInit(ts: Vector[Double], p: Int, q: Int): Array[Double] = {
    val addToLag = 1
    val m = math.max(p, q) + addToLag // m > max(p, q)
    // higher order AR(m) model
    val yArr = ts.toArray
    val arModel = Autoregression.fitModel(ts, m)
    val arTerms1 = Lag.lagMatTrimBoth(yArr, m, false)
    val yTrunc = yArr.drop(m)
    val estimated = arTerms1.zip(
      Array.fill(yTrunc.length)(arModel.coefficients)
      ).map { case (v, b) => v.zip(b).map { case (yi, bi) => yi * bi}.sum + arModel.c }
    // errors estimated from AR(m)
    val errors = yTrunc.zip(estimated).map { case (y, yhat) => y - yhat }
    // secondary regression, regresses X_t on AR and MA terms
    val arTerms2 = Lag.lagMatTrimBoth(yTrunc, p, false).drop(math.max(q - p, 0))
    val errorTerms = Lag.lagMatTrimBoth(errors, q, false).drop(math.max(p - q, 0))
    val allTerms = arTerms2.zip(errorTerms).map { case (ar, ma) => ar ++ ma }
    val regression = new OLSMultipleLinearRegression()
    regression.newSampleData(yTrunc.drop(m - addToLag), allTerms)
    val params = regression.estimateRegressionParameters()
    params
  }

}

class ARIMAModel(
    val order:(Int, Int, Int), // order of autoregression, differencing , and moving average
    val hasIntercept: Boolean = true,
    val coeffs: Array[Double] //coefficients: intercept, AR coeffs, exogenous coeffs, ma coeffs
    ) extends TimeSeriesModel {


  // Source: http://www.nuffield.ox.ac.uk/economics/papers/1997/w6/ma.pdf
  // TODO: double check this...currently wrong!
  def logLikelihoodCSS(
      y: Array[Double],
      iARTerms:Array[Array[Double]],
      maTerms: Array[Double])
    : Double = {
    val n = y.length.toDouble
    // total dimension
    val dims = coeffs.length
    // dimension of intercept + ar terms
    val dar = if (iARTerms.head.isEmpty) 0 else iARTerms.head.length
    // loop counters
    var i = 0
    var j = 0
    // estimates and accumulators
    var yhat = 0.0
    var resid = 0.0
    var css = 0.0

    while (i < n) {
      while (j < dims) {
        if (j < dar) {
          // still processing intercept/AR terms
          yhat += iARTerms(i)(j) * coeffs(j)
        } else {
          // moved on to MA terms
          yhat += maTerms(j - dar) * coeffs(j)
        }
        j += 1
      }
      resid = y(i) - yhat // residual (aka innovation)
      css += resid * resid
      updateMAErrors(maTerms, resid) // shift back errors to include latest
      yhat = 0 // restart y hat for next iteration
      i += 1 // advance counters
      j = 0
   }
    //log likelihood CSS
    val sigma2 = css / n
    (-n/2) * math.log(2 * math.Pi * sigma2) - css / (2 * sigma2)
   }

  /**
   * Updates the error vector in place for a new (more recent) error
   * For example, if we had errors at time [0, 1, 2], this returns errors at time [1, 2, 3]
   * @param errs array of errors of length q in ARIMA(p, d, q), holds errors for t-1 through t-q
   * @param newError the error at time t
   * @return
   */
  def updateMAErrors(errs: Array[Double], newError: Double): Unit= {
    val n = errs.length
    var i = 0
    while (i < n - 1) {
      errs(i) = errs(i + 1)
      i += 1
    }
    if (n > 0) {
      errs(i) = newError
    }
  }

  /**
   * {@inheritDoc}
   */
  def removeTimeDependentEffects(ts: Vector[Double], destTs: Vector[Double] = null): Vector[Double] = {
    throw new UnsupportedOperationException()
  }

  /**
   * {@inheritDoc}
   */
  def addTimeDependentEffects(ts: Vector[Double], destTs: Vector[Double]): Vector[Double] = {
    throw new UnsupportedOperationException()
  }

  def sample(n: Int, rand: RandomGenerator): Vector[Double] = {
    throw new UnsupportedOperationException()
  }
}
