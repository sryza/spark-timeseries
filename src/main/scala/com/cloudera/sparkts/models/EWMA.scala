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

package com.cloudera.sparkts.models

import com.cloudera.sparkts.MatrixUtil
import org.apache.commons.math3.analysis.{MultivariateFunction, MultivariateVectorFunction}
import org.apache.commons.math3.optim.nonlinear.scalar.gradient.NonLinearConjugateGradientOptimizer
import org.apache.commons.math3.optim.nonlinear.scalar.{GoalType, ObjectiveFunction,
  ObjectiveFunctionGradient}
import org.apache.commons.math3.optim.{InitialGuess, MaxEval, MaxIter, SimpleValueChecker}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}

/**
 * Fits an Exponentially Weight Moving Average model (EWMA) (aka. Simple Exponential Smoothing) to
 * a time series. The model is defined as S_t = (1 - a) * X_t + a * S_{t - 1}, where a is the
 * smoothing parameter, X is the original series, and S is the smoothed series. For more
 * information, please see https://en.wikipedia.org/wiki/Exponential_smoothing.
 */
object EWMA {
  /**
   * Fits an EWMA model to a time series. Uses the first point in the time series as a starting
   * value. Uses sum squared error as an objective function to optimize to find smoothing parameter
   * The model for EWMA is recursively defined as S_t = (1 - a) * X_t + a * S_{t-1}, where
   * a is the smoothing parameter, X is the original series, and S is the smoothed series
   * Note that the optimization is performed as unbounded optimization, although in its formal
   * definition the smoothing parameter is <= 1, which corresponds to an inequality bounded
   * optimization. Given this, the resulting smoothing parameter should always be sanity checked
   * https://en.wikipedia.org/wiki/Exponential_smoothing
   * @param ts the time series to which we want to fit an EWMA model
   * @return EWMA model
   */
  def fitModel(ts: Vector): EWMAModel = {
    val optimizer = new NonLinearConjugateGradientOptimizer(
      NonLinearConjugateGradientOptimizer.Formula.FLETCHER_REEVES,
      new SimpleValueChecker(1e-6, 1e-6))
    val gradient = new ObjectiveFunctionGradient(new MultivariateVectorFunction() {
      def value(params: Array[Double]): Array[Double] = {
        val g = new EWMAModel(params(0)).gradient(ts)
        Array(g)
      }
    })
    val objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        new EWMAModel(params(0)).sse(ts)
      }
    })
    // optimization parameters
    val initGuess = new InitialGuess(Array(.94))
    val maxIter = new MaxIter(10000)
    val maxEval = new MaxEval(10000)
    val goal = GoalType.MINIMIZE
    // optimization step
    val optimal = optimizer.optimize(objectiveFunction, goal, gradient, initGuess, maxIter, maxEval)
    val params = optimal.getPoint
    new EWMAModel(params(0))
  }
}

class EWMAModel(val smoothing: Double) extends TimeSeriesModel {

  /**
   * Calculates the SSE for a given timeseries ts given the smoothing parameter of the current model
   * The forecast for the observation at period t + 1 is the smoothed value at time t
   * Source: http://people.duke.edu/~rnau/411avg.htm
   * @param ts the time series to fit a EWMA model to
   * @return Sum Squared Error
   */
  private[sparkts] def sse(ts: Vector): Double = {
    val n = ts.size
    val smoothed = new DenseVector(Array.fill(n)(0.0))
    addTimeDependentEffects(ts, smoothed)

    var i = 0
    var error = 0.0
    var sqrErrors = 0.0
    while (i < n - 1) {
      error = ts(i + 1) - smoothed(i)
      sqrErrors += error * error
      i += 1
    }

    sqrErrors
  }

  /**
   * Calculates the gradient of the SSE cost function for our EWMA model
   * @return gradient
   */
  private[sparkts] def gradient(ts: Vector): Double = {
    val n = ts.size
    val smoothed = new DenseVector(Array.fill(n)(0.0))
    addTimeDependentEffects(ts, smoothed)

    var error = 0.0
    var prevSmoothed = ts(0)
    var prevDSda = 0.0 // derivative of the EWMA function at time t - 1: (d S(t - 1)/ d smoothing)
    var dSda = 0.0 // derivative of the EWMA function at time t: (d S(t) / d smoothing)
    var dJda = 0.0 // derivative of our SSE cost function
    var i = 0

    while (i < n - 1) {
      error = ts(i + 1) - smoothed(i)
      dSda = ts(i) - prevSmoothed + (1 - smoothing) * prevDSda
      dJda += error * dSda
      prevDSda = dSda
      prevSmoothed = smoothed(i)
      i += 1
    }
    2 * dJda
  }

  override def removeTimeDependentEffects(ts: Vector, dest: Vector = null): Vector = {
    val arr = dest.toArray
    arr(0) = ts(0) // by definition in our model S_0 = X_0

    for (i <- 1 until ts.size) {
      arr(i) = (ts(i) - (1 - smoothing) * ts(i - 1)) / smoothing
    }
    new DenseVector(arr)
  }

  override def addTimeDependentEffects(ts: Vector, dest: Vector): Vector = {
    val arr = dest.toArray
    arr(0) = ts(0) // by definition in our model S_0 = X_0

    for (i <- 1 until ts.size) {
      arr(i) = smoothing * ts(i) + (1 - smoothing) * dest(i - 1)
    }
    new DenseVector(arr)
  }
}
