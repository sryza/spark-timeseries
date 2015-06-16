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

import org.apache.commons.math3.analysis.UnivariateFunction
import org.apache.commons.math3.optim.MaxEval
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType
import org.apache.commons.math3.optim.univariate.{BrentOptimizer, UnivariateObjectiveFunction, SearchInterval}

object EWMA {
  /**
   * Fits an EWMA model to a time series. Uses the first point in the time series as a starting
   * value. Uses sum squared error as an objective function to optimize to find smoothing parameter
   * The model for EWMA is recursively defined as S_t = (1 - a) * X_t + a * S_{t-1}, where
   * a is the smoothing parameter, X is the original series, and S is the smoothed series
   * https://en.wikipedia.org/wiki/Exponential_smoothing
   * @param ts the time series to which we want to fit a EWMA model
   * @return EWMAmodel
   */
  def fitModel(ts: Vector[Double]): EWMAModel = {
    val objectiveFunction = new UnivariateObjectiveFunction(new UnivariateFunction() {
      def value(param: Double): Double = {
        new EWMAModel(param).sse(ts)
      }
    })

    val maxEval = new MaxEval(10000)
    val paramBounds = new SearchInterval(0.0, 1.0)
    val optimizer = new BrentOptimizer(1e-6, 1e-6)
    val optimal = optimizer.optimize(maxEval, objectiveFunction, GoalType.MINIMIZE, paramBounds)
    val param = optimal.getPoint
    new EWMAModel(param)
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
  private[sparkts] def sse(ts: Vector[Double]): Double = {
    val n = ts.length
    val smoothed = new DenseVector(Array.fill(n)(0.0))
    addTimeDependentEffects(ts, smoothed)
    val tsArr = ts.toArray
    val smoothedArr = smoothed.toArray
    val sqrErrors = smoothedArr.zip(tsArr.tail).map { case (yhat, y) => (yhat - y) * (yhat - y) }
    sum(sqrErrors)
  }

  override def removeTimeDependentEffects(ts: Vector[Double], dest: Vector[Double] = null)
    : Vector[Double] = {
    dest(0) = ts(0) // by definition in our model S_0 = X_0

    for (i <- 1 until ts.length) {
      dest(i) = (ts(i) - (1 - smoothing) * ts(i - 1)) / smoothing
    }
    dest
  }

  override def addTimeDependentEffects(ts: Vector[Double], dest: Vector[Double]): Vector[Double] = {
    dest(0) = ts(0) // by definition in our model S_0 = X_0

    for (i <- 1 until ts.length) {
      dest(i) = smoothing * ts(i) + (1 - smoothing) * dest(i - 1)
    }
    dest
  }
}
