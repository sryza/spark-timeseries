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

import org.apache.commons.math3.analysis.{MultivariateFunction, MultivariateVectorFunction, UnivariateFunction}
import org.apache.commons.math3.optim.nonlinear.scalar.gradient.NonLinearConjugateGradientOptimizer
import org.apache.commons.math3.optim._
import org.apache.commons.math3.optim.nonlinear.scalar.{ObjectiveFunction, ObjectiveFunctionGradient, GoalType}
import org.apache.commons.math3.optim.univariate.{BrentOptimizer, UnivariateObjectiveFunction, SearchInterval}

object Holt {
  /**
   * TODO: ADD explanation
   */
  def fitModel(ts: Vector[Double]): HoltModel = {
    val optimizer = new NonLinearConjugateGradientOptimizer(
    NonLinearConjugateGradientOptimizer.Formula.FLETCHER_REEVES,
    new SimpleValueChecker(1e-6, 1e-6))
    val gradient = new ObjectiveFunctionGradient(new MultivariateVectorFunction() {
      def value(params: Array[Double]): Array[Double] = {
        new HoltModel(params(0), params(1)).gradient(ts).toArray
      }
    })
    val objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        new GARCHModel(params(0), params(1), params(2)).logLikelihood(ts)
      }
    })
    val initialGuess = new InitialGuess(Array(.2, .2, .2)) // TODO: make this smarter
    val maxIter = new MaxIter(10000)
    val maxEval = new MaxEval(10000)
    val optimal = optimizer.optimize(objectiveFunction, gradient, initialGuess, maxIter, maxEval)
    val params = optimal.getPoint
    new GARCHModel(params(0), params(1), params(2))
  }
}

class HoltModel(val alpha: Double, val beta: Double) extends TimeSeriesModel {

  /**
   * TODO: add doc
   * @param ts
   * @return Sum Squared Error
   */
  private[sparkts] def sse(ts: Vector[Double]): Double = {
    val n = ts.length
    val smoothed = new DenseVector(Array.fill(n)(0.0))
    addTimeDependentEffects(ts, smoothed)

    var error = 0.0
    var sqrErrors = 0.0
    var i = 0

    while (i <  n) {
      error = ts(i) - smoothed(i)
      sqrErrors += error * error
      i += 1
    }
    sqrErrors
  }

  // TODO: ADD GRADIENT

  override def removeTimeDependentEffects(ts: Vector[Double], dest: Vector[Double] = null)
  : Vector[Double] = {
    if(ts.length < 2) {
      throw new Exception()
    }

    var currZ = ts(0)
    var nextZ = ts(1)

    var num = nextZ + alpha * currZ + beta * alpha * currZ - 2 * currZ
    var denom = alpha + alpha * beta - 1 // to solve for the first value

    dest(0) = num / denom
    dest(1) = currZ

    var prevTrend = dest(1) - dest(0)
    var prevLevel = dest(0)

    denom = alpha + alpha * beta // to solve for all other values
    for (i <- 2 until ts.length) {
      currZ = ts(i)
      // Note that for the last element we calculate a smoothed value, as we cannot observe this
      nextZ = if (i < ts.length - 1) ts(i + 1) else prevLevel + 2 * prevTrend
      num = nextZ + (alpha - 1) * currZ + beta * alpha * currZ - prevTrend
      dest(i) = num / denom
      prevTrend = nextZ - (alpha * dest(i) + (1 - alpha) * currZ)
      prevLevel = nextZ - prevTrend
    }

    dest
  }

  override def addTimeDependentEffects(ts: Vector[Double], dest: Vector[Double]): Vector[Double] = {
    applyHolt(ts, dest)._1
  }

  /**
   * Applies a Holt model to a time series and forecasts periods beyond the observed data.
   * @param ts A vector of the original time series to which to apply a Holt model
   * @param dest A vector into which to store the fitted values, and forecasted values
   * @return A vector containing the fitted values (up to length ts.length - 1), and the forecasted
   *         values (all values after and including ts.length)
   */
  def forecast(ts: Vector[Double], dest: Vector[Double]) = {
    val (fitted, prevLevel, prevTrend) = applyHolt(ts, dest)
    val n = ts.length
    val forecastPeriods = dest.length - ts.length

    for(i <- 0 until forecastPeriods) {
      dest(n + i) = prevLevel + i * prevTrend
    }
    dest
  }

  private def applyHolt(ts: Vector[Double], dest: Vector[Double])
    : (Vector[Double], Double, Double) = {
    if(ts.length < 2) {
      throw new Exception()
    }
    var level = ts(0) // by definition in our model initial level is the first point
    var trend = ts(1) - ts(0) // by definition in our model initial trend is the first change
    var prevLevel = level
    var prevTrend = trend
    dest(0) = level + trend

    for (i <- 0 until ts.length) {
      level = alpha * ts(i) + (1 - alpha) * (prevLevel + prevTrend)
      trend =  beta * (level - prevLevel) + (1 - beta) * prevTrend

      if (i < ts.length - 1) {
        dest(i + 1) = level + trend
      }
      prevLevel = level
      prevTrend = trend
    }

    (dest, prevLevel, prevTrend)
  }

}


