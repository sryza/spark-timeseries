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
        new HoltModel(params(0), params(1)).gradient(ts)
      }
    })
    val objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        new HoltModel(params(0), params(1)).sse(ts)
      }
    })
    val initGuess = new InitialGuess(Array(.2, .2)) // TODO: make this smarter
    val maxIter = new MaxIter(10000)
    val maxEval = new MaxEval(10000)
    val goal = GoalType.MINIMIZE
    val optimal = optimizer.optimize(objectiveFunction, goal, gradient, initGuess, maxIter, maxEval)
    val params = optimal.getPoint
    new HoltModel(params(0), params(1))
  }
}

class HoltModel(val alpha: Double, val beta: Double) extends TimeSeriesModel {
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


  private[sparkts] def gradient(ts: Vector[Double]): Array[Double] = {
    // partial derivatives of cost function
    var (dJda, dJdb) = (0.0, 0.0)
    // partial derivatives for forecast function
    var (dZda, dZdb) = (0.0, 0.0)
    // partial derivatives for level
    var (dLda, prevDLda)  = (0.0, 0.0)
    var (dLdb, prevDLdb) = (0.0, 0.0)
    // partial derivatives for trend
    var (dTda, prevDTda) = (0.0, 0.0)
    var (dTdb, prevDTdb) = (0.0, 0.0)

    var error = 0.0

    val n = ts.length
    val (fitted, level, trend) = getHoltComponents(ts, new DenseVector(Array.fill(n)(0.0)))

    var i = 0
    while(i < n) {
      error = ts(i) - fitted(i)
      dZda = prevDLda + prevDTda
      dZdb = prevDLdb + prevDTdb
      dJda += error * dZda
      dJdb += error * dZdb

      // calculate new derivatives
      // partial derivatives in terms of alpha
      dLda = ts(i) + (1 - alpha) * dZda - fitted(i)
      dTda = beta * dLda + prevDTda - beta * dZda

      // partial derivatives in terms of beta
      dLdb = (1 - alpha) * dZdb
      dTdb =  beta * dLdb + level(i) + prevDTdb - beta * dZdb - fitted(i)

      // update derivatives
      prevDLda = dLda
      prevDLdb = dLdb
      prevDTda = dTda
      prevDTdb = dTdb
      i += 1
    }

    Array(2 * dJda, 2 * dJdb)
  }

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
  override def addTimeDependentEffects(ts: Vector[Double], dest: Vector[Double]): Vector[Double] = {
    getHoltComponents(ts, dest)._1
  }

  /**
   * Applies a Holt model to a time series and forecast periods beyond the observed data.
   * @param ts A vector of the original time series to which to apply a Holt model
   * @param dest A vector into which to store the fitted values, and forecasted values
   * @return A vector where values at indices [0, ts.length - 1] correspond to fitted values, and
   *         values at indices [ts.length, dest.length - 1] correspond to forecasted values
   */
  def forecast(ts: Vector[Double], dest: Vector[Double]) = {
    val (fitted, level, trend) = getHoltComponents(ts, dest)
    val n = ts.length
    val forecastPeriods = dest.length - ts.length
    val levelVal = level(n - 1)
    val trendVal = trend(n - 1)

    for(i <- 0 until forecastPeriods) {
      dest(n + i) = levelVal + i * trendVal
    }

    dest
  }

  /**
   * Apply a Holt linear model to a time series and obtain the fitted values, along with the
   * level and trend components (ie. the components of the fitted values).
   * @param ts A time series on which we want to apply the Holt model
   * @param dest A vector to place the fitted values
   * @return A triple of vectors of fitted values, level values, and trend values
   */
  def getHoltComponents(ts: Vector[Double], dest: Vector[Double])
    : (Vector[Double], Vector[Double], Vector[Double]) = {
    if(ts.length < 2) {
      throw new Exception()
    }
    val n = ts.length
    val level = new DenseVector(Array.fill(n)(0.0))
    val trend = new DenseVector(Array.fill(n)(0.0))

    level(0) = ts(0) // by definition in our model initial level is the first point
    trend(0) = ts(1) - ts(0) // by definition in our model initial trend is the first change
    dest(0) = level(0) + trend(0)

    var prevTrend = trend(0)
    var prevLevel = level(0)

    for (i <- 0 until ts.length) {
      level(i) = alpha * ts(i) + (1 - alpha) * (prevLevel + prevTrend)
      trend(i) =  beta * (level(i) - prevLevel) + (1 - beta) * prevTrend
      prevLevel = level(i)
      prevTrend = trend(i)

      if (i < ts.length - 1) {
        dest(i + 1) = level(i) + trend(i)
      }
    }

    (dest, level, trend)
  }

}


