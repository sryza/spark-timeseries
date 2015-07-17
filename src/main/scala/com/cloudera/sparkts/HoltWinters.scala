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

import org.apache.commons.math3.analysis.{MultivariateFunction, MultivariateVectorFunction}
import org.apache.commons.math3.optim.{InitialGuess, MaxEval, MaxIter, SimpleBounds,
SimpleValueChecker}
import org.apache.commons.math3.optim.nonlinear.scalar.gradient.NonLinearConjugateGradientOptimizer
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer
import org.apache.commons.math3.optim.nonlinear.scalar.{GoalType, ObjectiveFunction,
ObjectiveFunctionGradient}



/**
 * TODO: add explanation
 */

object HoltWinters {
  def fitModel(ts: Vector[Double], m: Int, method: String = "BOBYQA"): HoltWintersModel = {
    method match {
      //case "CG" => fitModelWithCG(ts)
      case "BOBYQA" => fitModelWithBOBYQA(ts, m)
      case _ => throw new UnsupportedOperationException("Currently only supports 'CG' and 'BOBYQA'")
    }
  }

  def fitModelWithBOBYQA(ts: Vector[Double], m: Int): HoltWintersModel = {
    val optimizer = new BOBYQAOptimizer(5)
    val objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        new HoltWintersModel(m, params(0), params(1), params(2)).sse(ts)
      }
    })
    // The starting guesses in R's stats:HoltWinters
    val initGuess = new InitialGuess(Array(0.3, 0.1, 0.1))
    val maxIter = new MaxIter(10000)
    val maxEval = new MaxEval(10000)
    val goal = GoalType.MINIMIZE
    val bounds = new SimpleBounds(Array(0.0, 0.0, 0.0), Array(1.0, 1.0, 1.0))
    val optimal = optimizer.optimize(objectiveFunction, goal, bounds, initGuess, maxIter, maxEval)
    val params = optimal.getPoint
    new HoltWintersModel(m, params(0), params(1), params(2))
  }

  /*
  /**
   * Fits a Holt linear model to a time series by minimizing the SSE of the original time series
   * versus the estimates produced by the model. Performs unbounded optimization using
   * Conjugate Gradient Descent.
   * @param ts time series to fit model to
   * @return HoltModel with estimated alpha and beta parameters
   */
  def fitModelWithCG(ts: Vector[Double]): HoltModel = {
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
  */
}


class HoltWintersModel(
  val m: Int,
  val alpha: Double,
  val beta: Double,
  val gamma: Double) extends TimeSeriesModel {
  /**
   * Calculates sum of squared errors, used to estimate the alpha and beta parameters
   * @param ts A time series for which we want to calculate the SSE, given the current parameters
   * @return SSE
   */
  def sse(ts: Vector[Double]): Double = {
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

  /**
   * Calculate gradient of SSE, partial derivative in terms of alpha and beta
   * @param ts time series that we are trying to optimize parameters for
   * @return gradient of SSE at current parameters
   */
  //private[sparkts] def gradient(ts: Vector[Double]): Array[Double] = {
    //not yet implemented
  //}

  /**
   * {@inheritDoc}
   */
  override def removeTimeDependentEffects(ts: Vector[Double], dest: Vector[Double] = null)
  : Vector[Double] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  /**
   * {@inheritDoc}
   */
  override def addTimeDependentEffects(ts: Vector[Double], dest: Vector[Double]): Vector[Double] = {
    val fitted = getHoltWintersComponents(ts)._1
    for (i <- 0 until dest.length) {
      dest(i) = fitted(i)
    }
    dest
  }


  /**
   * TODO, explain
   * @param ts
   * @param dest
   */
  def forecast(ts: Vector[Double], dest: Vector[Double]) = {
    val (fitted, level, trend, season) = getHoltWintersComponents(ts)
    val n = ts.length
    var si = 0
    val levelVal = level(n - 1)
    val trendVal = trend(n - 1)

    for (i <- 0 until dest.length) {
      // if in sample, fitted, else forecasted
      if (i > n - 1) {
          si = if ((i - n) % m == 0) n else (n - m) + ((i - n) % m)
          dest(i) = levelVal + (i - n + 1) * trendVal + season(si - 1)
        } else {
          dest(i) = fitted(i)
        }
    }
    dest
  }

  /**
   * TODO: explain
   * 3 components, level, trend, seasonality
   */
  def getHoltWintersComponents(ts: Vector[Double])
    : (Vector[Double], Vector[Double], Vector[Double], Vector[Double]) = {
    val n = ts.length
    require(n >= 2, "Requires length of at least 2")

    val dest = new DenseVector(Array.fill(n)(0.0))
    val level = new DenseVector(Array.fill(n)(0.0))
    val trend = new DenseVector(Array.fill(n)(0.0))

    // http://robjhyndman.com/hyndsight/hw-initialization/
    // We follow the simple method (1998), and leave as TODO
    // to initalize using the 2008 suggestion
    val (initLevel, initTrend, season) = initHoltWintersSimple(ts)

    var prevTrend = initLevel
    var prevLevel = initTrend
    var si = m // seasonal index...used so that below reads as stated in source

    for (i <- 0 until ts.length) {
      level(i) = alpha * (ts(i) - season(si - m)) + (1 - alpha) * (prevLevel + prevTrend)
      trend(i) =  beta * (level(i) - prevLevel) + (1 - beta) * prevTrend
      // We'll stick to this variant, so that we can impose constraints on parameters
      // easily with BOBYQA
      season(si) = gamma * (ts(i) - level(i)) + (1 - gamma) * season(si - m)
      prevLevel = level(i)
      prevTrend = trend(i)

      if (i < ts.length - 1) {
        dest(i + 1) = level(i) + trend(i) + season(si - m)
      }
    }

    (dest, level, trend, season)
  }

  //TODO: add check for length...bad method for short/noisy series as per source
  //TODO: implemente alternative start described in Hyndman 2008
  def initHoltWintersSimple(ts: Vector[Double]): (Double, Double, Vector[Double]) = {
    val arrTs = ts.toArray
    val lm = arrTs.take(m).sum / m //average value for first m obs
    val bm = arrTs.take(m * 2).splitAt(m).zipped.map { case (prevx, x) =>
          x - prevx
      }.sum / (m * m)
    val siPrelim = arrTs.map(_ - lm)
    val si = siPrelim.take(m) ++ siPrelim // add m at the start for ease of indexing
    (lm, bm, new DenseVector(si))
  }

}


