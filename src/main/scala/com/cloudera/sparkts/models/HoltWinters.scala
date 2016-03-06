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

import org.apache.commons.math3.analysis.{MultivariateFunction}
import org.apache.commons.math3.optim.{InitialGuess, MaxEval, MaxIter, SimpleBounds}
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer
import org.apache.commons.math3.optim.nonlinear.scalar.{GoalType, ObjectiveFunction}
import org.apache.spark.mllib.linalg._

/**
 * TODO: add explanation
 */
object HoltWinters {
  def fitModel(ts: Vector, m: Int, method: String = "BOBYQA"): HoltWintersModel = {
    method match {
      //case "CG" => fitModelWithCG(ts)
      case "BOBYQA" => fitModelWithBOBYQA(ts, m)
      case _ => throw new UnsupportedOperationException("Currently only supports 'CG' and 'BOBYQA'")
    }
  }

  def fitModelWithBOBYQA(ts: Vector, m: Int): HoltWintersModel = {
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
  def sse(ts: Vector): Double = {
    val n = ts.size
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
   * {@inheritDoc}
   */
  override def removeTimeDependentEffects(ts: Vector, dest: Vector = null): Vector = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  /**
   * {@inheritDoc}
   */
  override def addTimeDependentEffects(ts: Vector, dest: Vector): Vector = {
    val destArr = dest.toArray
    val fitted = getHoltWintersComponents(ts)._1
    for (i <- 0 until dest.size) {
      destArr(i) = fitted(i)
    }
    dest
  }


  /**
   * TODO, explain
   * @param ts
   * @param dest
   */
  def forecast(ts: Vector, dest: Vector) = {
    val destArr = dest.toArray
    val (fitted, level, trend, season) = getHoltWintersComponents(ts)
    val n = ts.size
    var si = 0
    val levelVal = level(n - 1)
    val trendVal = trend(n - 1)

    for (i <- 0 until dest.size) {
      // if in sample, fitted, else forecasted
      if (i > n - 1) {
          si = if ((i - n) % m == 0) n else (n - m) + ((i - n) % m)
          destArr(i) = levelVal + (i - n + 1) * trendVal + season(si - 1)
        } else {
          destArr(i) = fitted(i)
        }
      si += 1
    }
    dest
  }

  /**
   * TODO: explain
   * 3 components, level, trend, seasonality
   */
  def getHoltWintersComponents(ts: Vector): (Vector, Vector, Vector, Vector) = {
    val n = ts.size
    require(n >= 2, "Requires length of at least 2")

    val dest = new Array[Double](n)
    val level = new Array[Double](n)
    val trend = new Array[Double](n)

    // http://robjhyndman.com/hyndsight/hw-initialization/
    // We follow the simple method (1998), and leave as TODO
    // to initalize using the 2008 suggestion
    val (initLevel, initTrend, season) = initHoltWintersSimple(ts)

    var prevTrend = initLevel
    var prevLevel = initTrend
    var si = 0
    dest(0) = initLevel + initTrend + season(0)

    for (i <- 0 until ts.size - 1) {
      si = if (i >= m) i - m else i
      level(i) = alpha * (ts(i) - season(si)) + (1 - alpha) * (prevLevel + prevTrend)
      trend(i) =  beta * (level(i) - prevLevel) + (1 - beta) * prevTrend
      // We'll stick to this variant, so that we can impose constraints on parameters
      // easily with BOBYQA
      if (i >= m ) { // only update seasonality after the first m periods
        season(i) = gamma * (ts(i) - level(i)) + (1 - gamma) * season(si)
      }
      prevLevel = level(i)
      prevTrend = trend(i)
      dest(i + 1) = level(i) + trend(i) + season(si + 1)
    }

    (Vectors.dense(dest), Vectors.dense(level), Vectors.dense(trend), Vectors.dense(season))
  }

  //TODO: add check for length...bad method for short/noisy series as per source
  //TODO: implemente alternative start described in Hyndman 2008
  def initHoltWintersSimple(ts: Vector): (Double, Double, Array[Double]) = {
    val arrTs = ts.toArray
    val lm = arrTs.take(m).sum / m //average value for first m obs
    val bm = arrTs.take(m * 2).splitAt(m).zipped.map { case (prevx, x) =>
        x - prevx
      }.sum / (m * m)
    val siPrelim = arrTs.take(m).map(_ - lm)
    //first m periods initialized to seasonal values, rest zeroed out
    val si = siPrelim ++ Array.fill(arrTs.length - m)(0.0)
    (lm, bm, si)
  }
}
