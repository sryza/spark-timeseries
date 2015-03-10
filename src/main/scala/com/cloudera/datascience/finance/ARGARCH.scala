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

package com.cloudera.datascience.finance

import org.apache.commons.math3.analysis.{MultivariateFunction, MultivariateVectorFunction}
import org.apache.commons.math3.optim.{MaxEval, MaxIter, InitialGuess, SimpleValueChecker}
import org.apache.commons.math3.optim.nonlinear.scalar.{ObjectiveFunction,
  ObjectiveFunctionGradient}
import org.apache.commons.math3.optim.nonlinear.scalar.gradient.NonLinearConjugateGradientOptimizer
import org.apache.commons.math3.random.RandomGenerator

object ARGARCH {
  /**
   * @param ts
   * @return The model and its log likelihood on the input data.
   */
  def fitModel(ts: Array[Double]): (ARGARCHModel, Double) = {
    val optimizer = new NonLinearConjugateGradientOptimizer(
      NonLinearConjugateGradientOptimizer.Formula.FLETCHER_REEVES,
      new SimpleValueChecker(1e-6, 1e-6))
    val gradient = new ObjectiveFunctionGradient(new MultivariateVectorFunction() {
      def value(params: Array[Double]): Array[Double] = {
        new ARGARCHModel(0.0, 0.0, params(0), params(1), params(2)).gradient(ts)
      }
    })
    val objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        new ARGARCHModel(0.0, 0.0, params(0), params(1), params(2)).logLikelihood(ts)
      }
    })
    val initialGuess = new InitialGuess(Array(.2, .2, .2)) //  TODO
    val maxIter = new MaxIter(10000)
    val maxEval = new MaxEval(10000)
    val optimal = optimizer.optimize(objectiveFunction, gradient, initialGuess, maxIter, maxEval)
    val params = optimal.getPoint
    (new ARGARCHModel(0.0, 0.0, params(0), params(1), params(2)), optimal.getValue)
  }
}

/**
 * A GARCH(1, 1) + AR(1) model, where
 *   y(i) = c + phi(i - 1) + eta(i),
 * and h(i), the variance of eta(i), is given by
 *   h(i) = omega + alpha * eta(i) ** 2 + beta * h(i - 1) ** 2
 */
class ARGARCHModel(
    val c: Double,
    val phi: Double,
    val alpha: Double,
    val beta: Double,
    val omega: Double) extends TimeSeriesFilter {

  /**
   * Takes a time series that is assumed to have this model's characteristics and standardizes it
   * to make the observations i.i.d.
   * @param ts Time series of observations with this model's characteristics.
   * @param dest Array to put the filtered series, can be the same as ts.
   * @return the dest series.
   */
  def standardize(ts: Array[Double], dest: Array[Double]): Array[Double] = {
    var prevEta = ts(0) - c
    var prevVariance = omega / (1.0 - alpha - beta)
    dest(0) = prevEta / math.sqrt(prevVariance)
    for (i <- 1 until ts.length) {
      val variance = omega + alpha * prevEta * prevEta + beta * prevVariance
      val eta = ts(i) - c - phi * ts(i - 1)
      dest(i) = eta / math.sqrt(variance)

      prevEta = eta
      prevVariance = variance
    }
    dest
  }

  def filter(ts: Array[Double], dest: Array[Double]): Array[Double] = {
    var prevVariance = omega / (1.0 - alpha - beta)
    var prevEta = ts(0) * math.sqrt(prevVariance)
    dest(0) = c + prevEta
    for (i <- 1 until ts.length) {
      val variance = omega + alpha * prevEta * prevEta + beta * prevVariance
      val standardizedEta = ts(i)
      val eta = standardizedEta * math.sqrt(variance)
      dest(i) = c + phi * dest(i - 1) + eta

      prevEta = eta
      prevVariance = variance
    }
    dest
  }

  def sampleWithVariances(n: Int, rand: RandomGenerator): (Array[Double], Array[Double]) = {
    val ts = new Array[Double](n)
    val variances = new Array[Double](n)
    variances(0) = omega / (1 - alpha - beta)
    var eta = math.sqrt(variances(0)) * rand.nextGaussian()
    for (i <- 1 until n) {
      variances(i) = omega + beta * variances(i-1) + alpha * eta * eta
      eta = math.sqrt(variances(i)) * rand.nextGaussian()
      ts(i) = c + phi * ts(i - 1) + eta
    }

    (ts, variances)
  }

  def sample(n: Int, rand: RandomGenerator): Array[Double] = sampleWithVariances(n, rand)._1

  /**
   * Based on http://www.unc.edu/~jbhill/Bollerslev_GARCH_1986.pdf
   * @param ts
   * @return
   */
  def logLikelihood(ts: Array[Double]): Double = {
    var sum = 0.0
    iterateWithHAndEta(ts) { (i, h, eta, prevH, prevEta) =>
      sum += -.5 * math.log(h) - .5 * eta * eta / h
    }
    sum + -.5 * math.log(2 * math.Pi) * (ts.length - 1)
  }

  /**
   * Based on http://www.unc.edu/~jbhill/Bollerslev_GARCH_1986.pdf
   * @param ts
   * @return
   */
  private[finance] def gradient(ts: Array[Double]): Array[Double] = {
    var omegaGradient = 0.0
    var alphaGradient = 0.0
    var betaGradient = 0.0
    var omegaDhdtheta = 0.0
    var alphaDhdtheta = 0.0
    var betaDhdtheta = 0.0

    iterateWithHAndEta(ts) { (i, h, eta, prevH, prevEta) =>
      omegaDhdtheta = 1 + beta * omegaDhdtheta
      alphaDhdtheta = prevEta * prevEta + beta * alphaDhdtheta
      betaDhdtheta = prevH + beta * betaDhdtheta

      val multiplier = (eta * eta / (h * h)) - (1 / h)
      omegaGradient += multiplier * omegaDhdtheta
      alphaGradient += multiplier * alphaDhdtheta
      betaGradient += multiplier * betaDhdtheta
    }
    Array(alphaGradient * .5, betaGradient * .5, omegaGradient * .5)
  }

  private def iterateWithHAndEta(ts: Array[Double])
      (fn: (Int, Double, Double, Double, Double) => Unit): Unit = {
    var prevH = omega / (1 - alpha - beta)
    var i = 1
    while (i < ts.length) {
      val h = omega + alpha * ts(i - 1) * ts(i - 1) + beta * prevH

      fn(i, h, ts(i), prevH, ts(i - 1))

      prevH = h
      i += 1
    }
  }
}
