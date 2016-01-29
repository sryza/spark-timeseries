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

import org.apache.commons.math3.analysis.{MultivariateFunction, MultivariateVectorFunction}
import org.apache.commons.math3.optim.nonlinear.scalar.gradient.NonLinearConjugateGradientOptimizer
import org.apache.commons.math3.optim.nonlinear.scalar.{ObjectiveFunction,
  ObjectiveFunctionGradient}
import org.apache.commons.math3.optim.{InitialGuess, MaxEval, MaxIter, SimpleValueChecker}
import org.apache.commons.math3.random.RandomGenerator
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}

object GARCH {
  /**
   * Fits a GARCH(1, 1) model to the given time series.
   *
   * @param ts The time series to fit the model to.
   * @return The model.
   */
  def fitModel(ts: Vector): GARCHModel = {
    val optimizer = new NonLinearConjugateGradientOptimizer(
      NonLinearConjugateGradientOptimizer.Formula.FLETCHER_REEVES,
      new SimpleValueChecker(1e-6, 1e-6))
    val gradient = new ObjectiveFunctionGradient(new MultivariateVectorFunction() {
      def value(params: Array[Double]): Array[Double] = {
        new GARCHModel(params(0), params(1), params(2)).gradient(ts)
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

object ARGARCH {
  /**
   * Fits an AR(1) + GARCH(1, 1) model to the given time series.
   *
   * @param ts The time series to fit the model to.
   * @return The model.
   */
  def fitModel(ts: Vector): ARGARCHModel = {
    val arModel = Autoregression.fitModel(ts)
    val residuals = arModel.removeTimeDependentEffects(ts, Vectors.zeros(ts.size))
    val garchModel = GARCH.fitModel(residuals)
    new ARGARCHModel(arModel.c, arModel.coefficients(0), garchModel.omega, garchModel.alpha,
      garchModel.beta)
  }
}

class GARCHModel(
    val omega: Double,
    val alpha: Double,
    val beta: Double) extends TimeSeriesModel {

  /**
   * Returns the log likelihood of the parameters on the given time series.
   *
   * Based on http://www.unc.edu/~jbhill/Bollerslev_GARCH_1986.pdf
   */
  def logLikelihood(ts: Vector): Double = {
    var sum = 0.0
    iterateWithHAndEta(ts) { (i, h, eta, prevH, prevEta) =>
      sum += -.5 * math.log(h) - .5 * eta * eta / h
    }
    sum + -.5 * math.log(2 * math.Pi) * (ts.size - 1)
  }

  /**
   * Find the gradient of the log likelihood with respect to the given time series.
   *
   * Based on http://www.unc.edu/~jbhill/Bollerslev_GARCH_1986.pdf
   * @return an 3-element array containing the gradient for the alpha, beta, and omega parameters.
   */
  private[sparkts] def gradient(ts: Vector): Array[Double] = {
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

  private def iterateWithHAndEta(ts: Vector)
                                (fn: (Int, Double, Double, Double, Double) => Unit): Unit = {
    var prevH = omega / (1 - alpha - beta)
    var i = 1
    while (i < ts.size) {
      val h = omega + alpha * ts(i - 1) * ts(i - 1) + beta * prevH

      fn(i, h, ts(i), prevH, ts(i - 1))

      prevH = h
      i += 1
    }
  }

  def removeTimeDependentEffects(ts: Vector, dest: Vector): Vector = {
    var prevEta = ts(0)
    var prevVariance = omega / (1.0 - alpha - beta)
    val destArr = dest.toArray
    destArr(0) = prevEta / math.sqrt(prevVariance)
    for (i <- 1 until ts.size) {
      val variance = omega + alpha * prevEta * prevEta + beta * prevVariance
      val eta = ts(i)
      destArr(i) = eta / math.sqrt(variance)

      prevEta = eta
      prevVariance = variance
    }
    new DenseVector(destArr)
  }

  override def addTimeDependentEffects(ts: Vector, dest: Vector): Vector = {
    var prevVariance = omega / (1.0 - alpha - beta)
    var prevEta = ts(0) * math.sqrt(prevVariance)
    val destArr = dest.toArray
    destArr(0) = prevEta
    for (i <- 1 until ts.size) {
      val variance = omega + alpha * prevEta * prevEta + beta * prevVariance
      val standardizedEta = ts(i)
      val eta = standardizedEta * math.sqrt(variance)
      destArr(i) = eta

      prevEta = eta
      prevVariance = variance
    }
    new DenseVector(destArr)
  }

  private def sampleWithVariances(n: Int, rand: RandomGenerator): (Array[Double], Array[Double]) = {
    val ts = new Array[Double](n)
    val variances = new Array[Double](n)
    variances(0) = omega / (1 - alpha - beta)
    var eta = math.sqrt(variances(0)) * rand.nextGaussian()
    for (i <- 1 until n) {
      variances(i) = omega + beta * variances(i-1) + alpha * eta * eta
      eta = math.sqrt(variances(i)) * rand.nextGaussian()
      ts(i) = eta
    }

    (ts, variances)
  }

  /**
   * Samples a random time series of a given length with the properties of the model.
   *
   * @param n The length of the time series to sample.
   * @param rand The random generator used to generate the observations.
   * @return The samples time series.
   */
  def sample(n: Int, rand: RandomGenerator): Array[Double] = sampleWithVariances(n, rand)._1
}

/**
 * A GARCH(1, 1) + AR(1) model, where
 *   y(i) = c + phi * y(i - 1) + eta(i),
 * and h(i), the variance of eta(i), is given by
 *   h(i) = omega + alpha * eta(i) ** 2 + beta * h(i - 1) ** 2
 *
 * @param c The constant term.
 * @param phi The autoregressive term.
 * @param omega The constant term in the variance.
 */
class ARGARCHModel(
    val c: Double,
    val phi: Double,
    val omega: Double,
    val alpha: Double,
    val beta: Double) extends TimeSeriesModel {

  override def removeTimeDependentEffects(ts: Vector, dest: Vector): Vector = {
    var prevEta = ts(0) - c
    var prevVariance = omega / (1.0 - alpha - beta)
    val destArr = dest.toArray
    destArr(0) = prevEta / math.sqrt(prevVariance)
    for (i <- 1 until ts.size) {
      val variance = omega + alpha * prevEta * prevEta + beta * prevVariance
      val eta = ts(i) - c - phi * ts(i - 1)
      destArr(i) = eta / math.sqrt(variance)

      prevEta = eta
      prevVariance = variance
    }
    new DenseVector(destArr)
  }

  override def addTimeDependentEffects(ts: Vector, dest: Vector): Vector = {
    var prevVariance = omega / (1.0 - alpha - beta)
    var prevEta = ts(0) * math.sqrt(prevVariance)
    val destArr = dest.toArray
    destArr(0) = c + prevEta
    for (i <- 1 until ts.size) {
      val variance = omega + alpha * prevEta * prevEta + beta * prevVariance
      val standardizedEta = ts(i)
      val eta = standardizedEta * math.sqrt(variance)
      destArr(i) = c + phi * dest(i - 1) + eta

      prevEta = eta
      prevVariance = variance
    }
    new DenseVector(destArr)
  }

  private def sampleWithVariances(n: Int, rand: RandomGenerator): (Array[Double], Array[Double]) = {
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

  /**
   * Samples a random time series of a given length with the properties of the model.
   *
   * @param n The length of the time series to sample.
   * @param rand The random generator used to generate the observations.
   * @return The samples time series.
   */
  def sample(n: Int, rand: RandomGenerator): Array[Double] = sampleWithVariances(n, rand)._1
}

class EGARCHModel(
    val omega: Double,
    val alpha: Double,
    val beta: Double) extends TimeSeriesModel {

  /**
   * Returns the log likelihood of the parameters on the given time series.
   *
   * Based on http://swopec.hhs.se/hastef/papers/hastef0564.pdf
   */
  def logLikelihood(ts: Array[Double]): Double = {
    throw new UnsupportedOperationException()
  }

  override def removeTimeDependentEffects(ts: Vector, dest: Vector): Vector = {
    throw new UnsupportedOperationException()
  }

  override def addTimeDependentEffects(ts: Vector, dest: Vector): Vector = {
    throw new UnsupportedOperationException()
  }
}
