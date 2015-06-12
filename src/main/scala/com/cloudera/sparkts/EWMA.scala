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
import org.apache.commons.math3.optim.{MaxEval, MaxIter, InitialGuess, SimpleValueChecker}
import org.apache.commons.math3.optim.nonlinear.scalar.{ObjectiveFunction,
ObjectiveFunctionGradient}
import org.apache.commons.math3.optim.nonlinear.scalar.gradient.NonLinearConjugateGradientOptimizer
import org.apache.commons.math3.random.RandomGenerator

object EWMA {
  /**
   * Fits an EWMA model to a time series. Uses the first point in the time series as a starting
   * value. Uses mean squared error as an object function to optimize to find smoothing paramter
   * The model for EWMA is recursively defined as Z_t = (1 - s) * X_t-1 + s * Z_{t-1}, where
   * s is the smoothing parameter, X is the original series, and Z is the smoothed series
   * //TODO: add reference for this notation
   */
  def fitModel(ts: Vector[Double]): EWMAModel = {
    val optimizer = new NonLinearConjugateGradientOptimizer(
      NonLinearConjugateGradientOptimizer.Formula.FLETCHER_REEVES,
      new SimpleValueChecker(1e-6, 1e-6)) //taken from GARCH
    val gradient = new ObjectiveFunctionGradient(new MultivariateVectorFunction() {
      def value(params: Array[Double]): Array[Double] = {
        new EWMAModel(params(0)).gradient(ts)
      }
    })
    val objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        new EWMAModel(params(0)).mse(ts)
      }
    })
    val initialGuess = new InitialGuess(Array(0.94)) // TODO: based off of JPM RiskMetrics, source!
    val maxIter = new MaxIter(10000)
    val maxEval = new MaxEval(10000)
    val optimal = optimizer.optimize(objectiveFunction, gradient, initialGuess, maxIter, maxEval)
    val params = optimal.getPoint
    new EWMAModel(params(0))
  }
}

class EWMAModel (val smoothing: Double) extends TimeSeriesModel {

  /**
   * Calculates the MSE for a given timeseries ts given the smoothing parameter of the current model
   * @param ts
   * @return Mean Squared Error
   */
  private[sparkts] def mse(ts: Vector[Double]): Double = {
    val arrTs = ts.toArray
    val smoothed = new DenseVector(arrTs)
    addTimeDependentEffects(ts, smoothed)
    val n = smoothed.length
    // we divide by 2 * n for convenience of derivative
    sum(smoothed.toArray.zip(arrTs).map { case (yhat, y) => (yhat - y) * (yhat - y) }) / (2 * n)
  }

  /**
   * Calculates gradient for MSE for model fitting
   *
   * @return gradient
   */
  private[sparkts] def gradient(ts: Vector[Double]): Array[Double] = {
    // gradient calculation for MSE
    val arrTs = ts.toArray
    val smoothed = new DenseVector(arrTs)
    addTimeDependentEffects(ts, smoothed)
    val n = smoothed.length
    val arrSmoothed = smoothed.toArray
    val errors = arrSmoothed.zip(arrTs).map { case (yhat, y) => yhat - y }
    // note we exclude Yhat0 and Y0, since derivative is not defined there (ie.. Y_-1 and Yhat_-1
    // are unknown. If your data is large, this shouldn't make much of a difference...
    val g = sum(errors.tail.zip(errors).map { case (error, dx) => error * dx }) / n
    Array(g)
  }

  override def removeTimeDependentEffects(ts: Vector[Double], dest: Vector[Double] = null)
    : Vector[Double] = {
    throw new UnsupportedOperationException() //TODO: work on this
  }

  override def addTimeDependentEffects(ts: Vector[Double], dest: Vector[Double])
    : Vector[Double] = {
    // should i keep this more functional style, or stick with below?
    // val arrTs = ts.toArray
    // val start = arrTs.head // Z_0 = X_0 by definition in our implementation
    // val smoothed = arrTs.scanLeft(start) { (z, x) => (1 - smoothing) * x + smoothing * z }

    var prevZ = ts(0)
    var prevX = ts(0)
    dest(0) = prevX // by definition in our model

    for (i <- 1 until ts.length) {
      val smoothed = (1 - smoothing) * prevX + smoothing * prevZ
      dest(i) = (1 - smoothing) * prevX + smoothing * prevZ
      prevZ = smoothed
      prevX = ts(i)
    }
    dest
  }
}
