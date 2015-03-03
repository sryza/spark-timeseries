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

import net.finmath.timeseries.models.parametric.{GARCH => FinmathGARCH, ARMAGARCH}
import org.apache.commons.math3.random.RandomGenerator

object GARCH {
  /**
   * @param ts
   * @return The model and its log likelihood on the input data.
   */
  def fitModel(ts: Array[Double]): (ARGARCHModel, Double) = {
/*    val model = new FinmathGARCH(ts)
    // Fit the model
    val params = model.getBestParameters
    (new GARCHModel(params.get("Alpha"), params.get("Beta"), params.get("Omega")),
      params.get("Likelihood"))*/

/*    val model = new ARMAGARCH(ts)
    // Fit the model
    val params = model.getBestParameters
    (new GARCHModel(params.get("Alpha"), params.get("Beta"), params.get("Omega")),
      params.get("Likelihood"))*/

    /*
    val x = new Array[Double](ts.length)
    val garchRegressNormal = new GarchRegress_Normal(x, ts)
    val params = garchRegressNormal.Calc_MLE_by_NelderMead()
    (new GARCHModel(params(3), params(4), params(2)), 0.0)
    */
    throw new UnsupportedOperationException
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
    var eta = 0.0
    for (i <- 1 until n) {
      variances(i) = omega + beta * variances(i-1) * variances(i-1) + alpha * eta * eta
      eta = math.sqrt(variances(i)) * rand.nextGaussian()
      ts(i) = c + alpha * ts(i - 1) + eta
    }

    (ts, variances)
  }

  def sample(n: Int, rand: RandomGenerator): Array[Double] = sampleWithVariances(n, rand)._1
}
