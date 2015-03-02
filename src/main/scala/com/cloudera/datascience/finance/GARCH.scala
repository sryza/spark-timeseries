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
import cern.colt.matrix.DoubleMatrix1D

object GARCH {
  /**
   * @param ts
   * @return The model and its log likelihood on the input data.
   */
  def fitModel(ts: Array[Double]): (GARCHModel, Double) = {
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
    null
  }
}

/**
 * A GARCH(1, 1) + AR(1) model, where
 *   y(i) = c + phi(i - 1) + eta(i),
 * and h(i), the variance of eta(i), is given by
 *   h(i) = omega + alpha * eta(i) ** 2 + beta * h(i - 1) ** 2
 */
class GARCHModel(val c: Double, val phi: Double, val alpha: Double, val beta: Double, val omega: Double) extends TimeSeriesFilter {

  def variances(ts: Array[Double]): Array[Double] = {
    val h = new Array[Double](ts.length)
    val e = new Array[Double](ts.length)

    h(0) = omega / (1.0 - alpha - beta)
    e(0) = ts(0) - kappa
    for (i <- 1 until ts.length) {
      val eval = e(i - 1)
      h(i) = omega + alpha * eval * eval + beta * h(i - 1)
      e(i) = ts(i) - kappa // TODO: also subtract ((element of x) * gamma)
    }
    h
  }

  /**
   * Takes a time series that is assumed to have this model's characteristics and standardizes it
   * to make the observations i.i.d.
   * @param ts Time series of observations with this model's characteristics.
   */
  def standardize(ts: Array[Double]): Array[Double] = {
    val standardized = new Array[Double](ts.length)

    var prevEta = ts(0) - c
    var prevVariance = omega / (1.0 - alpha - beta)
    standardized(0) = prevEta / math.sqrt(prevVariance)
    for (i <- 1 until ts.length) {
      val variance = omega + alpha * prevEta * prevEta + beta * prevVariance
      val eta = ts(i) - c - phi * ts(i - 1)
      standardized(i) = eta / math.sqrt(variance)

      prevEta = eta
      prevVariance = variance
    }
    standardized
  }

  /**
   * Takes a time series of i.i.d. observations and filters it to take on this model's
   * characteristics. Modifies the given array in place.
   * @param ts Time series of i.i.d. observations.
   */
  def filter(ts: Array[Double]): Unit = {
    var prevVariance = omega / (1.0 - alpha - beta)
    var prevEta = ts(0) * math.sqrt(prevVariance)
    ts(0) = c + prevEta
    for (i <- 1 until ts.length) {
      val variance = omega + alpha * prevEta * prevEta + beta * prevVariance
      val standardizedEta = ts(i)
      val eta = standardizedEta * math.sqrt(variance)
      ts(i) = c + phi * ts(i - 1) + eta

      prevEta = eta
      prevVariance = variance
    }
  }
}
