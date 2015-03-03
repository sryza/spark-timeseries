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

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

object Autoregression {
  def lagMatTrimBoth(x: Array[Double], maxLag: Int): Array[Array[Double]] = {
    val numObservations = x.size
    val lagMat = Array.ofDim[Double](numObservations - maxLag, maxLag)
    for (j <- 0 until numObservations - maxLag) {
      for (k <- 0 until maxLag) {
        lagMat(j)(k) = x((numObservations - maxLag - k - 1) + j)
      }
    }
    lagMat
  }

  def fitAutoregression(kAr: Int, endog: Array[Double], method: String, maxLagArg: Option[Int]): AutoregressionModel = {
    val numObservations = endog.size
    val maxLag = maxLagArg.getOrElse(math.round(math.pow(12*(numObservations/100.0), 1/4.0)).toInt)
    val kAr = maxLag

    // Make left hand side
    val Y = endog.slice(kAr, endog.length)
    // Make lagged right hand side
    val X = lagMatTrimBoth(endog, kAr)

    val regression = new OLSMultipleLinearRegression()
    regression.newSampleData(Y, X)
    val params = regression.estimateRegressionParameters()
    // TODO: figure out the constant term
    new AutoregressionModel(params, 0.0)
  }
}

class AutoregressionModel(coefficients: Array[Double], c: Double) {

}
