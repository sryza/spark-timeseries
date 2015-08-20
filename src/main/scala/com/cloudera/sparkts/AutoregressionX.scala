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

import com.cloudera.finance.Util.matToRowArrs
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

/**
 * Models a timeseries as a function of itself (autoregressive terms) and exogenous variables, which
 * are lagged up to degree xMaxLag. If `includeOriginalX` is set to true, the regression also
 * includes non-lagged variables. Returns a model of the ARXModel type
 */
object AutoregressionX {
  /**
   * Fir model using least squares approach
   * @param y
   * @param x
   * @param yMaxLag
   * @param xMaxLag
   * @param includeOriginalX
   * @param noIntercept
   * @return
   */
  def fitModel(
    y: Vector[Double],
    x: DenseMatrix[Double],
    yMaxLag: Int,
    xMaxLag: Int,
    includeOriginalX: Boolean = true,
    noIntercept: Boolean = false): ARXModel = {
    val maxLag = max(yMaxLag, xMaxLag)
    // Make left hand side
    val trimY = y(maxLag until y.length)
    // Create
    val predictors = assemblePredictors(y, x, yMaxLag, xMaxLag, includeOriginalX)
    val regression = new OLSMultipleLinearRegression()
    regression.setNoIntercept(noIntercept) // drop intercept in regression
    regression.newSampleData(trimY.toArray, matToRowArrs(predictors))
    val params = regression.estimateRegressionParameters()
    val (c, coeffs) = if (noIntercept) (0.0, params) else (params.head, params.tail)
    new ARXModel(c, coeffs, yMaxLag, xMaxLag, includeOriginalX)
  }

  def assemblePredictors(
    y: Vector[Double],
    x: Matrix[Double],
    yMaxLag: Int,
    xMaxLag: Int,
    includeOriginalX: Boolean = true): Matrix[Double] = {
    val maxLag = max(yMaxLag, xMaxLag)
    // AR terms from dependent variable (autoregressive portion)
    val arY = Lag.lagMatTrimBoth(y, yMaxLag).toDenseMatrix
    // Exogenous variables lagged as appropriate
    val laggedX = Lag.lagMatTrimBoth(x, xMaxLag).toDenseMatrix
    // adjust difference in size for arY and laggedX
    val arYAdj = arY((maxLag - yMaxLag) to -1, ::)
    val laggedXAdj = laggedX((maxLag - xMaxLag) to -1, ::)
    if (includeOriginalX){
      val xDense = x.toDenseMatrix
      DenseMatrix.horzcat(arYAdj, xDense(maxLag to -1, ::), laggedXAdj)
    } else {
      DenseMatrix.horzcat(arYAdj, laggedXAdj)
    }
  }
}

// Autoregressive
class ARXModel(
    val c: Double,
    val coefficients: Array[Double],
    val yMaxLag: Int,
    val xMaxLag: Int,
    includesOriginalX: Boolean) {
  def predict(y: Vector[Double], x: Matrix[Double]): Vector[Double] = {
    val predictors = AutoregressionX.assemblePredictors(y, x, yMaxLag, xMaxLag, includesOriginalX)
    val predictorsN = predictors.cols
    val resultsN = predictors.rows
    val results = DenseVector.zeros[Double](resultsN)

    var i = 0
    var j = 0

    while (i < resultsN) {
      results(i) = c
      while (j < predictorsN) {
        results(i) += predictors(i, j) * coefficients(j)
        j += 1
      }
      i += 1
    }
    results
  }
}