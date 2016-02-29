/**
 * Copyright (c) 2016, Cloudera, Inc. All Rights Reserved.
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

import breeze.linalg._
import com.cloudera.sparkts.Lag
import com.cloudera.sparkts.MatrixUtil.{matToRowArrs, toBreeze}
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

/**
 * Models a time series as a function of itself (autoregressive terms) and exogenous variables, which
 * are lagged up to degree xMaxLag.
 */
object AutoregressionX {
  /**
   * Fit an autoregressive model with additional exogenous variables. The model predicts a value
   * at time t of a dependent variable, Y, as a function of previous values of Y, and a combination
   * of previous values of exogenous regressors X_i, and current values of exogenous regressors X_i.
   * This is a generalization of an AR model, which is simply an ARX with no exogenous regressors.
   * The fitting procedure here is the same, using least squares. Note that all lags up to the
   * maxlag are included. In the case of the dependent variable the max lag is 'yMaxLag', while
   * for the exogenous variables the max lag is 'xMaxLag', with which each column in the original
   * matrix provided is lagged accordingly.
   *
   * @param y the dependent variable, time series
   * @param x a matrix of exogenous variables
   * @param yMaxLag the maximum lag order for the dependent variable
   * @param xMaxLag the maximum lag order for exogenous variables
   * @param includeOriginalX a boolean flag indicating if the non-lagged exogenous variables should
   *                         be included. Default is true
   * @param noIntercept a boolean flag indicating if the intercept should be dropped. Default is
   *                    false
   * @return an ARXModel, which is an autoregressive model with exogenous variables
   */
  def fitModel(
      y: Vector[Double],
      x: Matrix[Double],
      yMaxLag: Int,
      xMaxLag: Int,
      includeOriginalX: Boolean = true,
      noIntercept: Boolean = false): ARXModel = {
    val maxLag = max(yMaxLag, xMaxLag)
    val arrY = y.toArray
    // Make left hand side, note that we must drop the first maxLag terms
    val trimY = arrY.drop(maxLag)
    // Create predictors
    val predictors = assemblePredictors(arrY, matToRowArrs(x), yMaxLag, xMaxLag, includeOriginalX)
    val regression = new OLSMultipleLinearRegression()
    regression.setNoIntercept(noIntercept) // drop intercept in regression
    regression.newSampleData(trimY, predictors)
    val params = regression.estimateRegressionParameters()
    val (c, coeffs) = if (noIntercept) (0.0, params) else (params.head, params.tail)

    new ARXModel(c, coeffs, yMaxLag, xMaxLag, includeOriginalX)
  }


  private[sparkts] def assemblePredictors(
      y: Array[Double],
      x: Array[Array[Double]],
      yMaxLag: Int,
      xMaxLag: Int,
      includeOriginalX: Boolean = true): Array[Array[Double]] = {
    val maxLag = max(yMaxLag, xMaxLag)
    // AR terms from dependent variable (autoregressive portion)
    val arY = Lag.lagMatTrimBoth(y, yMaxLag)
    // exogenous variables lagged as appropriate
    val laggedX = Lag.lagMatTrimBoth(x, xMaxLag)

    // adjust difference in size for arY and laggedX so that they match up
    val arYAdj = arY.drop(maxLag - yMaxLag)

    val laggedXAdj = laggedX.drop(maxLag - xMaxLag)

    val trimmedX = if (includeOriginalX) x.drop(maxLag) else Array[Array[Double]]()

    // combine matrices by concatenating column-wise
    Array(arYAdj, laggedXAdj, trimmedX).transpose.map(_.reduceLeft(_ ++_))
  }
}

/**
 * An autoregressive model with exogenous variables.
 *
 * @param c An intercept term, zero if none desired.
 * @param coefficients The coefficients for the various terms. The order of coefficients is as
 *                     follows:
 *                     - Autoregressive terms for the dependent variable, in increasing order of lag
 *                     - For each column in the exogenous matrix (in their original order), the
 *                     lagged terms in increasing order of lag (excluding the non-lagged versions).
 *                     - The coefficients associated with the non-lagged exogenous matrix
 * @param yMaxLag The maximum lag order for the dependent variable.
 * @param xMaxLag The maximum lag order for exogenous variables.
 * @param includesOriginalX A boolean flag indicating if the non-lagged exogenous variables should
 *                         be included.
 */
class ARXModel(
    val c: Double,
    val coefficients: Array[Double],
    val yMaxLag: Int,
    val xMaxLag: Int,
    includesOriginalX: Boolean) {

  def predict(y: Vector[Double], x: Matrix[Double]): Vector[Double] = {
    val predictors = AutoregressionX.assemblePredictors(y.toArray, matToRowArrs(x), yMaxLag,
      xMaxLag, includesOriginalX)
    val results = DenseVector.zeros[Double](predictors.length)

    for ((rowArray, rowIndex) <- predictors.zipWithIndex) {
      results(rowIndex) = c
      for ((value, colIndex) <- rowArray.zipWithIndex) {
        results(rowIndex) += value * coefficients(colIndex)
      }
    }

    results
  }
}
