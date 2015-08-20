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
 * are lagged up to degree xMaxLag.
 */
object AutoregressionX {
  /**
   * Fit an autoregressive model with additional exogenous variables. The model predicts a value
   * at time t of a dependent variable, Y, as a function of previous values of Y, and a combination
   * of previous values of exogenous regressors X_i, and current values of exogenous regressors X_i.
   * This is a generalization of an AR model, which is simple an ARX with no exogenous regressors.
   * The fitting procedure here is the same, using least squares. Note that all lags up to the
   * maxlag are included. In the case of the dependent variable the max lag is `yMaxLag`, while
   * for the exogenous variables the max lag is `xMaxLag`, with which each column in the original
   * matrix provided is lagged accordingly.
   * @param y the dependent variable, time series
   * @param x a matric of exgenous variables
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
    // Make left hand side
    val trimY = y(maxLag until y.length)
    // Create predictors
    val predictors = assemblePredictors(y, x, yMaxLag, xMaxLag, includeOriginalX)
    val regression = new OLSMultipleLinearRegression()
    regression.setNoIntercept(noIntercept) // drop intercept in regression
    regression.newSampleData(trimY.toArray, matToRowArrs(predictors))
    val params = regression.estimateRegressionParameters()
    val (c, coeffs) = if (noIntercept) (0.0, params) else (params.head, params.tail)
    new ARXModel(c, coeffs, yMaxLag, xMaxLag, includeOriginalX)
  }

  // Jose note: I realllyyy don't like converting to arrays only to convert back to
  // matrix ... so suggestions appreciated. I initially worked on having a
  // Lag.lagMatTrimBoth(...) for `Matrix`, but ran into some issues due to the current semantics:
  // Namely, Lag.lagMatTrimBoth(Vector(...), 0, false) results in an "empty" breeze matrix
  // but it still has the appropriate number of rows, just 0 columns (and no data).
  // When you call matToRowArrs on that, it returns an array of empty arrays ( of the appropriate
  // length), which is reasonable, and matches the behavior of what happens with
  // Lag.lagMatTrimBoth(Array(...), 0, false). So its consistent. But these kind of matrices,
  // with rows, no columns, and no data, are not very well behaved in breeze. Indexing into them
  // causes issues. Trying to slice causes issues. Reshaping causes issues (sometimes). So I
  // avoided this. Which in turn leads to having to conver to array and then back to avoid this
  // ill behavior.
  def assemblePredictors(
    y: Vector[Double],
    x: Matrix[Double],
    yMaxLag: Int,
    xMaxLag: Int,
    includeOriginalX: Boolean = true): Matrix[Double] = {
    val yArr = y.toArray
    val xArr = matToRowArrs(x)

    val maxLag = max(yMaxLag, xMaxLag)
    // AR terms from dependent variable (autoregressive portion)
    val arY = Lag.lagMatTrimBoth(yArr, yMaxLag)

    // exogenous variables lagged as appropriate, reshape so that well behaved in edge cases
    val laggedX = Lag.lagMatTrimBoth(xArr, xMaxLag)

    // adjust difference in size for arY and laggedX so that they match up
    val arYAdj = arY.drop(maxLag - yMaxLag)
    val laggedXAdj = laggedX.drop(maxLag - xMaxLag)
    val trimmedX = if (includeOriginalX) xArr.drop(maxLag) else Array[Array[Double]]()
    val combinedData = Array(arYAdj, laggedXAdj, trimmedX).flatMap(_.transpose.toSeq.flatten)
    val rows = x.rows - maxLag
    new DenseMatrix(rows, combinedData, offset = 0)
  }
}

// Jose note: not extending timeseries model, since seems to me to be a different type of model
// addingTimeDpendent...etc wouldn't apply here with the original signature, since we need
// exogenous variables provided
/**
 * An autoregressive model with exogenous variables
 * @param c an intercept term, zero if none desired
 * @param coefficients the coefficients for the various terms. The order of coefficients is as
 *                     follows:
 *                     - Autoregressive terms for the dependent variable, in increasing order of lag
 *                     - For each column in the exogenous matrix (in their original order), the
 *                     lagged terms in increasingorder of lag (excluding the non-lagged versions).
 *                     - The coefficients associated with the non-lagged exogenous matrix
 * @param yMaxLag the maximum lag order for the dependent variable
 * @param xMaxLag the maximum lag order for exogenous variables
 * @param includesOriginalX a boolean flag indicating if the non-lagged exogenous variables should
 *                         be included
 */
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