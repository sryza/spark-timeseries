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

import org.apache.commons.math3.random.MersenneTwister

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class AutoregressionXSuite extends FunSuite {
  val rand = new MersenneTwister(10L)
  val nRows = 1000
  val nCols = 2
  val X = Array.fill(nRows, nCols)(rand.nextGaussian())
  val intercept = rand.nextGaussian * 10

  // tests an autoregressive model where the exogenous variables are not lagged
  test("fit ARX(1, 0, true)") {
    val xCoeffs = Array(0.8, 0.2)
    val rawY = X.map(_.zip(xCoeffs).map { case (b, v) => b * v }.sum + intercept)
    val arCoeff = 0.4
    val y = rawY.scanLeft(0.0) { case (priorY, currY) => currY + priorY * arCoeff }.tail
    val dy = new DenseVector(y)
    val dx = new DenseMatrix(rows = X.length, cols = X.head.length, data = X.transpose.flatten)
    val model = AutoregressionX.fitModel(dy, dx, 1, 0, includeOriginalX = true)
    val combinedCoeffs = Array(arCoeff) ++ xCoeffs

    model.c should be (intercept +- 1e-4)
    for (i <- combinedCoeffs.indices) {
      model.coefficients(i) should be (combinedCoeffs(i) +- 1e-4)
    }
  }

  // tests a model with no autoregressive term but with lagged exogenous variables
  test("fit ARX(0, 1, false) model") {
    val xCoeffs = Array(0.4, 0.15)
    val xLagged = Lag.lagMatTrimBoth(X, 1)
    val y = xLagged.map(_.zip(xCoeffs).map { case (b, v) => b * v }.sum + intercept)
    val dy = new DenseVector(Array(0.0) ++ y)
    // note that we provide the original X matrix to the fitting functiond
    val dx = new DenseMatrix(rows = X.length, cols = X.head.length, data = X.transpose.flatten)
    val model = AutoregressionX.fitModel(dy, dx, 0, 1, includeOriginalX = false)

    model.c should be (intercept +- 1e-4)
    for (i <- xCoeffs.indices) {
      model.coefficients(i) should be (xCoeffs(i) +- 1e-4)
    }
  }

  // this test simply reduces to a normal regression model
  test("fit ARX(0, 0, true) model") {
    // note that
    val xCoeffs = Array(0.8, 0.2)
    val y = X.map(_.zip(xCoeffs).map { case (b, v) => b * v }.sum + intercept)
    val dy = new DenseVector(y)
    val dx = new DenseMatrix(rows = X.length, cols = X.head.length, data = X.transpose.flatten)
    val model = AutoregressionX.fitModel(dy, dx, 0, 0, includeOriginalX = true)

    model.c should be (intercept +- 1e-4)
    for (i <- xCoeffs.indices) {
      model.coefficients(i) should be (xCoeffs(i) +- 1e-4)
    }
  }

  // tests a model with no autoregressive term but with lagged exogenous variables
  // of order 2 and inclusive of the original X values
  test("fit ARX(0, 2, true) model") {
    val xLagCoeffs = Array(0.4, 0.15, 0.2, 0.7)
    val xLagged = Lag.lagMatTrimBoth(X, 2)
    val yLaggedPart = xLagged.map(_.zip(xLagCoeffs).map { case (b, v) => b * v }.sum )
    val xNormalCoeffs = Array(0.3, 0.5)
    val yNormalPart = X.map(_.zip(xNormalCoeffs).map { case (b, v) => b * v }.sum )
    val y = yLaggedPart.zip(yNormalPart.drop(2)).map { case (l, n) => l + n + intercept }

    val dy = new DenseVector(Array(0.0, 0.0) ++ y)
    val dx = new DenseMatrix(rows = X.length, cols = X.head.length, data = X.transpose.flatten)
    val model = AutoregressionX.fitModel(dy, dx, 0, 2, includeOriginalX = true)
    val combinedCoeffs = xLagCoeffs ++ xNormalCoeffs

    model.c should be (intercept +- 1e-4)
    for (i <- combinedCoeffs.indices) {
      model.coefficients(i) should be (combinedCoeffs(i) +- 1e-4)
    }
  }

  test("fit ARX(1, 1, false) model") {
    val xCoeffs = Array(0.8, 0.2)
    val xLagged = Lag.lagMatTrimBoth(X, 1)
    val rawY = xLagged.map(_.zip(xCoeffs).map { case (b, v) => b * v }.sum + intercept)
    val arCoeff = 0.4
    val y = rawY.scanLeft(0.0) { case (priorY, currY) => currY + priorY * arCoeff }.tail
    val dy = new DenseVector(Array(0.0) ++ y)
    val dx = new DenseMatrix(rows = X.length, cols = X.head.length, data = X.transpose.flatten)
    val model = AutoregressionX.fitModel(dy, dx, 1, 1, includeOriginalX = false)
    val combinedCoeffs = Array(arCoeff) ++ xCoeffs

    model.c should be (intercept +- 1e-4)
    for (i <- combinedCoeffs.indices) {
      model.coefficients(i) should be (combinedCoeffs(i) +- 1e-4)
    }
  }
}
