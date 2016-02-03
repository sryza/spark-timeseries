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

import breeze.linalg
import breeze.linalg.DenseMatrix
import org.scalatest.{FunSuite, ShouldMatchers}

class RegressionARIMASuite extends FunSuite with ShouldMatchers {
  /**
   * Test cochrane orchutt method
   * ref: https://onlinecourses.science.psu.edu/stat501/node/365
   */
  test("Cohchrane-Orcutt-EmpData-With-MaxIter") {
    val metal = Array(44.2, 44.3, 44.4, 43.4, 42.8, 44.3, 44.4, 44.8, 44.4, 43.1, 42.6, 42.4, 42.2,
      41.8, 40.1, 42, 42.4, 43.1, 42.4, 43.1, 43.2, 42.8, 43, 42.8, 42.5, 42.6, 42.3, 42.9, 43.6,
      44.7, 44.5, 45, 44.8, 44.9, 45.2, 45.2, 45, 45.5, 46.2, 46.8, 47.5, 48.3, 48.3, 49.1, 48.9,
      49.4, 50, 50, 49.6, 49.9, 49.6, 50.7, 50.7, 50.9, 50.5, 51.2, 50.7, 50.3, 49.2, 48.1)

    val vendor = Array(322.0, 317, 319, 323, 327, 328, 325, 326, 330, 334, 337, 341, 322, 318, 320,
      326, 332, 334, 335, 336, 335, 338, 342, 348, 330, 326, 329, 337, 345, 350, 351, 354, 355, 357,
      362, 368, 348, 345, 349, 355, 362, 367, 366, 370, 371, 375, 380, 385, 361, 354, 357, 367, 376,
      381, 381, 383, 384, 387, 392, 396)
    val Y = linalg.DenseVector(metal)
    val regressors = new DenseMatrix[Double](vendor.length, 1)
    regressors(::, 0) := linalg.DenseVector(vendor)
    val regARIMA: RegressionARIMAModel = RegressionARIMA.fitModel(
      ts = Y, regressors = regressors, method = "cochrane-orcutt", 1)
    val beta: Array[Double] = regARIMA.regressionCoeff
    beta(0) should equal(28.918 +- 0.01)
    beta(1) should equal(0.0479 +- 0.001)
  }

  /**
   * ref : http://www.ats.ucla.edu/stat/stata/examples/chp/chpstata8.htm
   * data : http://www.ats.ucla.edu/stat/examples/chp/p203.txt
   */
  test("Cochrane-Orcutt-Stock-Data") {
    val expenditure = Array(214.6, 217.7, 219.6, 227.2, 230.9, 233.3, 234.1, 232.3, 233.7, 236.5,
      238.7, 243.2, 249.4, 254.3, 260.9, 263.3, 265.6, 268.2, 270.4, 275.6)

    val stock = Array(159.3, 161.2, 162.8, 164.6, 165.9, 167.9, 168.3, 169.7, 170.5, 171.6, 173.9,
      176.1, 178.0, 179.1, 180.2, 181.2, 181.6, 182.5, 183.3, 184.3)
    val Y = linalg.DenseVector(expenditure)
    val regressors = new DenseMatrix[Double](stock.length, 1)

    regressors(::, 0) := linalg.DenseVector(stock)
    val regARIMA = RegressionARIMA.fitCochraneOrcutt(Y, regressors, 11)
    val beta = regARIMA.regressionCoeff
    val rho = regARIMA.arimaCoeff(0)
    rho should equal(0.8241 +- 0.001)
    beta(0) should equal(-235.4889 +- 0.1)
    beta(1) should equal(2.75306 +- 0.001)
  }
}
