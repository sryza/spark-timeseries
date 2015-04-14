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
import breeze.numerics.polyval

import com.cloudera.finance.Util

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

/**
 * Adapted from statsmodels:
 *    https://github.com/statsmodels/statsmodels/blob/master/statsmodels/tsa/stattools.py
 *    https://github.com/statsmodels/statsmodels/blob/master/statsmodels/tsa/adfvalues.py
 */
object TimeSeriesStatisticalTests {
  private val ADF_TAU_STAR = Map[String, Array[Double]](
    "nc" -> Array(-1.04, -1.53, -2.68, -3.09, -3.07, -3.77),
    "c" -> Array(-1.61, -2.62, -3.13, -3.47, -3.78, -3.93),
    "ct" -> Array(-2.89, -3.19, -3.50, -3.65, -3.80, -4.36),
    "ctt" -> Array(-3.21, -3.51, -3.81, -3.83, -4.12, -4.63)
  )

  private val ADF_TAU_MIN = Map[String, Array[Double]](
    "nc" -> Array(-19.04, -19.62, -21.21, -23.25, -21.63, -25.74),
    "c" -> Array(-18.83, -18.86, -23.48, -28.07, -25.96, -23.27),
    "ct" -> Array(-16.18, -21.15, -25.37, -26.63, -26.53, -26.18),
    "ctt" -> Array(-17.17, -21.1, -24.33, -24.03, -24.33, -28.22)
  )

  private val ADF_TAU_MAX = Map[String, Array[Double]](
    "nc" -> Array(Double.PositiveInfinity, 1.51, 0.86, 0.88, 1.05, 1.24),
    "c" -> Array(2.74, 0.92, 0.55, 0.61, 0.79, 1),
    "ct" -> Array(0.7, 0.63, 0.71, 0.93, 1.19, 1.42),
    "ctt" -> Array(0.54, 0.79, 1.08, 1.43, 3.49, 1.92)
  )

  private val ADF_TAU_SMALLP = Map[String, Array[Array[Double]]](
    "nc" -> Array(
      Array(0.6344, 1.2378, 3.2496 * 1e-2),
      Array(1.9129, 1.3857, 3.5322 * 1e-2),
      Array(2.7648, 1.4502, 3.4186 * 1e-2),
      Array(3.4336, 1.4835, 3.19 * 1e-2),
      Array(4.0999, 1.5533, 3.59 * 1e-2),
      Array(4.5388, 1.5344, 2.9807 * 1e-2)
    ),
    "c" -> Array(
      Array(2.1659, 1.4412, 3.8269 * 1e-2),
      Array(2.92, 1.5012, 3.9796 * 1e-2),
      Array(3.4699, 1.4856, 3.164 * 1e-2),
      Array(3.9673, 1.4777, 2.6315 * 1e-2),
      Array(4.5509, 1.5338, 2.9545 * 1e-2),
      Array(5.1399, 1.6036, 3.4445 * 1e-2)
    ),
    "ct" -> Array(
      Array(3.2512, 1.6047, 4.9588 * 1e-2),
      Array(3.6646, 1.5419, 3.6448 * 1e-2),
      Array(4.0983, 1.5173, 2.9898 * 1e-2),
      Array(4.5844, 1.5338, 2.8796 * 1e-2),
      Array(5.0722, 1.5634, 2.9472 * 1e-2),
      Array(5.53, 1.5914, 3.0392 * 1e-2)
    ),
    "ctt" -> Array(
      Array(4.0003, 1.658, 4.8288 * 1e-2),
      Array(4.3534, 1.6016, 3.7947 * 1e-2),
      Array(4.7343, 1.5768, 3.2396 * 1e-2),
      Array(5.214, 1.6077, 3.3449 * 1e-2),
      Array(5.6481, 1.6274, 3.3455 * 1e-2),
      Array(5.9296, 1.5929, 2.8223 * 1e-2)
    )
  )

  private val ADF_LARGE_SCALING = Array(1.0, 1e-1, 1e-1, 1e-2)
  private val ADF_TAU_LARGEP = Map[String, Array[Array[Double]]](
    "nc" -> Array(
      Array(0.4797,9.3557,-0.6999,3.3066),
      Array(1.5578,8.558,-2.083,-3.3549),
      Array(2.2268,6.8093,-3.2362,-5.4448),
      Array(2.7654,6.4502,-3.0811,-4.4946),
      Array(3.2684,6.8051,-2.6778,-3.4972),
      Array(3.7268,7.167,-2.3648,-2.8288)
    ),
    "c" -> Array(
      Array(1.7339,9.3202,-1.2745,-1.0368),
      Array(2.1945,6.4695,-2.9198,-4.2377),
      Array(2.5893,4.5168,-3.6529,-5.0074),
      Array(3.0387,4.5452,-3.3666,-4.1921),
      Array(3.5049,5.2098,-2.9158,-3.3468),
      Array(3.9489,5.8933,-2.5359,-2.721)
    ),
    "ct" -> Array(
      Array(2.5261,6.1654,-3.7956,-6.0285),
      Array(2.85,5.272,-3.6622,-5.1695),
      Array(3.221,5.255,-3.2685,-4.1501),
      Array(3.652,5.9758,-2.7483,-3.2081),
      Array(4.0712,6.6428,-2.3464,-2.546),
      Array(4.4735,7.1757,-2.0681,-2.1196)
    ),
    "ctt" -> Array(
      Array(3.0778,4.9529,-4.1477,-5.9359),
      Array(3.4713,5.967,-3.2507,-4.2286),
      Array(3.8637,6.7852,-2.6286,-3.1381),
      Array(4.2736,7.6199,-2.1534,-2.4026),
      Array(4.6679,8.2618,-1.822,-1.9147),
      Array(5.0009,8.3735,-1.6994,-1.6928)
    )
  ).mapValues {
    arr => arr.map {
      subarr => (0 until 4).map(i => ADF_LARGE_SCALING(i) * subarr(i)).toArray
    }.toArray
  }

  /**
   * Returns MacKinnon's approximate p-value for the given test statistic.
   *
   * MacKinnon, J.G. 1994  "Approximate Asymptotic Distribution Functions for
   *    Unit-Root and Cointegration Tests." Journal of Business & Economics
   *    Statistics, 12.2, 167-76.
   *
   * @param testStat "T-value" from an Augmented Dickey-Fuller regression.
   * @param regression The method of regression that was used. Following MacKinnon's notation, this
   *                   can be "c" for constant, "nc" for no constant, "ct" for constant and trend,
   *                   and "ctt" for constant, trend, and trend-squared.
   * @param n The number of series believed to be I(1). For (Augmented) Dickey-Fuller n = 1.
   * @return The p-value for the ADF statistic using MacKinnon 1994.
   */
  private def mackinnonp(testStat: Double, regression: String = "c", n: Int = 1): Double = {
    val maxStat = ADF_TAU_MAX(regression)
    val minStat = ADF_TAU_MIN(regression)
    val starStat = ADF_TAU_STAR(regression)
    if (testStat > maxStat(n - 1)) {
      return 1.0
    } else if (testStat < minStat(n - 1)) {
      return 0.0
    }
    val tauCoef = if (testStat <= starStat(n - 1)) {
      ADF_TAU_SMALLP(regression)(n - 1)
    } else {
      ADF_TAU_LARGEP(regression)(n - 1)
    }
    new NormalDistribution().cumulativeProbability(polyval(tauCoef.reverse, testStat))
  }

  def vanderflipped(vec: Array[Double], n: Int): Matrix[Double] = {
    val numRows = vec.size
    val matArr = Array.fill[Double](numRows * n)(1.0)
    val mat = new DenseMatrix[Double](numRows, matArr, 0)

    for (c <- 1 until n) {
      for (r <- 0 until numRows) {
        mat.update(r, c, vec(r) * mat(r, c - 1))
      }
    }
    mat
  }

  def addTrend(mat: Matrix[Double], trend: String = "c", prepend: Boolean = false)
    : Matrix[Double] = {
    val trendOrder = trend.toLowerCase match {
      case "c" => 0
      case "ct" | "t" => 1
      case "ctt" => 2
      case _ => throw new IllegalArgumentException(s"Trend $trend is not c, ct, or ctt")
    }

    val nObs = mat.rows
    var trendMat = vanderflipped((1 until nObs + 1).map(_.toDouble).toArray, trendOrder + 1)

    if (trend == "t") {
      trendMat = trendMat(0 to trendMat.rows, 1 to 2)
    }

    if (prepend) {
      DenseMatrix.horzcat(trendMat, mat)
    } else {
      DenseMatrix.horzcat(mat, trendMat)
    }
  }

  /**
   * Augmented Dickey-Fuller test for a unit root in a univariate time series.
   *
   * @param ts The time series.
   * @return A tuple containing the test statistic and p value.
   */
  def adftest(ts: Vector[Double], maxLag: Int, regression: String = "c"): (Double, Double) = {
    val tsDiff: DenseVector[Double] = diff(ts.toDenseVector)
    val lagMat = Lag.lagMatTrimBoth(tsDiff, maxLag, true)
    val nObs = lagMat.rows

    // replace 0 tsDiff with level of ts
    // TODO: unnecessary extra copying here
    // TODO: are indices off by one?
    lagMat(0 until nObs, 0 to 0) :=
      ts(ts.length - nObs - 1 until ts.length - 1).toDenseVector.toDenseMatrix.t
    // trim
    val tsdShort = tsDiff(tsDiff.length - nObs to tsDiff.length - 1)

    val ols = new OLSMultipleLinearRegression()
    ols.setNoIntercept(true)
    if (regression != "nc") {
      val withTrend = Util.matToRowArrs(addTrend(lagMat, regression))
      ols.newSampleData(tsdShort.toArray, withTrend)
    } else {
      ols.newSampleData(tsdShort.toArray, Util.matToRowArrs(lagMat))
    }

    val olsParamStandardErrors = ols.estimateRegressionParametersStandardErrors()
    val coefficients = ols.estimateRegressionParameters()
    val adfStat = coefficients(0) / olsParamStandardErrors(0)

    val pValue = mackinnonp(adfStat, regression, 1)
    (adfStat, pValue)
  }

  /**
   * Durbin-Watson test for serial correlation.
   *
   * @return The Durbin-Watson test statistic.  A value close to 0.0 gives evidence for positive
   *         serial correlation, a value close to 4.0 gives evidence for negative serial
   *         correlation, and a value close to 2.0 gives evidence for no serial correlation.
   */
  def dwtest(residuals: Vector[Double]): Double = {
    var residsSum = residuals(0) * residuals(0)
    var diffsSum = 0.0
    var i = 1
    while (i < residuals.length) {
      residsSum += residuals(i) * residuals(i)
      val diff = residuals(i) - residuals(i - 1)
      diffsSum += diff * diff
      i += 1
    }
    diffsSum / residsSum
  }
}
