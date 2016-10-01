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
package com.cloudera.sparkts.stats

import breeze.numerics.polyval
import breeze.linalg.{DenseMatrix => BreezeDenseMatrix}
import com.cloudera.sparkts.{Lag, MatrixUtil, UnivariateTimeSeries}
import com.cloudera.sparkts.MatrixUtil.{toBreeze, fromBreeze}
import org.apache.commons.math3.distribution.{ChiSquaredDistribution, NormalDistribution}
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.mllib.linalg._

import scala.collection.immutable.ListMap

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
    }
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
    if (testStat > maxStat(n - 1)) {
      return 1.0
    }
    val minStat = ADF_TAU_MIN(regression)
    if (testStat < minStat(n - 1)) {
      return 0.0
    }
    val starStat = ADF_TAU_STAR(regression)
    val tauCoef = if (testStat <= starStat(n - 1)) {
      ADF_TAU_SMALLP(regression)(n - 1)
    } else {
      ADF_TAU_LARGEP(regression)(n - 1)
    }
    new NormalDistribution().cumulativeProbability(polyval(tauCoef, testStat))
  }

  private def vanderflipped(vec: Array[Double], n: Int): Matrix = {
    val numRows = vec.length
    val matArr = Array.fill[Double](numRows * n)(1.0)
    val mat = new DenseMatrix(numRows, n, matArr)
    val breezeMat = toBreeze(mat)

    for (c <- 1 until n) {
      for (r <- 0 until numRows) {
        breezeMat(r, c) = vec(r) * mat(r, c - 1)
      }
    }
    mat
  }

  private def addTrend(mat: Matrix, trend: String = "c", prepend: Boolean = false): Matrix = {
    val trendOrder = trend.toLowerCase match {
      case "c" => 0
      case "ct" | "t" => 1
      case "ctt" => 2
      case _ => throw new IllegalArgumentException(s"Trend $trend is not c, ct, or ctt")
    }

    val nObs = mat.numRows
    var trendMat = toBreeze(
      vanderflipped((1 until nObs + 1).map(_.toDouble).toArray, trendOrder + 1))

    if (trend == "t") {
      trendMat = trendMat(0 to trendMat.rows, 1 to 2)
    }

    fromBreeze(if (prepend) {
      BreezeDenseMatrix.horzcat(trendMat, toBreeze(mat))
    } else {
      BreezeDenseMatrix.horzcat(toBreeze(mat), trendMat)
    })
  }

  /**
   * Augmented Dickey-Fuller test for a unit root in a univariate time series.
   *
   * The null hypothesis is that the time series contains a unit root, implying that
   * differencing is required to make it stationary.  The alternative hypothesis is that the
   * time series is stationary.  Lower values of the test statistic imply lower p-values,
   * and thus higher likelihood of rejecting the null hypothesis.
   *
   * @param ts The time series.
   * @return A tuple containing the test statistic and p value.
   */
  def adftest(ts: Vector, maxLag: Int, regression: String = "c"): (Double, Double) = {
    val breezeTs = toBreeze(ts)
    
    // The vector that we get back from differencesAtLag preserves the first element so that so that the vector
    // returned is size-preserving.  For ADF, we just want the differenced values, so we slice to get a vector
    // that excludes the first element.
    val tsDiff = new DenseVector(UnivariateTimeSeries.differencesAtLag(ts, 1).toArray.slice(1, ts.size))
    val lagMat = Lag.lagMatTrimBoth(tsDiff, maxLag, true)
    val nObs = lagMat.numRows

    // replace 0 tsDiff with level of ts
    // TODO: unnecessary extra copying here
    // TODO: are indices off by one?
    toBreeze(lagMat)(0 until nObs, 0 to 0) :=
      breezeTs(ts.size - nObs - 1 until ts.size - 1).toDenseVector.toDenseMatrix.t
    // trim
    val tsdShort = toBreeze(tsDiff)(tsDiff.size - nObs to tsDiff.size - 1)

    val ols = new OLSMultipleLinearRegression()
    ols.setNoIntercept(true)
    if (regression != "nc") {
      val withTrend = MatrixUtil.matToRowArrs(addTrend(lagMat, regression))
      ols.newSampleData(tsdShort.toArray, withTrend)
    } else {
      ols.newSampleData(tsdShort.toArray, MatrixUtil.matToRowArrs(lagMat))
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
  def dwtest(residuals: Vector): Double = {
    var residsSum = residuals(0) * residuals(0)
    var diffsSum = 0.0
    var i = 1
    while (i < residuals.size) {
      residsSum += residuals(i) * residuals(i)
      val diff = residuals(i) - residuals(i - 1)
      diffsSum += diff * diff
      i += 1
    }
    diffsSum / residsSum
  }

  /**
   * Breusch-Godfrey test for serial correlation in a model
   * The statistic asymptotically follows an X^2 distribution with maxLag degrees of freedom,
   * and provides a test for the null hypothesis of lack of serial correlation up to degree maxLag
   * From http://en.wikipedia.org/wiki/Breusch%E2%80%93Godfrey_test:
   * Given estimated residuals u_hat_t from an OLS model of the form
   * y_t = a0 + a1 * x1_t + a2 * x2_t +  ... + u_t
   * We calculate an auxiliary regression of the form:
   * u_hat_t = a0 + a1 * x1_t + a2 * x2_t + ... + p1 * u_hat_t-1 + p2 * u_hat_t-2 ...
   * Our test statistic is then (# of obs - maxLag) * (R^2 of the auxiliary regression)
   * @return The Breusch-Godfrey statistic and p value
   */
  def bgtest(residuals: Vector, factors: Matrix, maxLag: Int): (Double, Double) = {
    val origResiduals = residuals.toArray
    val origFactors = MatrixUtil.matToRowArrs(factors) // X (wiki)
    // auxiliary regression model
    val lagResids = Lag.lagMatTrimBoth(origResiduals, maxLag, false) // u_hat_lagged (wiki)
    val nObs = lagResids.length
    val dropLen = residuals.size - nObs // drop x # of elements to run new regression
    val auxOLS = new OLSMultipleLinearRegression() // auxiliary OLS for bg test
    val auxFactors = origFactors.drop(dropLen).zip(lagResids).map { case (x, u_t) => x ++ u_t }
    auxOLS.newSampleData(origResiduals.drop(dropLen), auxFactors) // u_hat= A*X + P*u_hat_lagged + e
    val bgstat = nObs * auxOLS.calculateRSquared()
    (bgstat, 1 - new ChiSquaredDistribution(maxLag).cumulativeProbability(bgstat))
  }

  /**
   * Ljung-Box test for serial correlation in residuals up to lag `maxLag`. The null hypothesis
   * is that values are independently distributed up to the given lag. The alternate hypothesis
   * is that serial correlation is present. The test statistic follows a Chi-Squared distribution
   * with `maxLag` degrees of freedom. See [[https://en.wikipedia.org/wiki/Ljung%E2%80%93Box_test]]
   * for more information.
   * @return the test statistic and the p-value associated with it.
   */
  def lbtest(residuals: Vector, maxLag: Int): (Double, Double) = {
    val autoCorrs = UnivariateTimeSeries.autocorr(residuals, maxLag)
    val n = residuals.size
    val adjAutoCorrs = autoCorrs.toArray.zipWithIndex.map { case (p, k) =>
      (p * p) / (n - k - 1)
    }
    val testStatistic = n * (n + 2) * adjAutoCorrs.sum
    val pValue = 1 - new ChiSquaredDistribution(maxLag).cumulativeProbability(testStatistic)
    (testStatistic, pValue)
  }

  /**
   * Breusch-Pagan test for heteroskedasticity in a model
   * The statistic follows a X^2 distribution with (# of regressors - 1) degrees of freedom
   * and provides a test for a null hypothesis of homoskedasticity
   * From http://en.wikipedia.org/wiki/Breusch%E2%80%93Pagan_test
   * Given a vector of estimated residuals (u) from an OLS model, we create an auxiliary regression
   * that models the squared residuals (u^2) as a function of the original regressors (X)
   * u^2 = beta * X
   * We construct our test statistic as (# of observations) * R^2 of our auxiliary regression
   * @return The Breusch-Pagan statistic and p value
   */
  def bptest(residuals: Vector, factors: Matrix): (Double, Double) = {
    val residualsSquared = residuals.toArray.map(x => x * x) // u^2
    val origFactors = MatrixUtil.matToRowArrs(factors) // X
    val auxOLS = new OLSMultipleLinearRegression() // auxiliary OLS for bp test
    auxOLS.newSampleData(residualsSquared, origFactors) // u^2 = beta * X
    val bpstat = residuals.size * auxOLS.calculateRSquared()
    // auxOLS uses intercept term, so (# of regressors - 1) = # factors cols
    val df = factors.numCols
    (bpstat, 1 - new ChiSquaredDistribution(df).cumulativeProbability(bpstat))
  }

  /**
   * Critical values associated with KPSS test when testing with a simple constant
   * Source:
   * Kwiatkowski, D., Phillips, P., Schmidt, P., & Shin, Y.
   * Testing the null hypothesis of stationarity against the alternative of a unit root.
   * Journal of Econometrics, 159-178.
   */
  private val kpssConstantCriticalValues = ListMap(
    0.10 -> 0.347, 0.05 -> 0.463, 0.025 -> 0.574, 0.01 -> 0.739
  )

  /**
   * Critical values associated with KPSS test when testing with a constant and time trend
   * Source:
   * Kwiatkowski, D., Phillips, P., Schmidt, P., & Shin, Y.
   * Testing the null hypothesis of stationarity against the alternative of a unit root.
   * Journal of Econometrics, 159-178.
   */
  private val kpssConstantAndTrendCriticalValues = ListMap(
    0.10 -> 0.119, 0.05 -> 0.146, 0.025 -> 0.176, 0.01 -> 0.216
  )

  /**
   * Performs the KPSS stationarity test (Kwiatkowski, Phillips, Schmidt and Shin). The null
   * hypothesis corresponds to stationarity (level stationarity if method = "c", or trend
   * stationarity if method = "ct"). If method = "c", a regression of the form ts_i = alpha +
   * error_i is fit. If method = "ct", a regression of the form ts_i = alpha + beta * i + error_i.
   * The test then performs a check for the variance of the errors. The null hypothesis of
   * stationarity corresponds to a null hypothesis of variance = 0 for the errors.
   * For more information please see pg 129 of [[http://faculty.washington
   * .edu/ezivot/econ584/notes/unitroot.pdf]]. For the original paper on the test, please see
   * [[http://www.deu.edu.tr/userweb/onder.hanedar/dosyalar/kpss.pdf]].
   * Finally, the current implementation follows R's tseries package implementation closely, which
   * can be found at [[https://cran.r-project.org/web/packages/tseries/index.html]]
   * @param ts time series to test for stationarity
   * @param method "c" or "ct", short for fitting with a constant or a constant and a time trend
   * @return the KPSS statistic and a map of critical values according to the method selected
   */
  def kpsstest(ts: Vector, method: String): (Double, Map[Double, Double]) = {
    require(List("c", "ct").contains(method), "trend must be c or ct")
    val n = ts.size
    // constant
    val c = Array.fill(n)(1.0)
    // create appropriate regression matrix and obtain critical values
    val (regressors, criticalValues) = if (method == "c") {
      (c.map(Array(_)), kpssConstantCriticalValues)
    } else {
      // time trend
      val t = Array.tabulate(n)(x => 1.0 + x.toDouble)
      (Array(c, t).transpose, kpssConstantAndTrendCriticalValues)
    }
    val ols = new OLSMultipleLinearRegression()
    ols.setNoIntercept(true)
    ols.newSampleData(ts.toArray, regressors)
    val residuals = ols.estimateResiduals()
    // sum of (square of cumulative sum of errors)
    val s2 = residuals.scanLeft(0.0)(_ + _).tail.map(math.pow(_, 2)).sum
    // long run variance estimate using Newey-West estimator
    // we follow the default lag used in kpss.test in R's tseries package
    val lag = (3 * math.sqrt(n) / 13).toInt
    val longRunVariance = neweyWestVarianceEstimator(residuals, lag)
    val stat = (s2 / longRunVariance) / (n * n)
    (stat, criticalValues)
  }

  /**
   * Estimates long run variance using the Newey-West estimator, which consists on calculating
   * the variance between lagged values and applying a weight that decreases as the lag increases.
   * We translate the C implementation used by R's tseries:kpss.test, found at
   * https://github.com/cran/tseries/blob/master/src/ppsum.c
   * However, we add our own comments for clarity.
   * See pg 87 of [[http://faculty.washington.edu/ezivot/econ584/notes/timeSeriesConcepts.pdf]] for
   * more information
   */
  private def neweyWestVarianceEstimator(errors: Array[Double], lag: Int): Double = {
    val n = errors.length
    var sumOfTerms = 0.0
    var cellContrib = 0.0
    var i = 0
    var j = 0

    i = 1
    while (i <= lag) {
      j = i
      cellContrib = 0.0
      while (j < n) {
        // covariance between values at time t and time t-i
        cellContrib += errors(j) * errors(j - i)
        j += 1
      }
      // Newey-West weighing (decreasing weight for longer lags)
      sumOfTerms += cellContrib * (1 - (i.toDouble / (lag + 1)))
      i += 1
    }
    // we multiply by 2 as we calculated the sum of top row in matrix (lag 0, lag 1), (lag 0, lag 2)
    // etc but we need to also calculate first column (lag 1, lag 0), (lag 2, lag 0) etc
    // we divide by n and obtain a partial estimate of the variance, just missing no lag variance
    val partialEstVar = (sumOfTerms * 2) / n
    // add no-lag variance
    partialEstVar + (errors.map(math.pow(_, 2)).sum / n)
  }
}
