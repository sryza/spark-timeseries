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

import breeze.linalg._
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests._
import org.apache.commons.math3.stat.regression.{OLSMultipleLinearRegression, SimpleRegression}
import scala.language.postfixOps

/**
 * This model is basically a regression with ARIMA error structure
 * see
 * [[https://onlinecourses.science.psu.edu/stat510/node/53]]
 * [[https://www.otexts.org/fpp/9/1]]
 * [[http://robjhyndman.com/talks/RevolutionR/11-Dynamic-Regression.pdf]]
 * The basic idea is that for usual regression models
 * Y = B*X + e
 * e should be IID ~ N(0,sigma^2^), but in time series problems, e tends to have time series
 * characteristics.
 */
object RegressionARIMA {
  def fitModel(
      ts: Vector[Double],
      regressors: DenseMatrix[Double],
      method: String,
      optimizationArgs: Any*): RegressionARIMAModel = {
    method match {
      case "cochrane-orcutt" =>
        if (optimizationArgs.isEmpty) {
          fitCochraneOrcutt(ts, regressors)
        } else {
          if (!optimizationArgs(0).isInstanceOf[Int]){
            throw new IllegalArgumentException(
              "Maximum iteration parameter to Cochrane-Orcutt must be integer")
          }
          if (optimizationArgs.length > 1){
            throw new IllegalArgumentException("Number of Cochrane-Orcutt arguments can't exceed 3")
          }
          val maxIter = optimizationArgs(0).asInstanceOf[Int]
          fitCochraneOrcutt(ts, regressors, maxIter)
        }
      case _ =>
        throw new UnsupportedOperationException(
          "Regression ARIMA method \"" + method + "\" not defined.")
    }
  }

  /**
   * Fit linear regression model with AR(1) errors , for references on Cochrane Orcutt model:
   * See [[https://onlinecourses.science.psu.edu/stat501/node/357]]
   * See : Applied Linear Statistical Models - Fifth Edition - Michael H. Kutner , page 492
   * The method assumes the time series to have the following model
   *
   * Y_t = B.X_t + e_t
   * e_t = rho*e_t-1+w_t
   * e_t has autoregressive structure , where w_t is iid ~ N(0,&sigma 2)
   *
   * Outline of the method :
   * 1) OLS Regression for Y (timeseries) over regressors (X)
   * 2) Apply auto correlation test (Durbin-Watson test) over residuals , to test whether e_t still
   * have auto-regressive structure
   * 3) if test fails stop , else update update coefficients (B's) accordingly and go back to step 1)
   *
   * @param ts : Vector of size N for time series data to create the model for
   * @param regressors Matrix N X K for the timed values for K regressors over N time points
   * @param maxIter maximum number of iterations in iterative cochrane-orchutt estimation
   * @return instance of class [[RegressionARIMAModel]]
   */

  def fitCochraneOrcutt(ts: Vector[Double], regressors: DenseMatrix[Double], maxIter: Int = 10)
    : RegressionARIMAModel = {
    // Check parameters
    (0 until regressors.cols).find(c => regressors(::, c).length != ts.length).foreach { c =>
      throw new IllegalArgumentException(s"regressor at column index $c has length "
        + s"${regressors(::, c).length} which is not equal to time series length ${ts.length}")
    }
    val rhoDiffThr = 0.001
    val rhos = new Array[Double](maxIter)
    // Step 1) OLS Multiple Linear Regression
    val yxOlsMultReg = new OLSMultipleLinearRegression()
    yxOlsMultReg.setNoIntercept(false) // Regression of the form y = a+B.X + e
    val regressorArr = new Array[Array[Double]](regressors.rows)
    (0 until regressors.rows).foreach { row =>
      regressorArr(row) = regressors(row, ::).inner.toArray
    }
    yxOlsMultReg.newSampleData(ts.toArray, regressorArr)
    var beta = yxOlsMultReg.estimateRegressionParameters()

    // Step 2) auto correlation test
    var origRegResiduals = yxOlsMultReg.estimateResiduals()

    // Step 3) start checking / iteration phase
    var finished = !isAutoCorrelated(origRegResiduals)
    val resReg = new SimpleRegression(false)
    var iter = 0
    while (!finished) {
      // Create regression et = rho*et-1
      resReg.addObservations(origRegResiduals.slice(0, origRegResiduals.length - 1).map(Array(_)),
        origRegResiduals.slice(1, origRegResiduals.length))

      // Get rho from et = rho*et-1
      rhos(iter) = resReg.getSlope

      // Clear res regression model
      resReg.clear()

      // Calculate transformed regression
      // Y_dash_t = Y_t - rho *Y_t-1
      val Ydash = new DenseVector[Double](ts.toArray.drop(1)) -
        new DenseVector[Double](ts.toArray.dropRight(1)) * rhos(iter)

      // X_dash_t = X_t - rho *X_t-1
      // Note: use braces in breeze.lang operators to enforce precedence, as it seems not to follow
      // the defaults
      val Xdash: DenseMatrix[Double] = regressors((1 to regressors.rows - 1), ::) -
        (regressors((0 to regressors.rows - 2), ::) :* rhos(iter))
      val XdashArr = new Array[Array[Double]](Xdash.rows)
      (0 until Xdash.rows).foreach(r => XdashArr(r) = Xdash(r, ::).inner.toArray)

      // Transformed regression
      yxOlsMultReg.newSampleData(Ydash.toArray, XdashArr)

      // Get B.hat, transformed regression params
      beta = yxOlsMultReg.estimateRegressionParameters()
      // Update B0
      beta(0) = beta(0) / (1 - rhos(iter))
      // Update regression residuals
      val Yhat = (regressors * DenseVector(beta.drop(1)).t.inner).toDenseVector + beta(0)
      origRegResiduals = (DenseVector(ts.toArray) - Yhat).toArray
      // Iteration finished
      iter = iter + 1

      // Check for need of another iteration
      val transformedRegResiduals = yxOlsMultReg.estimateResiduals()
      // Residuals auto correlated ?
      val isResAR = isAutoCorrelated(transformedRegResiduals)
      // Exceeded maximum iterations
      val exceededMaxIter = iter >= maxIter
      // Values of rho converged
      val rhosConverged = iter >= 2 && Math.abs(rhos(iter - 1) - rhos(iter - 2)) <= rhoDiffThr

      finished = !isResAR || exceededMaxIter || rhosConverged
    }

    new RegressionARIMAModel(regressionCoeff = beta, arimaOrders = Array(1, 0, 0),
      arimaCoeff = Array(rhos(Math.max(0, iter - 1))))
  }

  /**
   * Test autocorrelation in data, current implementation uses Durbin Watson test, with the
   * following rules:
   * * If test statistic close to 0 => +ve autocorrelation
   * * close to 4 => -ve autocorrelation
   * * close to 2 => no autocorrelation
   * @param data data value to test auto correlations
   * @return boolean to indicate existence of auto correlation
   */
  private def isAutoCorrelated(data: Array[Double]): Boolean = {
    val dwMargin = 0.05 // margin for closeness for dw statistic values

    val dwTestStatistic = dwtest(new org.apache.spark.mllib.linalg.DenseVector(data))
    dwTestStatistic <= (2 - dwMargin) || dwTestStatistic >= (2 + dwMargin)
  }
}

/**
 * @param regressionCoeff coefficients for regression , including intercept , for example
 *                        if model has 3 regressors then length of [[regressionCoeff]] is 4
 * @param arimaOrders p,d,q for the arima error structure, length of [[arimaOrders]] must be 3
 * @param arimaCoeff AR , d , and MA terms , length of arimaCoeff = p+d+q
 */
class RegressionARIMAModel(
  val regressionCoeff: Array[Double],
  val arimaOrders: Array[Int],
  val arimaCoeff: Array[Double]) extends TimeSeriesModel {

  override def addTimeDependentEffects(
      ts: org.apache.spark.mllib.linalg.Vector,
      dest: org.apache.spark.mllib.linalg.Vector): org.apache.spark.mllib.linalg.Vector = {
    throw new UnsupportedOperationException()
  }

  override def removeTimeDependentEffects(
      ts: org.apache.spark.mllib.linalg.Vector,
      dest: org.apache.spark.mllib.linalg.Vector): org.apache.spark.mllib.linalg.Vector = {
    throw new UnsupportedOperationException()
  }
}
