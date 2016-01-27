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
import breeze.linalg.{DenseMatrix, DenseVector}
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests._
import org.apache.commons.math3.stat.regression.{OLSMultipleLinearRegression, SimpleRegression}
import scala.language.postfixOps

/**
  * This model is basically a regression with ARIMA error structure
 * see
 * [[https://onlinecourses.science.psu.edu/stat510/node/53]]
 * [[https://www.otexts.org/fpp/9/1]]
 * [[http://robjhyndman.com/talks/RevolutionR/11-Dynamic-Regression.pdf]]
 * the basic idea is that for usual regression models
 * Y = B*X + e
 * e should be IID ~ N(0,sigma^2^) , but in time series problems , e tend to have time series characteristics
  */
object RegressionARIMA {

  

  def fitModel(ts: linalg.Vector[Double], regressors: DenseMatrix[Double], method: String, optimizationArgs: Any*): RegressionARIMAModel = {
    val model: RegressionARIMAModel = method match {
      case "cochrane-orcutt" => {
        if (optimizationArgs.length == 0) {
          fitCochraneOrcutt(ts, regressors)
        }
        else {
          if (!optimizationArgs(0).isInstanceOf[Int]){
            throw new IllegalArgumentException("Maximum iteration parameter to Cochrane orcutt must be integer")
          }
          if (optimizationArgs.length > 1){
            throw new IllegalArgumentException("Number of cochrane orcutt arguments can't exceed 3")
          }
          val maxIter = optimizationArgs(0).asInstanceOf[Int]
          fitCochraneOrcutt(ts, regressors, maxIter)
        }
      }
      case _ => {
        throw new UnsupportedOperationException("Regression ARIMA method : " + method + " not defined.")
      }
    }
    return model;
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
   * 2)Apply auto correlation test (Durbin-Watson test) over residuals , to test whether e_t still have auto-regressive structure
   * 3)if test fails stop , else update update coefficients (B's) accordingly and go back to step 1)
   *
   * @param ts : Vector of size N for time series data to create the model for
   * @param regressors Matrix N X K for the timed values for K regressors over N time points
   * @param maxIter maximum number of iterations in iterative cochrane-orchutt estimation
   * @return instance of class [[RegressionARIMAModel]]
   */


  def fitCochraneOrcutt(ts: linalg.Vector[Double], regressors: DenseMatrix[Double], maxIter: Int = 10): RegressionARIMAModel = {

    //parameters check
    (0 until regressors.cols).map(i => {
      if (regressors(::, i).length != ts.length) {
        throw new IllegalArgumentException("regressor at column index = " + i + " has length = " + regressors(::, i).length +
          "which is not equal to time series length (" + ts.length + ")")

      }
    })
    val rhoDiffThr = 0.001
    val rhos = new Array[Double](maxIter)
    //Step 1) OLS Multiple Linear Regression
    val yxOlsMultReg: OLSMultipleLinearRegression = new OLSMultipleLinearRegression()
    yxOlsMultReg.setNoIntercept(false) // Regression of the form y = a+B.X + e
    val regressorArr: Array[Array[Double]] = new Array[Array[Double]](regressors.rows)
    (0 until regressors.rows).map(row => {
      regressorArr(row) = regressors(row, ::).inner.toArray
    })
    yxOlsMultReg.newSampleData(ts.toArray, regressorArr)
    var beta: Array[Double] = yxOlsMultReg.estimateRegressionParameters()

    //Step 2) auto correlation test
    var origRegResiduals: Array[Double] = yxOlsMultReg.estimateResiduals()

    //step 3) start checking / iteration phase

    var finished = !isAutoCorrelated(origRegResiduals)
    val resReg = new SimpleRegression(false)
    var iter = 0
    while (!finished) {
      //create regression et= rho*et-1

      resReg.addObservations(origRegResiduals.slice(0, origRegResiduals.length - 1).map(Array(_)), origRegResiduals.slice(1, origRegResiduals.length))
      //get rho from et = rho*et-1
      rhos(iter) = resReg.getSlope

      //clear res regression model
      resReg.clear()
      //calculate transformed regression
      // Y_dash_t = Y_t - rho *Y_t-1

      val Ydash: DenseVector[Double] = new DenseVector[Double](ts.toArray.drop(1)) -
        new DenseVector[Double](ts.toArray.dropRight(1)) * rhos(iter)
      //X_dash_t = X_t - rho *X_t-1
      //Note : use braces in breeze.lang operators to enforce presedence  , as it seems not to follow the defaults
      val Xdash: DenseMatrix[Double] = regressors((1 to regressors.rows - 1), ::) - (regressors((0 to regressors.rows - 2), ::) :* rhos(iter))
      val XdashArr = new Array[Array[Double]](Xdash.rows)
      (0 until Xdash.rows).map(r => XdashArr(r) = Xdash(r, ::).inner.toArray)
      //transformed regression

      yxOlsMultReg.newSampleData(Ydash.toArray, XdashArr)
      //get B.hat , transformed regression params
      beta = yxOlsMultReg.estimateRegressionParameters()
      //update B0
      beta(0) = beta(0) / (1 - rhos(iter))
      //update regression residuals
      val Yhat: DenseVector[Double] = (regressors * DenseVector(beta.drop(1)).t.inner).toDenseVector + beta(0)
      origRegResiduals = (DenseVector(ts.toArray) - Yhat).toArray
      //iteration finished
      iter = iter + 1
      //check for need of other iteration
      val transformedRegResiduals = yxOlsMultReg.estimateResiduals()
      //residuals auto correlated ?
      val isResAR = isAutoCorrelated(transformedRegResiduals)
      //exceeded maximum iterations
      val exceededMaxIter = iter >= maxIter
      //values of rho converged
      val rhosConverged = if (iter >= 2) {
        Math.abs(rhos(iter - 1) - rhos(iter - 2)) <= rhoDiffThr
      } else false

      finished = !isResAR | exceededMaxIter | rhosConverged
    }

    return new RegressionARIMAModel(regressionCoeff = beta, arimaOrders = Array(1, 0, 0), arimaCoeff = Array(rhos(Math.max(0, iter - 1))))
  }

  /**
   * Test autocorrelation in data , current implemtnation use durbin watson test , with the following rules
   * if test statistic close to 0 => +ve autocorrelation
   * close to 4 => -ve autocorrelation
   * close to 2 => no autocorrelation
   * @param data data value to test auto correlations
   * @return boolen to indicate existence of auto correlation
   */
  def isAutoCorrelated(data: Array[Double]): Boolean = {
    //define constants
    val dwMargin = 0.05 //margin for closeness for dw statistic values

    val dwTestStatistic = dwtest(new DenseVector[Double](data))
    if (dwTestStatistic <= (2 - dwMargin) || dwTestStatistic >= (2 + dwMargin))
      return true
    return false
  }

}

/**
 * @param regressionCoeff coefficients for regression , including intercept , for example
 *                        if model has 3 regressors then length of [[regressionCoeff]] is 4
 * @param arimaOrders p,d,q for the arima error structure, length of [[arimaOrders]] must be 3
 * @param arimaCoeff AR , d , and MA terms , length of arimaCoeff = p+d+q
 */
class RegressionARIMAModel(regressionCoeff: Array[Double], arimaOrders: Array[Int]
                           , arimaCoeff: Array[Double]) extends TimeSeriesModel {

  def getRegressionCoeff = this.regressionCoeff

  def getArimaOrders = this.arimaOrders

  def getArimaCoeff = this.arimaCoeff


  /**
   * Takes a time series that is assumed to have this model's characteristics and returns a time
   * series with time-dependent effects of this model removed.
   *
   * This is the inverse of [[TimeSeriesModel#addTimeDependentEffects]].
   *
   * @param ts Time series of observations with this model's characteristics.
   * @param dest Array to put the filtered series, can be the same as ts.
   * @return The dest series, for convenience.
   */
  override def removeTimeDependentEffects(ts: _root_.breeze.linalg.Vector[Double], dest: _root_.breeze.linalg.Vector[Double]): _root_.breeze.linalg.Vector[Double] = ???

  /**
   * Takes a series of i.i.d. observations and returns a time series based on it with the
   * time-dependent effects of this model added.
   *
   * @param ts Time series of i.i.d. observations.
   * @param dest Array to put the filtered series, can be the same as ts.
   * @return The dest series, for convenience.
   */
  override def addTimeDependentEffects(ts: linalg.Vector[Double], dest: linalg.Vector[Double]): linalg.Vector[Double] = ???
}