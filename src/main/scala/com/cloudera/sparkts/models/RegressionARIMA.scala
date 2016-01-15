package com.cloudera.sparkts.models

import breeze.linalg
import breeze.linalg.{DenseVector, Transpose, DenseMatrix}
import org.apache.commons.math3.stat.regression
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

import scala.language.postfixOps

/**
 * Created by baddar on 1/14/16.
 * This object contains different methods to
 */

object RegressionARIMA {

  val COCHRANE_ORCHUTT = 1;
  val REG_ARMA = 2;

  /**
   * Fit linear regression model with AR(1) errors , for references on cochrane orchutt model:
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
   * 2)Apply Durbin-Watson test over residuals , to test whether e_t still have auto-regressive structure
   * 3)if test fails stop , else update update coefficients (B's) accordingly and go back to step 1)
   *
   * @param ts : Vector of size N for time series data to create the model for
   * @param regressors Matrix N X K for the timed values for K regressors over N time points
   * @param maxIter maximum number of iterations in iterative cochrane-orchutt estimation
   * @return instance of class [[RegressionARIMAModel]]
   */
  def fitCochraneOrchutt(ts:linalg.Vector[Double],regressors: DenseMatrix[Double],maxIter: Int =10) : RegressionARIMAModel =  {

    //parameters check
    (0 until regressors.cols).map(i=> {if(regressors(::,i).length != ts.length) {
      throw new IllegalArgumentException("regressor at column index = "+i+" has length = "+regressors(::,i).length+
        "which is not equal to time series length ("+ts.length+")")

    }})

    //Step 1) OLS Multiple Linear Regression
    val olsMultReg: OLSMultipleLinearRegression = new OLSMultipleLinearRegression()
    olsMultReg.setNoIntercept(false)

    return null;
  }


}

class RegressionARIMAModel(intercept : Double,regressionCoeff: Array[Double],arimaCoeff:Array[Double]) extends TimeSeriesModel{
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