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

import org.scalatest.FunSuite
import org.apache.spark.mllib.linalg._
import org.scalatest.Matchers._

class HoltWintersModelSuite extends FunSuite {

  /**
   * Ref : Examples in https://stat.ethz.ch/R-manual/R-devel/library/stats/html/HoltWinters.html
   * > m <- HoltWinters(AirPassengers)
   * > library(forecast)
   * > forecasts = forecast.HoltWinters(model,h=7)
   */
  val tsAirPassengers = new DenseVector(Array(
    112.0,118.0,132.0,129.0,121.0,135.0,148.0,148.0,136.0,119.0,104.0,118.0,115.0,126.0,
    141.0,135.0,125.0,149.0,170.0,170.0,158.0,133.0,114.0,140.0,145.0,150.0,178.0,163.0,
    172.0,178.0,199.0,199.0,184.0,162.0,146.0,166.0,171.0,180.0,193.0,181.0,183.0,218.0,
    230.0,242.0,209.0,191.0,172.0,194.0,196.0,196.0,236.0,235.0,229.0,243.0,264.0,272.0,
    237.0,211.0,180.0,201.0,204.0,188.0,235.0,227.0,234.0,264.0,302.0,293.0,259.0,229.0,
    203.0,229.0,242.0,233.0,267.0,269.0,270.0,315.0,364.0,347.0,312.0,274.0,237.0,278.0,
    284.0,277.0,317.0,313.0,318.0,374.0,413.0,405.0,355.0,306.0,271.0,306.0,315.0,301.0,
    356.0,348.0,355.0,422.0,465.0,467.0,404.0,347.0,305.0,336.0,340.0,318.0,362.0,348.0,
    363.0,435.0,491.0,505.0,404.0,359.0,310.0,337.0,360.0,342.0,406.0,396.0,420.0,472.0,
    548.0,559.0,463.0,407.0,362.0,405.0,417.0,391.0,419.0,461.0,472.0,535.0,622.0,606.0,
    508.0,461.0,390.0,432.0
  ))

  test("Optimal Paramaters alpha beta gamma - Additive Model") {

    val period = 12
    val model = HoltWinters.fitModel(tsAirPassengers, period, "additive", "BOBYQA")

    model.alpha should be (0.24796 +- 0.01)
    model.beta should be (0.03453 +- 0.01)
    model.gamma should be (1.0 +- 0.01)
  }

  test("Forecast - Additive Model") {
    val period = 12
    val model = HoltWinters.fitModel(tsAirPassengers, period, "additive", "BOBYQA")

    val forecasted = new DenseVector(new Array[Double](period))
    model.forecast(tsAirPassengers, forecasted)

    val actualForecasted = new Array[Double](period)
    actualForecasted(0) = 453.4977
    actualForecasted(1) = 429.3906
    actualForecasted(2) = 467.0361
    actualForecasted(3) = 503.2574
    actualForecasted(4) = 512.3395
    actualForecasted(5) = 571.8880
    actualForecasted(6) = 652.6095
    actualForecasted(7) = 637.4623
    actualForecasted(8) = 539.7548
    actualForecasted(9) = 490.7250
    actualForecasted(10) = 424.4593
    actualForecasted(11) = 469.5315

    for (i <- 0 until period) {
      forecasted(i) should be (actualForecasted(i) +- 10)
    }
  }

  /**
   * Ref : Examples in https://stat.ethz.ch/R-manual/R-devel/library/stats/html/HoltWinters.html
   * > m <- HoltWinters(co2, seasonal = "mult")
   * > library(forecast)
   * > forecasts = forecast.HoltWinters(model,h=7)
   */
  val tsCO2 = new DenseVector(Array(
    315.42,316.31,316.50,317.56,318.13,318.00,316.39,314.65,313.68,313.18,314.66,315.43,
    316.27,316.81,317.42,318.87,319.87,319.43,318.01,315.74,314.00,313.68,314.84,316.03,
    316.73,317.54,318.38,319.31,320.42,319.61,318.42,316.63,314.83,315.16,315.94,316.85,
    317.78,318.40,319.53,320.42,320.85,320.45,319.45,317.25,316.11,315.27,316.53,317.53,
    318.58,318.92,319.70,321.22,322.08,321.31,319.58,317.61,316.05,315.83,316.91,318.20,
    319.41,320.07,320.74,321.40,322.06,321.73,320.27,318.54,316.54,316.71,317.53,318.55,
    319.27,320.28,320.73,321.97,322.00,321.71,321.05,318.71,317.66,317.14,318.70,319.25,
    320.46,321.43,322.23,323.54,323.91,323.59,322.24,320.20,318.48,317.94,319.63,320.87,
    322.17,322.34,322.88,324.25,324.83,323.93,322.38,320.76,319.10,319.24,320.56,321.80,
    322.40,322.99,323.73,324.86,325.40,325.20,323.98,321.95,320.18,320.09,321.16,322.74,
    323.83,324.26,325.47,326.50,327.21,326.54,325.72,323.50,322.22,321.62,322.69,323.95,
    324.89,325.82,326.77,327.97,327.91,327.50,326.18,324.53,322.93,322.90,323.85,324.96,
    326.01,326.51,327.01,327.62,328.76,328.40,327.20,325.27,323.20,323.40,324.63,325.85,
    326.60,327.47,327.58,329.56,329.90,328.92,327.88,326.16,324.68,325.04,326.34,327.39,
    328.37,329.40,330.14,331.33,332.31,331.90,330.70,329.15,327.35,327.02,327.99,328.48,
    329.18,330.55,331.32,332.48,332.92,332.08,331.01,329.23,327.27,327.21,328.29,329.41,
    330.23,331.25,331.87,333.14,333.80,333.43,331.73,329.90,328.40,328.17,329.32,330.59,
    331.58,332.39,333.33,334.41,334.71,334.17,332.89,330.77,329.14,328.78,330.14,331.52,
    332.75,333.24,334.53,335.90,336.57,336.10,334.76,332.59,331.42,330.98,332.24,333.68,
    334.80,335.22,336.47,337.59,337.84,337.72,336.37,334.51,332.60,332.38,333.75,334.78,
    336.05,336.59,337.79,338.71,339.30,339.12,337.56,335.92,333.75,333.70,335.12,336.56,
    337.84,338.19,339.91,340.60,341.29,341.00,339.39,337.43,335.72,335.84,336.93,338.04,
    339.06,340.30,341.21,342.33,342.74,342.08,340.32,338.26,336.52,336.68,338.19,339.44,
    340.57,341.44,342.53,343.39,343.96,343.18,341.88,339.65,337.81,337.69,339.09,340.32,
    341.20,342.35,342.93,344.77,345.58,345.14,343.81,342.21,339.69,339.82,340.98,342.82,
    343.52,344.33,345.11,346.88,347.25,346.62,345.22,343.11,340.90,341.18,342.80,344.04,
    344.79,345.82,347.25,348.17,348.74,348.07,346.38,344.51,342.92,342.62,344.06,345.38,
    346.11,346.78,347.68,349.37,350.03,349.37,347.76,345.73,344.68,343.99,345.48,346.72,
    347.84,348.29,349.23,350.80,351.66,351.07,349.33,347.92,346.27,346.18,347.64,348.78,
    350.25,351.54,352.05,353.41,354.04,353.62,352.22,350.27,348.55,348.72,349.91,351.18,
    352.60,352.92,353.53,355.26,355.52,354.97,353.75,351.52,349.64,349.83,351.14,352.37,
    353.50,354.55,355.23,356.04,357.00,356.07,354.67,352.76,350.82,351.04,352.69,354.07,
    354.59,355.63,357.03,358.48,359.22,358.12,356.06,353.92,352.05,352.11,353.64,354.89,
    355.88,356.63,357.72,359.07,359.58,359.17,356.94,354.92,352.94,353.23,354.09,355.33,
    356.63,357.10,358.32,359.41,360.23,359.55,357.53,355.48,353.67,353.95,355.30,356.78,
    358.34,358.89,359.95,361.25,361.67,360.94,359.55,357.49,355.84,356.00,357.59,359.05,
    359.98,361.03,361.66,363.48,363.82,363.30,361.94,359.50,358.11,357.80,359.61,360.74,
    362.09,363.29,364.06,364.76,365.45,365.01,363.70,361.54,359.51,359.65,360.80,362.38,
    363.23,364.06,364.61,366.40,366.84,365.68,364.52,362.57,360.24,360.83,362.49,364.34
  ))

  test("Optimal Paramaters alpha beta gamma - Multiplicative Model") {
    val period = 12
    val model = HoltWinters.fitModel(tsCO2, period, "multiplicative", "BOBYQA")

    model.alpha should be(0.51265 +- 0.01)
    model.beta should be(0.00949 +- 0.01)
    model.gamma should be(0.47289 +- 0.1)
  }

  test("Forecast - Multiplicative Model") {
    val period = 12
    val model = HoltWinters.fitModel(tsCO2, period, "multiplicative", "BOBYQA")

    val forecasted = new DenseVector(new Array[Double](period))
    model.forecast(tsCO2, forecasted)

    val actualForecasted = new Array[Double](period)
    actualForecasted(0) = 365.1079
    actualForecasted(1) = 365.9664
    actualForecasted(2) = 366.7343
    actualForecasted(3) = 368.1364
    actualForecasted(4) = 368.6674
    actualForecasted(5) = 367.9508
    actualForecasted(6) = 366.5318
    actualForecasted(7) = 364.3799
    actualForecasted(8) = 362.4731
    actualForecasted(9) = 362.7520
    actualForecasted(10) = 364.2203
    actualForecasted(11) = 365.6741

    for (i <- 0 until period) {
      forecasted(i) should be (actualForecasted(i) +- 10)
    }
  }
}