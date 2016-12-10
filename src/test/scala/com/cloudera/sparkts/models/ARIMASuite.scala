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

import com.cloudera.sparkts.UnivariateTimeSeries.{differencesOfOrderD, inverseDifferencesOfOrderD}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.mllib.linalg.DenseVector
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.{Success, Try}

class ARIMASuite extends FunSuite {
  test("compare with R") {
    // > R.Version()$version.string
    // [1] "R version 3.2.0 (2015-04-16)"
    // > set.seed(456)
    // y <- arima.sim(n=250,list(ar=0.3,ma=0.7),mean = 5)
    // write.table(y, file = "resources/R_ARIMA_DataSet1.csv", row.names = FALSE, col.names = FALSE)
    val dataFile = getClass.getClassLoader.getResourceAsStream("R_ARIMA_DataSet1.csv")
    val rawData = scala.io.Source.fromInputStream(dataFile).getLines().toArray.map(_.toDouble)
    val data = new DenseVector(rawData)

    val model = ARIMA.fitModel(1, 0, 1, data)
    val Array(c, ar, ma) = model.coefficients
    ar should be (0.3 +- 0.05)
    ma should be (0.7 +- 0.05)
  }

  test("Data sampled from a given model should result in similar model if fit") {
    val rand = new MersenneTwister(10L)
    val model = new ARIMAModel(2, 1, 2, Array(8.2, 0.2, 0.5, 0.3, 0.1))
    val sampled = model.sample(1000, rand)
    val newModel = ARIMA.fitModel(2, 1, 2, sampled)
    val Array(c, ar1, ar2, ma1, ma2) = model.coefficients
    val Array(cTest, ar1Test, ar2Test, ma1Test, ma2Test) = newModel.coefficients
    // intercept is given more leeway
    c should be (cTest +- 1)
    ar1Test should be (ar1 +- 0.1)
    ma1Test should be (ma1 +- 0.1)
    ar2Test should be (ar2 +- 0.1)
    ma2Test should be (ma2 +- 0.1)
  }

  test("Fitting CSS with BOBYQA and conjugate gradient descent should be fairly similar") {
    val rand = new MersenneTwister(10L)
    val model = new ARIMAModel(2, 1, 2, Array(8.2, 0.2, 0.5, 0.3, 0.1))
    val sampled = model.sample(1000, rand)
    val fitWithBOBYQA = ARIMA.fitModel(2, 1, 2, sampled, method = "css-bobyqa")
    val fitWithCGD = ARIMA.fitModel(2, 1, 2, sampled, method = "css-cgd")

    val Array(c, ar1, ar2, ma1, ma2) = fitWithBOBYQA.coefficients
    val Array(cCGD, ar1CGD, ar2CGD, ma1CGD, ma2CGD) = fitWithCGD.coefficients

    // give more leeway for intercept
    cCGD should be (c +- 1)
    ar1CGD should be (ar1 +- 0.1)
    ar2CGD should be (ar2 +- 0.1)
    ma1CGD should be (ma1 +- 0.1)
    ma2CGD should be (ma2 +- 0.1)
  }

  test("Fitting ARIMA(p, d, q) should be the same as fitting a d-order differenced ARMA(p, q)") {
    val rand = new MersenneTwister(10L)
    val model = new ARIMAModel(1, 1, 2, Array(0.3, 0.7, 0.1), hasIntercept = false)
    val sampled = model.sample(1000, rand)
    val arimaModel = ARIMA.fitModel(1, 1, 2, sampled, includeIntercept = false)
    val differencedSample = new DenseVector(differencesOfOrderD(sampled, 1).toArray.drop(1))
    val armaModel = ARIMA.fitModel(1, 0, 2, differencedSample, includeIntercept = false)

    val Array(refAR, refMA1, refMA2) = model.coefficients
    val Array(iAR, iMA1, iMA2) = arimaModel.coefficients
    val Array(ar, ma1, ma2) = armaModel.coefficients

    // ARIMA model should match parameters used to sample, to some extent
    iAR should be (refAR +- 0.05)
    iMA1 should be (refMA1 +- 0.05)
    iMA2 should be (refMA2 +- 0.05)

    // ARMA model parameters of differenced sample should be equal to ARIMA model parameters
    ar should be (iAR)
    ma1 should be (iMA1)
    ma2 should be (iMA2)
  }

  test("Adding ARIMA effects to series, and removing should return the same series") {
    val rand = new MersenneTwister(20L)
    val model = new ARIMAModel(1, 1, 2, Array(8.3, 0.1, 0.2, 0.3), hasIntercept = true)
    val whiteNoise = new DenseVector(Array.fill(100)(rand.nextGaussian))
    val arimaProcess = new DenseVector(Array.fill(100)(0.0))
    model.addTimeDependentEffects(whiteNoise, arimaProcess)
    val closeToWhiteNoise = new DenseVector(Array.fill(100)(0.0))
    model.removeTimeDependentEffects(arimaProcess, closeToWhiteNoise)

    for (i <- 0 until whiteNoise.size) {
      val diff = whiteNoise(i) - closeToWhiteNoise(i)
      math.abs(diff) should be < 1e-4
    }
  }

  test("Fitting ARIMA(0, 0, 0) with intercept term results in model with average as parameter") {
    val rand = new MersenneTwister(10L)
    val sampled = new DenseVector(Array.fill(100)(rand.nextGaussian))
    val model = ARIMA.fitModel(0, 0, 0, sampled)
    val mean = sampled.toArray.sum / sampled.size
    model.coefficients(0) should be (mean +- 1e-4)
  }

  test("Fitting ARIMA(0, 0, 0) with intercept term results in model with average as the forecast") {
    val rand = new MersenneTwister(10L)
    val sampled = new DenseVector(Array.fill(100)(rand.nextGaussian))
    val model = ARIMA.fitModel(0, 0, 0, sampled)
    val mean = sampled.toArray.sum / sampled.size
    model.coefficients(0) should be (mean +- 1e-4)
    val forecast = model.forecast(sampled, 10)
    for(i <- 100 until 110) {
      forecast(i) should be (mean +- 1e-4)
    }
  }

  test("Fitting an integrated time series of order 3") {
    // > set.seed(10)
    // > vals <- arima.sim(list(ma = c(0.2), order = c(0, 3, 1)), 200)
    // > arima(order = c(0, 3, 1), vals, method = "CSS")
    //
    // Call:
    //  arima(x = vals, order = c(0, 3, 1), method = "CSS")
    //
    //  Coefficients:
    //   ma1
    //  0.2523
    //  s.e.  0.0623
    //
    //  sigma^2 estimated as 0.9218:  part log likelihood = -275.65
    // > write.table(y, file = "resources/R_ARIMA_DataSet2.csv", row.names = FALSE, col.names =
    // FALSE)
    val dataFile = getClass.getClassLoader.getResourceAsStream("R_ARIMA_DataSet2.csv")
    val rawData = scala.io.Source.fromInputStream(dataFile).getLines().toArray.map(_.toDouble)
    val data = new DenseVector(rawData)
    val model = ARIMA.fitModel(0, 3, 1, data)
    val Array(c, ma) = model.coefficients
    ma should be (0.2 +- 0.05)
  }

  test("Stationarity and Invertibility checks") {
    // Testing violations of stationarity and invertibility
    val model1 = new ARIMAModel(1, 0, 0, Array(0.2, 1.5), hasIntercept = true)
    model1.isStationary() should be (false)
    model1.isInvertible() should be (true)

    val model2 =  new ARIMAModel(0, 0, 1, Array(0.13, 1.8), hasIntercept = true)
    model2.isStationary() should be (true)
    model2.isInvertible() should be (false)

    //  http://www.econ.ku.dk/metrics/Econometrics2_05_II/Slides/07_univariatetimeseries_2pp.pdf
    // AR(2) model on slide 31 should be stationary
    val model3 = new ARIMAModel(2, 0, 0, Array(0.003359, 1.545, -0.5646), hasIntercept = true)
    model3.isStationary() should be (true)
    model3.isInvertible() should be (true)

    // http://www.econ.ku.dk/metrics/Econometrics2_05_II/Slides/07_univariatetimeseries_2pp.pdf
    // ARIMA(1, 0, 1) model from slide 36 should be stationary and invertible
    val model4 = new ARIMAModel(1, 0, 1, Array(-0.09341,  0.857361, -0.300821), hasIntercept = true)
    model4.isStationary() should be (true)
    model4.isInvertible() should be (true)
  }

  test("Auto fitting ARIMA models") {
    val model1 = new ARIMAModel(2, 0, 0, Array(2.5, 0.4, 0.3), hasIntercept = true)
    val rand = new MersenneTwister(10L)
    val sampled = model1.sample(250, rand)

    // a series with a high integration order
    val highI = inverseDifferencesOfOrderD(sampled, 5)

    // auto fitting without increasing the maxD parameter should result in a failure
    val highIntegrationFailure = Try(ARIMA.autoFit(highI)).isFailure
    highIntegrationFailure should be (true)

    // but should work if we increase the differencing order limit
    val highIntegrationWorks = Try(ARIMA.autoFit(highI, maxD = 10)).isSuccess
    highIntegrationWorks should be (true)

    // in this test, we'll throw an exception and not go on if the auto fit function fails
    // the sample we're trying to model is I(0), and we can always fit a model with just the
    // intercept, so a failure here would be indicative of other issues
    val (maxP, maxQ) = (5, 5)
    val fitted = Try(ARIMA.autoFit(sampled, maxP = maxP, maxQ = maxQ)) match {
      case Success(model) => model
      case _ => throw new Exception("Unable to fit model in test suite")
    }
    val fittedApproxAIC = fitted.approxAIC(sampled)
    // The model should have a lower AIC than the dummy model (just intercept)
    // testing other models effectively boils down to the function implementation
    // so we don't do that here
    val justIntercept = ARIMA.fitModel(0, fitted.d, 0, sampled, includeIntercept = true)
    justIntercept.approxAIC(sampled) should be > fittedApproxAIC
  }

  test("Polynomial eigensolver should find easy root") {
    ARIMA.findRoots(Array(1, -0.4))(0).abs() should be (2.5)
  }

  // To compare with R:
  // roots <- abs(polyroot(c(1, 0.5, -0.3, 1.9, -3.0, 0.5)))
  test("Polynomial eigensolver should find harder roots") {
    val roots = ARIMA.findRoots(Array(1, 0.5, -0.3, 1.9, -3.0, 0.5))
    roots.map(x => (x.abs() * 1E5).round / 1E5) should contain theSameElementsAs
            Array(0.77959, 0.55383, 0.77959, 1.12229, 5.29438)
  }
}
