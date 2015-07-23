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

import java.util.Random

import breeze.linalg._

import org.apache.commons.math3.random.MersenneTwister

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class ARIMASuite extends FunSuite {
  test("compare with R") {
    // > R.Version()$version.string
    // [1] "R version 3.2.0 (2015-04-16)"
    // > set.seed(456)
    // y <- arima.sim(n=250,list(ar=0.3,ma=0.7),mean = 5)
    // write.table(y, file = "resources/R_ARIMA_Data.csv", row.names = FALSE, col.names = FALSE)
    val dataFile = getClass.getClassLoader.getResourceAsStream("R_ARIMA_Data.csv")
    val rawData = scala.io.Source.fromInputStream(dataFile).getLines().toArray.map(_.toDouble)
    val data = new DenseVector(rawData)

    val model = ARIMA.fitModel((1, 0, 1), data)
    val Array(c, ar, ma) = model.coefficients
    ar should be (0.3 +- 0.05)
    ma should be (0.7 +- 0.05)
  }

  test("Data sampled from a given model should result in similar model if fit") {
    val model = new ARIMAModel((2, 1, 2), Array(8.2, 0.2, 0.5, 0.3, 0.1))
    val sampled = model.sample(1000)
    val newModel = ARIMA.fitModel((2, 1, 2), sampled)
    val Array(c, ar1, ar2, ma1, ma2) = model.coefficients
    val Array(cTest, ar1Test, ar2Test, ma1Test, ma2Test) = newModel.coefficients
    c should be (cTest +- 0.1)
    ar1Test should be (ar1 +- 0.1)
    ma1Test should be (ma1 +- 0.1)
    ar2Test should be (ar2 +- 0.1)
    ma2Test should be (ma2 +- 0.1)
  }

  test("Fitting ARIMA(p, d, q) should be the same as fitting a d-order differenced ARMA(p, q)") {
    val model = new ARIMAModel((1, 1, 2), Array(0.3, 0.7, 0.1), hasIntercept = false)
    val sampled = model.sample(1000)
    val arimaModel = ARIMA.fitModel((1, 1, 2), sampled, includeIntercept = false)
    val differencedSample = new DenseVector(ARIMA.differences(sampled, 1).toArray.drop(1))
    val armaModel = ARIMA.fitModel((1, 0, 2), differencedSample, includeIntercept = false)

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

  ignore("Adding ARIMA effects to whitenoise, and removing should make series close to white noise") {
    val rand = new MersenneTwister(20L)
    val model = new ARIMAModel((1, 1, 2), Array(8.3, 0.1, 0.2, 0.3), hasIntercept = true)
    val whiteNoise = new DenseVector(Array.fill(100)(rand.nextGaussian))
    val arimaProcess = new DenseVector(Array.fill(100)(0.0))
    model.addTimeDependentEffects(whiteNoise, arimaProcess)
    val closeToWhiteNoise = new DenseVector(Array.fill(100)(0.0))
    model.removeTimeDependentEffects(arimaProcess, closeToWhiteNoise)
    closeToWhiteNoise.toArray.sum / closeToWhiteNoise.length should be < 1.0
  }
}
