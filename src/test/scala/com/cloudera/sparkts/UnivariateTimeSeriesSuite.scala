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

import com.cloudera.sparkts.models.ARModel

import scala.Double.NaN

import org.apache.spark.mllib.linalg._

import com.cloudera.sparkts.UnivariateTimeSeries._

import org.apache.commons.math3.random.MersenneTwister

import org.scalatest.{FunSuite, ShouldMatchers}

class UnivariateTimeSeriesSuite extends FunSuite with ShouldMatchers {
  test("lagIncludeOriginalsTrue") {
    val lagMatrix = UnivariateTimeSeries.lag(Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0), 2, true)
    lagMatrix should be (Matrices.dense(3, 3, Array(3.0, 4.0, 5.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0)))
  }

  test("lagIncludeOriginalsFalse") {
    val lagMatrix = UnivariateTimeSeries.lag(Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0), 2, false)
    lagMatrix should be (Matrices.dense(3, 2, Array(2.0, 3.0, 4.0, 1.0, 2.0, 3.0)))
  }

  test("lastNotNaN") {
    lastNotNaN(new DenseVector(Array(1.0, 2.0, 3.0, 4.0))) should be (3)
    lastNotNaN(new DenseVector(Array(1.0, 2.0, NaN, 4.0))) should be (3)
    lastNotNaN(new DenseVector(Array(1.0, 2.0, 3.0, NaN))) should be (2)
  }

  test("autocorr") {
    val rand = new MersenneTwister(5L)
    val iidAutocorr = autocorr(Array.fill(10000)(rand.nextDouble * 5.0), 3)
    iidAutocorr.foreach(math.abs(_) should be < .03)

    val arModel = new ARModel(1.5, Array(.2))
    val arSeries = arModel.sample(10000, rand)
    val arAutocorr = autocorr(arSeries, 3)
    math.abs(.2 - arAutocorr(0)) should be < 0.02
    arAutocorr(1) should be < 0.06
    arAutocorr(2) should be < 0.06
    arAutocorr(1) should be > 0.0
    arAutocorr(2) should be > 0.0
  }

  test("upsampling") {
    // replicating upsampling examples
    // from http://www.mathworks.com/help/signal/ref/upsample.html?searchHighlight=upsample
    val y = new DenseVector(Array(1.0, 2.0, 3.0, 4.0))
    val yUp1 = upsample(y, 3, useZero = true).toArray
    yUp1 should be (Array(1.0, 0.0, 0.0, 2.0, 0.0, 0.0, 3.0, 0.0, 0.0, 4.0, 0.0, 0.0))

    val yUp2 = upsample(y, 3, useZero = true, phase = 2).toArray
    yUp2 should be (Array(0.0, 0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 0.0, 3.0, 0.0, 0.0, 4.0))
  }

  test("downsampling") {
    // replicating downsampling examples
    // from http://www.mathworks.com/help/signal/ref/downsample.html?searchHighlight=downsample
    val y = new DenseVector((1 to 10).toArray.map(_.toDouble))
    val yDown1 = downsample(y, 3).toArray
    yDown1 should be (Array(1.0, 4.0, 7.0, 10.0))

    val yDown2 = downsample(y, 3, phase = 2).toArray
    yDown2 should be (Array(3.0, 6.0, 9.0))

  }

  test("signal reconstruction with spline") {
    // If we have a frequent signal, downsample it (at a rate that doesn't cause aliasing)
    // and we upsample, and apply a filter (interpolation), then the result should be fairly
    // close to the original signal. In our case, we drop NAs that are not filled by interpolation
    // (i.e no extrapolation)

    val y = (1 to 1000).toArray.map(_.toDouble / 100.0).map(Math.sin)
    val vy = new DenseVector(y)
    val lessFreq = downsample(vy, 100)
    val moreFreq = upsample(lessFreq, 100)

    // work on copies
    val splineY = fillSpline(new DenseVector(moreFreq.toArray)).toArray
    val lineY = fillLinear(new DenseVector(moreFreq.toArray)).toArray

    val MSE = (est: Array[Double], obs: Array[Double]) => {
      val errs = est.zip(obs).filter(!_._1.isNaN).map { case (yhat, yi) =>
        (yhat - yi) * (yhat - yi)
      }
      errs.sum / errs.length
    }

    val sE = MSE(splineY, y)
    val lE = MSE(lineY, y)

    // a cubic spline should be better than linear interpolation
    sE should be < lE
  }

  test("differencing at lag") {
    val rand = new MersenneTwister(10L)
    val n = 100
    val sampled = new DenseVector(Array.fill(n)(rand.nextGaussian))
    val lag = 5
    val diffed = differencesAtLag(sampled, lag)
    val invDiffed = inverseDifferencesAtLag(diffed, lag)

    for (i <- 0 until n) {
      sampled(i) should be (invDiffed(i) +- 1e-6)
    }

    diffed(10) should be (sampled(10) - sampled(5))
    diffed(99) should be (sampled(99) - sampled(94))
  }

  test("differencing of order d") {
    val rand = new MersenneTwister(10L)
    val n = 100
    val sampled = new DenseVector(Array.fill(n)(rand.nextGaussian))
    // differencing at order 1 and lag 1 should be the same
    val diffedOfOrder1 = differencesOfOrderD(sampled, 1)
    val diffedAtLag1 = differencesAtLag(sampled, 1)

    for (i <- 0 until n) {
      diffedAtLag1(i) should be (diffedOfOrder1(i) +- 1e-6)
    }

    // differencing at order and inversing should return the original series
    val diffedOfOrder5 = differencesOfOrderD(sampled, 5)
    val invDiffedOfOrder5 = inverseDifferencesOfOrderD(diffedOfOrder5, 5)

    for (i <- 0 until n) {
      invDiffedOfOrder5(i) should be (sampled(i) +- 1e-6)
    }

    // Differencing of order n + 1 should be the same as differencing one time a
    // vector that has already been differenced to order n
    val diffedOfOrder6 = differencesOfOrderD(sampled, 6)
    val diffedOneMore = differencesOfOrderD(diffedOfOrder5, 1)
    // compare start at index = 6
    for (i <- 6 until n) {
      diffedOfOrder6(i) should be (diffedOneMore(i) +- 1e-6)
    }
  }
}

