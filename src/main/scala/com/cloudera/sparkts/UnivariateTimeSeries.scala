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
import breeze.stats._

import org.apache.commons.math3.analysis.interpolation.SplineInterpolator

object UnivariateTimeSeries {
  def autocorr(ts: Array[Double], numLags: Int): Array[Double] = {
    autocorr(new DenseVector(ts), numLags).toDenseVector.data
  }

  def quotients(ts: Vector[Double], lag: Int): Vector[Double] = {
    val ret = new Array[Double](ts.length - lag)
    var i = 0
    while (i < ret.length) {
      ret(i) = ts(i + lag) / ts(i)
      i += 1
    }
    new DenseVector(ret)
  }

  def price2ret(ts: Vector[Double], lag: Int): Vector[Double] = {
    val ret = new Array[Double](ts.length - lag)
    var i = 0
    while (i < ret.length) {
      ret(i) = ts(i + lag) / ts(i) - 1.0
      i += 1
    }
    new DenseVector(ret)
  }

  /**
   * Computes the sample autocorrelation of the given series.
   */
  def autocorr(ts: Vector[Double], numLags: Int): Vector[Double] = {
    val corrs = new Array[Double](numLags)
    var i = 1
    while (i <= numLags) {
      val slice1 = ts(i until ts.length)
      val slice2 = ts(0 until ts.length - i)
      val mean1 = mean(slice1)
      val mean2 = mean(slice2)
      var variance1 = 0.0
      var variance2 = 0.0
      var covariance = 0.0
      var j = 0
      while (j < ts.length - i) {
        val diff1 = slice1(j) - mean1
        val diff2 = slice2(j) - mean2
        variance1 += diff1 * diff1
        variance2 += diff2 * diff2
        covariance += diff1 * diff2
        j += 1
      }

      corrs(i - 1) = covariance / (math.sqrt(variance1) * math.sqrt(variance2))
      i += 1
    }
    new DenseVector[Double](corrs)
  }

  /**
   * Trim leading NaNs from a series.
   */
  def trimLeading(ts: Vector[Double]): Vector[Double] = {
    val start = firstNotNaN(ts)
    if (start < ts.length) {
      ts(start until ts.length)
    } else {
      DenseVector.zeros[Double](0)
    }
  }

  /**
   * Trim trailing NaNs from a series.
   */
  def trimTrailing(ts: Vector[Double]): Vector[Double] = {
    val end = lastNotNaN(ts)
    if (end > 0) {
      ts(0 until end)
    } else {
      DenseVector.zeros[Double](0)
    }
  }

  def firstNotNaN(ts: Vector[Double]): Int = {
    var i = 0
    while (i < ts.length) {
      if (!java.lang.Double.isNaN(ts(i))) {
        return i
      }
      i += 1
    }
    i
  }

  def lastNotNaN(ts: Vector[Double]): Int = {
    var i = ts.length - 1
    while (i >= 0) {
      if (!java.lang.Double.isNaN(ts(i))) {
        return i
      }
      i -= 1
    }
    i
  }

  def fillts(ts: Vector[Double], fillMethod: String): Vector[Double] = {
    fillMethod match {
      case "linear" => fillLinear(ts)
      case "nearest" => fillNearest(ts)
      case "next" => fillNext(ts)
      case "previous" => fillPrevious(ts)
      case "spline" => fillSpline(ts)
      case _ => throw new UnsupportedOperationException()
    }
  }

  def fillNearest(values: Array[Double]): Array[Double] = {
    fillNearest(new DenseVector(values)).data
  }

  def fillNearest(values: Vector[Double]): DenseVector[Double] = {
    val result = new DenseVector(values.toArray)
    var lastExisting = -1
    var nextExisting = -1
    var i = 1
    while (i < result.length) {
      if (result(i).isNaN) {
        if (nextExisting < i) {
          nextExisting = i + 1
          while (nextExisting < result.length && result(nextExisting).isNaN) {
            nextExisting += 1
          }
        }

        if (lastExisting < 0 && nextExisting >= result.length) {
          throw new IllegalArgumentException("Input is all NaNs!")
        } else if (nextExisting >= result.length || // TODO: check this
          (lastExisting >= 0 && i - lastExisting < nextExisting - i)) {
          result(i) = result(lastExisting)
        } else {
          result(i) = result(nextExisting)
        }
      } else {
        lastExisting = i
      }
      i += 1
    }
    result
  }

  def fillPrevious(values: Array[Double]): Array[Double] = {
    fillPrevious(new DenseVector(values)).data
  }

  /**
   * fills in NaN with the previously available not NaN, scanning from left to right.
   * 1 NaN NaN 2 Nan -> 1 1 1 2 2
   */
  def fillPrevious(values: Vector[Double]): DenseVector[Double] = {
    val result = new DenseVector(values.toArray)
    var filler = Double.NaN // initial value, maintains invariant
    var i = 0
    while (i < result.length) {
      filler = if (result(i).isNaN) filler else result(i)
      result(i) = filler
      i += 1
    }
    result
  }

  def fillNext(values: Array[Double]): Array[Double] = {
    fillNext(new DenseVector(values)).data
  }

  /**
   * fills in NaN with the next available not NaN, scanning from right to left.
   * 1 NaN NaN 2 Nan -> 1 2 2 2 NaN
   */
  def fillNext(values: Vector[Double]): DenseVector[Double] = {
    val result = new DenseVector(values.toArray)
    var filler = Double.NaN // initial value, maintains invariant
    var i = result.length - 1
    while (i >= 0) {
      filler = if (result(i).isNaN) filler else result(i)
      result(i) = filler
      i -= 1
    }
    result
  }
  
  def fillWithDefault(values: Array[Double], filler: Double): Array[Double] = {
    fillWithDefault(new DenseVector(values), filler).data
  } 

  /**
   * fills in NaN with a default value
   */
  def fillWithDefault(values: Vector[Double], filler: Double): DenseVector[Double] = {
    val result = new DenseVector(values.toArray)
    var i = 0
    while (i < result.length) {
      result(i) = if (result(i).isNaN) filler else result(i)
      i += 1
    }
    result
  }

  def fillLinear(values: Array[Double]): Array[Double] = {
    fillLinear(new DenseVector(values)).data
  }

  def fillLinear(values: Vector[Double]): DenseVector[Double] = {
    val result = new DenseVector(values.toArray)
    var i = 1
    while (i < result.length - 1) {
      val rangeStart = i
      while (i < result.length - 1 && result(i).isNaN) {
        i += 1
      }
      val before = result(rangeStart - 1)
      val after = result(i)
      if (i != rangeStart && !before.isNaN && !after.isNaN) {
        val increment = (after - before) / (i - (rangeStart - 1))
        for (j <- rangeStart until i) {
          result(j) = result(j - 1) + increment
        }
      }
      i += 1
    }
    result
  }

  def fillSpline(values: Array[Double]): Array[Double] = {
    fillSpline(new DenseVector(values)).data
  }

  /**
   * Fill in NaN values using a natural cubic spline.
   * @param values Vector to interpolate
   * @return Interpolated vector
   */
  def fillSpline(values: Vector[Double]): DenseVector[Double]= {
    val result = new DenseVector(values.toArray)
    val interp = new SplineInterpolator()
    val knotsAndValues = values.toArray.zipWithIndex.filter(!_._1.isNaN)
    // Note that the type of unzip is missed up in scala 10.4 as per
    // https://issues.scala-lang.org/browse/SI-8081
    // given that this project is using scala 10.4, we cannot use unzip, so unpack manually
    val knotsX = knotsAndValues.map(_._2.toDouble)
    val knotsY = knotsAndValues.map(_._1)
    val filler = interp.interpolate(knotsX, knotsY)

    // values that we can interpolate between, others need to be filled w/ other function
    var i = knotsX(0).toInt
    val end = knotsX.last.toInt

    while (i < end) {
      result(i) = filler.value(i.toDouble)
      i += 1
    }
    result
  }

  def ar(values: Vector[Double], maxLag: Int): ARModel = Autoregression.fitModel(values, maxLag)

  /**
   * Down sample by taking every nth element starting from offset phase
   * @param values Vector to down sample
   * @param n take every nth element
   * @param phase offset from starting index
   * @return downsampled vector with appropriate length
   */
  def downsample(values: Vector[Double], n: Int, phase: Int = 0) = {
    val origLen = values.length
    val newLen = Math.ceil((values.length - phase) / n.toDouble).toInt
    val sampledValues = new DenseVector(Array.fill(newLen)(0.0))
    var i = phase
    var j = 0

    while (j < newLen) {
      sampledValues(j) = values(i)
      i += n
      j += 1
    }
    sampledValues
  }

  /**
   * Up sample by inserting n - 1 elements into the original values vector, starting at index phase
   * @param values the original data vector
   * @param n the number of insertions between elements
   * @param phase the offset to begin
   * @param useZero fill with zeros rather than NaN
   * @return upsampled vector filled with zeros or NaN, as specified by user
   */
  def upsample(values: Vector[Double], n: Int, phase: Int = 0, useZero: Boolean = false) = {
    val filler = if (useZero) 0 else Double.NaN
    val origLen = values.length
    val newLen = origLen * n
    val sampledValues = new DenseVector(Array.fill(newLen)(filler))
    var i = phase
    var j = 0

    while (j < origLen) {
      sampledValues(i) = values(j)
      i += n
      j += 1
    }
    sampledValues
  }

  /**
   * Difference a vector with respect to the m-th prior element. Size-preserving by leaving first
   * `m` elements intact. This is the inverse of the `inverseDifferences` function.
   * @param ts Series to difference
   * @param lag The difference lag (e.g. x means y(i) = ts(i) - ts(i - x), etc)
   * @return a new differenced vector
   */
  def differencesAtLag(ts: Vector[Double], lag: Int): Vector[Double] = {
    if (lag == 0) {
      // for consistency, since we create a new vector in else-branch
      ts.copy
    } else {
      val n = ts.length
      val diffedTs = new DenseVector(Array.fill(n)(0.0))
      var i = 0

      while (i < n) {
        // elements prior to `lag` are copied over without modification
        diffedTs(i) = if (i < lag) ts(i) else ts(i) - ts(i - lag)
        i += 1
      }
      diffedTs
    }
  }

  /**
   * Calculate an "inverse-differenced" vector of a given lag. Size-preserving by leaving first
   * `lag` elements intact. This is the inverse of the `differences` function.
   * @param ts Series to add up
   * @param lag The difference lag to add (e.g. x means y(i) = ts(i) + y(i -
   *              x), etc)
   * @return a new vector where the difference operation as been inverted
   */
  def inverseDifferencesAtLag(ts: Vector[Double], lag: Int): Vector[Double] = {
    if (lag == 0) {
      // for consistency, since we create a new vector in else-branch
      ts.copy
    } else {
      val n = ts.length
      val addedTs = new DenseVector(Array.fill(n)(0.0))
      var i = 0

      while (i < n) {
        // elements prior to `order` are copied over without modification
        addedTs(i) = if (i < lag) ts(i) else ts(i) + addedTs(i - lag)
        i += 1
      }
      addedTs
    }
  }

  /**
   * Performs differencing of order `d`. This means we recursively difference a vector a total of
   * d-times. So that d = 2 is a vector of the differences of differences. Note that for each
   * difference level, d_i, the element at ts(d_i - 1) corresponds to the value in the prior
   * iteration.
   * @param ts time series to difference
   * @param d order of differencing
   * @return a vector of the same length differenced to order d
   */
  def differencesOfOrderD(ts: Vector[Double], d: Int): Vector[Double] = {
    def differencesOfOrderD0(ts: Vector[Double], d: Int, ix: Int): Vector[Double] = {
      val diffedTs = ts.copy
      if (d == 0) {
        diffedTs
      } else {
        val n = ts.length
        var i = ix
        while (i < n) {
          diffedTs(i) = ts(i) - ts(i - 1)
          i += 1
        }
        differencesOfOrderD0(diffedTs, d - 1, ix + 1)
      }
    }
    differencesOfOrderD0(ts, d, 1)
  }

  /**
   * Inverses differencing of order `d`.
   * @param diffedTs time series to reverse differencing process
   * @param d order of differencing
   * @return a vector of the same length, whcih when differenced to order ts, yields the original
   *         vector provided
   */
  def inverseDifferencesOfOrderD(diffedTs: Vector[Double], d: Int): Vector[Double] = {
    def inverseDifferencesOfOrderD0(diffedTs: Vector[Double], d: Int, ix: Int): Vector[Double] = {
      val ts = diffedTs.copy
      if (d == 0) {
        diffedTs
      } else {
        val n = diffedTs.length
        var i = ix
        while (i < n) {
          ts(i) = diffedTs(i) + ts(i - 1)
          i += 1
        }
        inverseDifferencesOfOrderD0(ts, d - 1, ix - 1)
      }
    }
    inverseDifferencesOfOrderD0(diffedTs, d, d)
  }
}

