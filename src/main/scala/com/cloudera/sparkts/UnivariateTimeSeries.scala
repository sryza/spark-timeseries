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

import java.util.Arrays

import breeze.stats._
import com.cloudera.sparkts.models.{ARModel, Autoregression}

import org.apache.commons.math3.analysis.interpolation.SplineInterpolator
import org.apache.spark.mllib.linalg._

object UnivariateTimeSeries {

  /**
   * Lags the univariate time series
   *
   * Example input vector: (1.0, 2.0, 3.0, 4.0, 5.0)
   *
   * With lag 2 and includeOriginal = true should give output matrix:
   *
   * 3.0   2.0   1.0
   * 4.0   3.0   2.0
   * 5.0   4.0   3.0
   */
  def lag(ts: Vector, maxLag: Int, includeOriginal: Boolean): Matrix = {
    Lag.lagMatTrimBoth(ts, maxLag, includeOriginal)
  }

  def autocorr(ts: Array[Double], numLags: Int): Array[Double] = {
    autocorr(new DenseVector(ts), numLags).toArray
  }

  def quotients(ts: Vector, lag: Int): Vector = {
    val ret = new Array[Double](ts.size - lag)
    var i = 0
    while (i < ret.length) {
      ret(i) = ts(i + lag) / ts(i)
      i += 1
    }
    new DenseVector(ret)
  }

  def price2ret(ts: Vector, lag: Int): Vector = {
    val ret = new Array[Double](ts.size - lag)
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
  def autocorr(ts: Vector, numLags: Int): Vector = {
    val corrs = new Array[Double](numLags)
    var i = 1
    val breezeTs = MatrixUtil.toBreeze(ts)
    while (i <= numLags) {
      val slice1 = breezeTs(i until ts.size)
      val slice2 = breezeTs(0 until ts.size - i)
      val mean1 = mean(slice1)
      val mean2 = mean(slice2)
      var variance1 = 0.0
      var variance2 = 0.0
      var covariance = 0.0
      var j = 0
      while (j < ts.size - i) {
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
    new DenseVector(corrs)
  }

  /**
   * Trim leading NaNs from a series.
   */
  def trimLeading(ts: Vector): Vector = {
    val start = firstNotNaN(ts)
    if (start < ts.size) {
      Vectors.dense(Arrays.copyOfRange(ts.toArray, start, ts.size))
    } else {
      Vectors.zeros(0)
    }
  }

  /**
   * Trim trailing NaNs from a series.
   */
  def trimTrailing(ts: Vector): Vector = {
    val end = lastNotNaN(ts)
    if (end > 0) {
      Vectors.dense(Arrays.copyOfRange(ts.toArray, 0, end))
    } else {
      Vectors.zeros(0)
    }
  }

  def firstNotNaN(ts: Vector): Int = {
    var i = 0
    while (i < ts.size) {
      if (!java.lang.Double.isNaN(ts(i))) {
        return i
      }
      i += 1
    }
    i
  }

  def lastNotNaN(ts: Vector): Int = {
    var i = ts.size - 1
    while (i >= 0) {
      if (!java.lang.Double.isNaN(ts(i))) {
        return i
      }
      i -= 1
    }
    i
  }

  def fillts(ts: Vector, fillMethod: String): Vector = {
    fillMethod match {
      case "linear" => fillLinear(ts)
      case "nearest" => fillNearest(ts)
      case "next" => fillNext(ts)
      case "previous" => fillPrevious(ts)
      case "spline" => fillSpline(ts)
      case "zero" => fillValue(ts, 0)
      case _ => throw new UnsupportedOperationException()
    }
  }

  /**
   * Replace all NaNs with a specific value
   */
  def fillValue(values: Array[Double], filler: Double): Array[Double] = {
    fillValue(new DenseVector(values), filler).toArray
  }

  /**
   * Replace all NaNs with a specific value
   */
  def fillValue(values: Vector, filler: Double): DenseVector = {
    val result = values.copy.toArray
    var i = 0
    while (i < result.size) {
      if (result(i).isNaN) result(i) = filler
      i += 1
    }
    new DenseVector(result)
  }

  def fillNearest(values: Array[Double]): Array[Double] = {
    fillNearest(new DenseVector(values)).toArray
  }

  def fillNearest(values: Vector): DenseVector = {
    val result = values.copy.toArray
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

        if (lastExisting < 0 && nextExisting >= result.size) {
          throw new IllegalArgumentException("Input is all NaNs!")
        } else if (nextExisting >= result.size || // TODO: check this
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
    new DenseVector(result)
  }

  def fillPrevious(values: Array[Double]): Array[Double] = {
    fillPrevious(new DenseVector(values)).toArray
  }

  /**
   * fills in NaN with the previously available not NaN, scanning from left to right.
   * 1 NaN NaN 2 Nan -> 1 1 1 2 2
   */
  def fillPrevious(values: Vector): DenseVector = {
    val result = values.copy.toArray
    var filler = Double.NaN // initial value, maintains invariant
    var i = 0
    while (i < result.length) {
      filler = if (result(i).isNaN) filler else result(i)
      result(i) = filler
      i += 1
    }
    new DenseVector(result)
  }

  def fillNext(values: Array[Double]): Array[Double] = {
    fillNext(new DenseVector(values)).toArray
  }

  /**
   * fills in NaN with the next available not NaN, scanning from right to left.
   * 1 NaN NaN 2 Nan -> 1 2 2 2 NaN
   */
  def fillNext(values: Vector): DenseVector = {
    val result = values.copy.toArray
    var filler = Double.NaN // initial value, maintains invariant
    var i = result.length - 1
    while (i >= 0) {
      filler = if (result(i).isNaN) filler else result(i)
      result(i) = filler
      i -= 1
    }
    new DenseVector(result)
  }

  def fillWithDefault(values: Array[Double], filler: Double): Array[Double] = {
    fillWithDefault(new DenseVector(values), filler).toArray
  }

  /**
   * fills in NaN with a default value
   */
  def fillWithDefault(values: Vector, filler: Double): DenseVector = {
    val result = values.copy.toArray
    var i = 0
    while (i < result.length) {
      result(i) = if (result(i).isNaN) filler else result(i)
      i += 1
    }
    new DenseVector(result)
  }

  def fillLinear(values: Array[Double]): Array[Double] = {
    fillLinear(new DenseVector(values)).toArray
  }

  def fillLinear(values: Vector): DenseVector = {
    val result = values.copy.toArray
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
    new DenseVector(result)
  }

  def fillSpline(values: Array[Double]): Array[Double] = {
    fillSpline(new DenseVector(values)).toArray
  }

  /**
   * Fill in NaN values using a natural cubic spline.
   * @param values Vector to interpolate
   * @return Interpolated vector
   */
  def fillSpline(values: Vector): DenseVector = {
    val result = values.copy.toArray
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
    new DenseVector(result)
  }

  def ar(values: Vector, maxLag: Int): ARModel = Autoregression.fitModel(values, maxLag)

  /**
   * Down sample by taking every nth element starting from offset phase
   * @param values Vector to down sample
   * @param n take every nth element
   * @param phase offset from starting index
   * @return downsampled vector with appropriate length
   */
  def downsample(values: Vector, n: Int, phase: Int = 0): DenseVector = {
    val origLen = values.size
    val newLen = Math.ceil((values.size - phase) / n.toDouble).toInt
    val sampledValues = Array.fill(newLen)(0.0)
    var i = phase
    var j = 0

    while (j < newLen) {
      sampledValues(j) = values(i)
      i += n
      j += 1
    }
    new DenseVector(sampledValues)
  }

  /**
   * Up sample by inserting n - 1 elements into the original values vector, starting at index phase
   * @param values the original data vector
   * @param n the number of insertions between elements
   * @param phase the offset to begin
   * @param useZero fill with zeros rather than NaN
   * @return upsampled vector filled with zeros or NaN, as specified by user
   */
  def upsample(
      values: Vector,
      n: Int,
      phase: Int = 0,
      useZero: Boolean = false): DenseVector = {
    val filler = if (useZero) 0 else Double.NaN
    val origLen = values.size
    val newLen = origLen * n
    val sampledValues = Array.fill(newLen)(filler)
    var i = phase
    var j = 0

    while (j < origLen) {
      sampledValues(i) = values(j)
      i += n
      j += 1
    }
    new DenseVector(sampledValues)
  }

  /**
   * Difference a vector with respect to the m-th prior element. Size-preserving by leaving first
   * `m` elements intact. This is the inverse of the `inverseDifferences` function.
   * @param ts Series to difference
   * @param destTs Series to store the differenced values (and return for convenience)
   * @param lag The difference lag (e.g. x means destTs(i) = ts(i) - ts(i - x), etc)
   * @param startIndex the starting index for the differencing. Must be at least equal to lag
   * @return the differenced vector, for convenience
   */
  def differencesAtLag(
      ts: Vector,
      destTs: Vector,
      lag: Int,
      startIndex: Int): Vector = {
    require(startIndex >= lag, "starting index cannot be less than lag")
    val diffedTs = if (destTs == null) ts.copy else destTs
    if (lag == 0) {
      diffedTs
    } else {
      val arr = diffedTs.toArray
      val n = ts.size
      var i = 0

      while (i < n) {
        // elements prior to starting point are copied over without modification
        arr(i) = if (i < startIndex) ts(i) else ts(i) - ts(i - lag)
        i += 1
      }
      diffedTs
    }
  }

  /**
   * Convenience wrapper around `differencesAtLag[Vector[Double], Vector[Double], Int, Int]`
   * @param ts vector to difference
   * @param lag the difference lag (e.g. x means destTs(i) = ts(i) - ts(i - x), etc)
   * @return the differenced vector, for convenience
   */
  def differencesAtLag(ts: Vector, lag: Int): Vector = {
    differencesAtLag(ts, null, lag, lag)
  }

  /**
   * Calculate an "inverse-differenced" vector of a given lag. Size-preserving by leaving first
   * `startIndex` elements intact. This is the inverse of the `differences` function.
   * @param diffedTs differenced vector that we want to inverse
   * @param destTs Series to store the added up values (and return for convenience)
   * @param lag The difference lag (e.g. x means destTs(i) = diffedTs(i) + destTs(i - x), etc)
   * @param startIndex the starting index for the differencing. Must be at least equal to lag
   * @return the inverse differenced vector, for convenience
   */
  def inverseDifferencesAtLag(
      diffedTs: Vector,
      destTs: Vector,
      lag: Int,
      startIndex: Int): Vector = {
    require(startIndex >= lag, "starting index cannot be less than lag")
    val addedTs = if (destTs == null) diffedTs.copy else destTs
    if (lag == 0) {
      addedTs
    } else {
      val n = diffedTs.size
      var i = 0

      val arr = addedTs.toArray
      while (i < n) {
        // elements prior to starting point are copied over without modification
        arr(i) = if (i < startIndex) diffedTs(i) else diffedTs(i) + addedTs(i - lag)
        i += 1
      }
      addedTs
    }
  }

  /**
   * Convenience wrapper around `inverseDifferencesAtLag[Vector[Double], Vector[Double], Int, Int]`
   * @param diffedTs differenced vector that we want to inverse
   * @param lag the difference lag (e.g. x means destTs(i) = ts(i) - ts(i - x), etc)
   * @return the inverse differenced vector, for convenience
   */
  def inverseDifferencesAtLag(diffedTs: Vector, lag: Int): Vector = {
    inverseDifferencesAtLag(diffedTs, null, lag, lag)
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
  def differencesOfOrderD(ts: Vector, d: Int): Vector = {
    // we create 2 copies to avoid copying with every call, and simply swap them as necessary
    // for higher order differencing
    var (diffedTs, origTs) = (ts.copy, ts.copy)
    var swap: Vector = null
    for (i <- 1 to d) {
      swap = origTs
      origTs = diffedTs
      diffedTs = swap
      differencesAtLag(origTs, diffedTs, 1, i)
    }
    diffedTs
  }

  /**
   * Inverses differencing of order `d`.
   * @param diffedTs time series to reverse differencing process
   * @param d order of differencing
   * @return a vector of the same length, which when differenced to order ts, yields the original
   *         vector provided
   */
  def inverseDifferencesOfOrderD(diffedTs: Vector, d: Int): Vector = {
    val addedTs = diffedTs.copy
    for (i <- d to 1 by -1) {
      inverseDifferencesAtLag(addedTs, addedTs, 1, i)
    }
    addedTs
  }

  def rollSum(ts: Vector, n: Int): Vector = {
    new DenseVector(ts.toArray.sliding(n).toList.map(_.sum).toIndexedSeq.toArray[Double])
  }

}

