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

  def ar(values: Vector[Double], maxLag: Int): ARModel = Autoregression.fitModel(values, maxLag)
}