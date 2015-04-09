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

object UnivariateTimeSeries {
  /**
   * Trim leading NaNs from a series.
   */
  def trimLeading(ts: Vector[Double]): Vector[Double] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Trim trailing NaNs from a series.
   */
  def trimTrailing(ts: Vector[Double]): Vector[Double] = {
    throw new UnsupportedOperationException()
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
      if (!java.lang.Double.isNaN(i)) {
        return i
      }
      i -= 1
    }
    i
  }

  def fillts(ts: Vector[Double], fillMethod: String): Unit = {
    fillMethod match {
      case "linear" => fillLinear(ts)
      case "nearest" => fillNearest(ts)
    }
  }

  def fillNearest(values: Array[Double]): Unit = {
    fillNearest(new DenseVector(values))
  }

  def fillNearest(values: Vector[Double]): Unit = {
    var lastExisting = -1
    var nextExisting = -1
    var i = 1
    while (i < values.length) {
      if (values(i).isNaN) {
        if (nextExisting < i) {
          nextExisting = i + 1
          while (nextExisting < values.length && values(nextExisting).isNaN) {
            nextExisting += 1
          }
        }

        if (lastExisting < 0 && nextExisting >= values.length) {
          throw new IllegalArgumentException("Input is all NaNs!")
        } else if (nextExisting >= values.length || // TODO: check this
          (lastExisting >= 0 && i - lastExisting < nextExisting - i)) {
          values(i) = values(lastExisting)
        } else {
          values(i) = values(nextExisting)
        }
      } else {
        lastExisting = i
      }
      i += 1
    }
  }

  def fillNext(values: Array[Double]): Unit = {
    fillNext(new DenseVector(values))
  }

  def fillNext(values: Vector[Double]): Unit = {
    throw new UnsupportedOperationException()
  }

  def fillPrevious(values: Array[Double]): Unit = {
    fillNearest(new DenseVector(values))
  }

  def fillPrevious(values: Vector[Double]): Unit = {
    throw new UnsupportedOperationException()
  }

  def fillLinear(values: Array[Double]): Unit = {
    fillNearest(new DenseVector(values))
  }

  def fillLinear(values: Vector[Double]): Unit = {
    var i = 1
    while (i < values.length - 1) {
      val rangeStart = i
      while (i < values.length - 1 && values(i).isNaN) {
        i += 1
      }
      val before = values(rangeStart - 1)
      val after = values(i)
      if (i != rangeStart && !before.isNaN && !after.isNaN) {
        val increment = (after - before) / (i - (rangeStart - 1))
        for (j <- rangeStart until i) {
          values(j) = values(j - 1) + increment
        }
      }
      i += 1
    }
  }
}
