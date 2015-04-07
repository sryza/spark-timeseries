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

package com.cloudera.finance.ts

import com.cloudera.finance.Util

import com.github.nscala_time.time.Imports._

class TimeSeries(val index: UniformDateTimeIndex, val data: Array[Array[Double]]) {
  def observations(): Array[Array[Double]] = Util.transpose(data)

  def differences(windowSize: Int): TimeSeries = {
    new TimeSeries(index.drop(windowSize), data.map { hist =>
      hist.sliding(windowSize).map(window => window.last - window.head).toArray
    })
  }

  def differences(): TimeSeries = differences(1)
}

private object TimeSeries {
  def select(oldIndex: UniformDateTimeIndex, newIndex: UniformDateTimeIndex): Array[Double] = {
    throw new UnsupportedOperationException()
  }

  def fillts(ts: Array[Double], fillMethod: String): Unit = {
    fillMethod match {
      case "linear" => fillLinear(ts)
      case "nearest" => fillNearest(ts)
    }
  }

  def fillts(ts: TimeSeries, fillMethod: String): Unit = {
    ts.data.foreach(fillts(_, fillMethod))
  }

  def fillNearest(values: Array[Double]): Unit = {
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
    throw new UnsupportedOperationException()
  }

  def fillPrevious(values: Array[Double]): Unit = {
    throw new UnsupportedOperationException()
  }

  def fillLinear(values: Array[Double]): Unit = {
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

trait TimeSeriesFilter extends Serializable {
  /**
   * Takes a time series of i.i.d. observations and filters it to take on this model's
   * characteristics.
   * @param ts Time series of i.i.d. observations.
   * @param dest Array to put the filtered time series, can be the same as ts.
   * @return the dest param.
   */
  def filter(ts: Array[Double], dest: Array[Double]): Array[Double]
}
