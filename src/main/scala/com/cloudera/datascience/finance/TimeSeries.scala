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

package com.cloudera.datascience.finance

import com.github.nscala_time.time.Imports._

class TimeSeries(val dates: Array[DateTime], val data: Array[Array[Double]]) {
  def filter(): TimeSeries = {

  }
}

object TimeSeries {
  def fillTimeSeries(ts: TimeSeries, fillMethod: String): Unit = {
    fillMethod match {
      case "linear" => ts.data.foreach(fillLinear)
      case "nearest" => ts.data.foreach(fillNearest)
    }
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

  }

  def fillPrevious(values: Array[Double]): Unit = {

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
