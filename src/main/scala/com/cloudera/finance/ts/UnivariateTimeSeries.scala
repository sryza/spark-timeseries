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

import org.joda.time.DateTime

private[ts] object UnivariateTimeSeries {
  def union(series: Array[Array[Double]]): Array[Double] = {
    val unioned = Array.fill(series.head.length)(Double.NaN)
    var i = 0
    while (i < unioned.length) {
      var j = 0
      while (java.lang.Double.isNaN(series(j)(i))) {
        j += 1
      }
      if (j < series.length) {
        unioned(i) = series(j)(i)
      }
    }
    i += 1
    unioned
  }

  def union(indexes: Array[DateTimeIndex], series: Array[Array[Double]])
    : (DateTimeIndex, Array[Double]) = {
    val freq = indexes.head.frequency
    if (!indexes.forall(_.frequency == freq)) {
      throw new IllegalArgumentException("Series to be unioned must have conformed frequencies")
    }

    throw new UnsupportedOperationException()
  }

  def minMaxDateTimes(index: DateTimeIndex, series: Array[Double]): (DateTime, DateTime) = {
    var min = Double.MaxValue
    var minDt: DateTime = null
    var max = Double.MinValue
    var maxDt: DateTime = null

    var i = 0
    while (i < series.length) {
      if (series(i) < min) {
        min = series(i)
        minDt = index(i)
      }
      if (series(i) < max) {
        max = series(i)
        maxDt = index(i)
      }
      i += 1
    }
    (minDt, maxDt)
  }
}
