/**
 * Copyright (c) 2016, Cloudera, Inc. All Rights Reserved.
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

import org.apache.spark.mllib.linalg.{Vectors, Vector}

private[sparkts] object Resample {
  /**
   * Converts a time series to a new date-time index, with flexible semantics for aggregating
   * observations when downsampling.
   *
   * Based on the closedRight and stampRight parameters, resampling partitions time into non-
   * overlapping intervals, each corresponding to a date-time in the target index. Each resulting
   * value in the output series is determined by applying an aggregation function over all the
   * values that fall within the corresponding window in the input series. If no values in the
   * input series fall within the window, a NaN is used.
   *
   * Compare with the equivalent functionality in Pandas:
   * http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.resample.html
   *
   * @param ts The values of the input series.
   * @param sourceIndex The date-time index of the input series.
   * @param targetIndex The date-time index of the resulting series.
   * @param aggr Function for aggregating multiple points that fall within a window.
   * @param closedRight If true, the windows are open on the left and closed on the right. Otherwise
   *                    the windows are closed on the left and open on the right.
   * @param stampRight If true, each date-time in the resulting series marks the end of a window.
   *                   This means that all observations after the end of the last window will be
   *                   ignored. Otherwise, each date-time in the resulting series marks the start of
   *                   a window. This means that all observations after the end of the last window
   *                   will be ignored.
   * @return The values of the resampled series.
   */
  def resample(
      ts: Vector,
      sourceIndex: DateTimeIndex,
      targetIndex: DateTimeIndex,
      aggr: (Array[Double], Int, Int) => Double,
      closedRight: Boolean,
      stampRight: Boolean): Vector = {
    val tsarr = ts.toArray
    val result = new Array[Double](targetIndex.size)
    val sourceIter = sourceIndex.nanosIterator().buffered
    val targetIter = targetIndex.nanosIterator().buffered

    // Values within interval corresponding to stamp "c" (with next stamp at "n")
    //
    // !closedRight && stampRight:
    // 1 2 3 4
    //         c
    //
    // !closedRight && !stampRight:
    // 1 2 3 4
    // c       n
    //
    // closedRight && stampRight:
    // 1 2 3 4
    //       c
    //
    // closedRight && !stampRight
    //   1 2 3 4
    // c       n

    // End predicate should return true iff dt falls after the window labeled by cur DT (at i)
    val endPredicate: (Long, Long, Long) => Boolean = if (!closedRight && stampRight) {
      (cur, next, dt) => dt >= cur
    } else if (!closedRight && !stampRight) {
      (cur, next, dt) => dt >= next
    } else if (closedRight && stampRight) {
      (cur, next, dt) => dt > cur
    } else {
      (cur, next, dt) => dt > next
    }

    var i = 0 // index in result array
    var j = 0 // index in source array

    // Skip observations that don't belong with any stamp
    if (!stampRight) {
      val firstStamp = targetIter.head
      while (sourceIter.head < firstStamp || (closedRight && sourceIter.head == firstStamp)) {
        sourceIter.next()
        j += 1
      }
    }

    // Invariant is that nothing lower than j should be needed to populate result(i)
    while (i < result.length) {
      val cur = targetIter.next()
      val next = if (targetIter.hasNext) targetIter.head else Long.MaxValue
      val sourceStartIndex = j

      while (sourceIter.hasNext && !endPredicate(cur, next, sourceIter.head)) {
        sourceIter.next()
        j += 1
      }
      val sourceEndIndex = j

      if (sourceStartIndex == sourceEndIndex) {
        result(i) = Double.NaN
      } else {
        result(i) = aggr(tsarr, sourceStartIndex, sourceEndIndex)
      }

      i += 1
    }
    Vectors.dense(result)
  }
}
