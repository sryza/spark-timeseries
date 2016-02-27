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

import java.time.{ZonedDateTime, ZoneId}

import org.apache.spark.mllib.linalg._

import org.scalatest._

import scala.collection.mutable.ArrayBuffer

class ResampleSuite extends FunSuite with ShouldMatchers {
  def verify(
      series: String,
      closedRight: Boolean,
      stampRight: Boolean,
      result: String): Unit = {
    def seriesStrToIndexAndVec(str: String): (DateTimeIndex, Vector) = {
      val Z = ZoneId.of("Z")
      val baseDT = ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, Z)
      val seriesPointsRaw = str.toCharArray.zipWithIndex.filter(_._1 != ' ').map { case (c, i) =>
        (if (c == 'N') Double.NaN else c.toString.toDouble, i)
      }.toBuffer
      // Account for numbers with multiple digits:
      val seriesPoints = new ArrayBuffer[(Double, Int)]()
      val iter = seriesPointsRaw.iterator.buffered
      while (iter.hasNext) {
        val point = iter.next()
        if (iter.hasNext && iter.head._2 == point._2 + 1) {
          seriesPoints += ((point._1 * 10 + iter.next()._1, point._2))
        } else {
          seriesPoints += point
        }
      }
      val index = DateTimeIndex.irregular(seriesPoints.map(x => baseDT.plusDays(x._2)).toArray)
      val vec = Vectors.dense(seriesPoints.map(_._1).toArray)
      (index, vec)
    }

    val (sourceIndex, sourceVec) = seriesStrToIndexAndVec(series)
    val (resultIndex, resultVec) = seriesStrToIndexAndVec(result)

    def aggr(arr: Array[Double], start: Int, end: Int) = {
      arr.slice(start, end).sum
    }
    val resampled = Resample.resample(sourceVec, sourceIndex, resultIndex, aggr, closedRight,
      stampRight)

    resampled should be (resultVec)
  }

  test("downsampling") {
    verify(
      "0  1  2  3  4  5  6  7  8",
      false, false,
      "3        12       21"
    )

    verify(
      "0  1  2  3  4  5  6  7  8",
      true, false,
      "6        15       15"
    )

    verify(
      "1  2  3  4  5  6  7  8  9",
      false, true,
      "N        6        15"
    )

    verify(
      "0  1  2  3  4  5  6  7  8",
      true, true,
      "0        6        15"
    )

    verify(
      "0  1  2  3  4  5  6  7  8",
      false, false,
      "         12       21       N"
    )

    verify(
      "1  2  3  4  5  6  7  8  9",
      true, false,
      "         18       17        N"
    )

    verify(
      "0  1  2  3  4  5  6  7  8",
      false, true,
      "         3        12       21"
    )

    verify(
      "1  2  3  4  5  6  7  8  9",
      true, true,
      "         10       18       17"
    )

    verify(
      "0  1  2  3  4  5  6  7  8",
      false, false,
      "6         15       15"
    )

    verify(
      "0  1  2  3  4  5  6  7  8",
      true, false,
      "6         15       15"
    )

    verify(
      "1  2  3  4  5  6  7  8  9",
      false, true,
      "N         10       18"
    )

    verify(
      "0  1  2  3  4  5  6  7  8",
      true, true,
      "0         6        15"
    )
  }

  test("upsampling") {
    verify(
      "1    2    3    4    5",
      false, false,
      "1 N  2  N 3 N  4 N  5"
    )

    verify(
      "1    2    3    4    5",
      false, false,
      "1   2   N 3 N  4 N  5"
    )
  }
}
