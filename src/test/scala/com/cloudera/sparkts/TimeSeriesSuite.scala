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

import breeze.linalg.DenseMatrix
import java.time._
import java.time.format._
import com.cloudera.sparkts.DateTimeIndex._
import com.cloudera.sparkts.TimeSeries._

import org.scalatest.{FunSuite, ShouldMatchers}

import MatrixUtil._
import TimeSeries._

class TimeSeriesSuite extends FunSuite with ShouldMatchers {
  test("timeSeriesFromIrregularSamples") {
    val dt = ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, ZoneId.of("Z"))
    val samples = Array(
      ((dt, Array(1.0, 2.0, 3.0))),
      ((dt.plusDays(1), Array(4.0, 5.0, 6.0))),
      ((dt.plusDays(2), Array(7.0, 8.0, 9.0))),
      ((dt.plusDays(4), Array(10.0, 11.0, 12.0)))
    )

    val labels = Array("a", "b", "c", "d")
    val ts = timeSeriesFromIrregularSamples(samples, labels)
    ts.data.valuesIterator.toArray should be ((1 to 12).map(_.toDouble).toArray)
  }

  test("lagsIncludingOriginals") {
    val originalIndex = new UniformDateTimeIndex(ZonedDateTime.now(), 5, new DayFrequency(1))

    val data = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))

    val originalTimeSeries = new TimeSeries(originalIndex, data, Array("a", "b"))

    val laggedTimeSeries = originalTimeSeries.lags(2, true, laggedStringKey)

    laggedTimeSeries.keys should be (Array("a", "lag1(a)", "lag2(a)", "b", "lag1(b)", "lag2(b)"))
    laggedTimeSeries.index.size should be (3)

    toBreeze(laggedTimeSeries.data) should be (DenseMatrix((3.0, 2.0, 1.0, 8.0, 7.0, 6.0),
      (4.0, 3.0, 2.0, 9.0, 8.0, 7.0), (5.0, 4.0, 3.0, 10.0, 9.0, 8.0)))
  }

  test("lagsExcludingOriginals") {
    val originalIndex = new UniformDateTimeIndex(ZonedDateTime.now(), 5, new DayFrequency(1))

    val data = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))

    val originalTimeSeries = new TimeSeries(originalIndex, data, Array("a", "b"))

    val laggedTimeSeries = originalTimeSeries.lags(2, false)

    laggedTimeSeries.keys should be (Array(("a", 1), ("a", 2), ("b", 1), ("b", 2)))
    laggedTimeSeries.index.size should be (3)
    toBreeze(laggedTimeSeries.data) should be (
      DenseMatrix((2.0, 1.0, 7.0, 6.0), (3.0, 2.0, 8.0, 7.0), (4.0, 3.0, 9.0, 8.0)))
  }

  test("customLags") {
    val originalIndex = new UniformDateTimeIndex(ZonedDateTime.now(), 5, new DayFrequency(1))

    val data = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))

    val originalTimeSeries = new TimeSeries(originalIndex, data, Array("a", "b"))

    val lagMap = Map[String, (Boolean, Int)](("a" -> (true, 0)), ("b" -> (false, 2)))
    val laggedTimeSeries = originalTimeSeries.lags(lagMap.toMap, laggedStringKey _)

    laggedTimeSeries.keys should be (Array("a", "lag1(b)", "lag2(b)"))
    laggedTimeSeries.index.size should be (3)
    toBreeze(laggedTimeSeries.data) should be (
      DenseMatrix((3.0, 7.0, 6.0), (4.0, 8.0, 7.0), (5.0, 9.0, 8.0)))
  }

  test("filterByInstant") {
    val dt = ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, ZoneId.of("Z"))
    val samples = Array(
      ((dt, Array(1.0, 2.0, 3.0))),
      ((dt.plusDays(1), Array(4.0, 5.0, 6.0))),
      ((dt.plusDays(2), Array(7.0, 8.0, 9.0))),
      ((dt.plusDays(4), Array(10.0, 11.0, 12.0)))
    )

    val labels = Array("a", "b", "c")
    val ts = timeSeriesFromIrregularSamples(samples, labels)

    ts.data.numRows should be (4)
    val filteredTS = ts.filterByInstant(v => v > 5.0, Array("b"))
    filteredTS.data.numRows should be (2)
    filteredTS.toInstants()(0)._1.isAfter(dt.plusDays(1)) should be (true)
    filteredTS.toInstants()(1)._1.isAfter(dt.plusDays(1)) should be (true)
  }

  test("differencesByFrequency example 1") {
    val dt = ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, ZoneId.of("Z"))
    val samples = Array(
      ((dt, Array(1.0, 2.0, 3.0))),
      ((dt.plusMinutes(1), Array(4.0, 5.0, 6.0))),
      ((dt.plusMinutes(2), Array(5.5, 6.5, 7.0))),
      ((dt.plusMinutes(4), Array(7.0, 8.0, 9.0))),
      ((dt.plusMinutes(7), Array(7.0, 8.0, 9.0))),
      ((dt.plusMinutes(7).plusSeconds(5), Array(7.1, 8.2, 9.3))),
      ((dt.plusMinutes(7).plusSeconds(15), Array(7.4, 8.4, 9.4))),
      ((dt.plusMinutes(8), Array(6.0, 7.0, 8.0))),
      ((dt.plusMinutes(12), Array(7.0, 8.0, 9.0))),
      ((dt.plusMinutes(14), Array(10.5, 11.6, 12.7)))
    )

    val labels = Array("a", "b", "c")
    val ts = timeSeriesFromIrregularSamples(samples, labels)

    val differencedTS = ts.differencesByFrequency(new MinuteFrequency(1))

    differencedTS.toInstants()(0)._1.getMinute() should be (1)
    differencedTS.toInstants()(1)._1.getMinute() should be (2)
    differencedTS.toInstants()(2)._1.getMinute() should be (4)
    differencedTS.toInstants()(3)._1.getMinute() should be (7)
    differencedTS.toInstants()(4)._1.getMinute() should be (7)
    differencedTS.toInstants()(5)._1.getMinute() should be (7)
    differencedTS.toInstants()(6)._1.getMinute() should be (8)
    differencedTS.toInstants()(7)._1.getMinute() should be (12)
    differencedTS.toInstants()(8)._1.getMinute() should be (14)

    differencedTS.toInstants()(0)._2.toArray should be (Array(3.0, 3.0, 3.0))
    differencedTS.toInstants()(1)._2.toArray should be (Array(1.5, 1.5, 1.0))
    differencedTS.toInstants()(2)._2.toArray should be (Array(1.5, 1.5, 2.0))
    differencedTS.toInstants()(3)._2.toArray should be (Array(0.0, 0.0, 0.0))
    arrayShouldBe(differencedTS.toInstants()(4)._2.toArray, Array(0.1, 0.2, 0.3), 0.01)
    arrayShouldBe(differencedTS.toInstants()(5)._2.toArray, Array(0.4, 0.4, 0.4), 0.01)
    arrayShouldBe(differencedTS.toInstants()(6)._2.toArray, Array(-1.0, -1.0, -1.0), 0.01)
    arrayShouldBe(differencedTS.toInstants()(7)._2.toArray, Array(1.0, 1.0, 1.0), 0.01)
    arrayShouldBe(differencedTS.toInstants()(8)._2.toArray, Array(3.5, 3.6, 3.7), 0.01)
  }

  test("differencesByFrequency example 2") {
    val dt = ZonedDateTime.of(2015, 4, 8, 1, 0, 0, 0, ZoneId.of("Z"))
    val samples = Array(
      ((dt, Array(3.5))),
      ((dt.plusHours(1), Array(3.6))),
      ((dt.plusHours(9), Array(4.6))),
      ((dt.plusHours(10), Array(5.9)))
    )

    val labels = Array("a")
    val ts = timeSeriesFromIrregularSamples(samples, labels)

    // This should only return 1 timestamp: 11pm, with value: 2.4
    val differencedTS = ts.differencesByFrequency(new HourFrequency(10))

    differencedTS.head._2.size shouldBe (1)
    differencedTS.head._2(0) shouldBe (2.4 +- 0.01)
  }

  def arrayShouldBe(left: Array[Double], right: Array[Double], tol: Double) = {
    for (i <- 0 until left.length)
    {
      left(i) should be (right(i) +- tol)
    }
  }
}
