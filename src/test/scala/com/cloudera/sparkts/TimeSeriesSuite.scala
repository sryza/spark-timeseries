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

import java.time.temporal.ChronoUnit

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

    val laggedTimeSeries = originalTimeSeries.lags(2, true, laggedStringKey _)

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

  test("union two time series") {
    val index1 = new UniformDateTimeIndex(ZonedDateTime.now(), 5, new DayFrequency(1))
    val data1 = DenseMatrix((11.0, 16.0), (12.0, 17.0), (13.0, 18.0), (14.0, 19.0), (15.0, 20.0))
    val ts1 = new TimeSeries(index1, data1, Array("c", "d"))

    val index2 = new UniformDateTimeIndex(ZonedDateTime.now().minusMonths(1), 5, new DayFrequency(1))
    val data2 = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))
    val ts2 = new TimeSeries(index2, data2, Array("a", "b"))

    val defVal = -10.0
    val tsUnion = ts1.union(defVal, ts2)

    tsUnion.keys should be (Array("c", "d", "a", "b"))
    tsUnion.index.size should be (10)
    toBreeze(tsUnion.data) should be (DenseMatrix(
      (defVal, defVal, 1.0, 6.0),
      (defVal, defVal, 2.0, 7.0),
      (defVal, defVal, 3.0, 8.0),
      (defVal, defVal, 4.0, 9.0),
      (defVal, defVal, 5.0, 10.0),
      (11.0, 16.0, defVal, defVal),
      (12.0, 17.0, defVal, defVal),
      (13.0, 18.0, defVal, defVal),
      (14.0, 19.0, defVal, defVal),
      (15.0, 20.0, defVal, defVal)))
  }

  test("intersect two time series") {
    val dt = ZonedDateTime.now().truncatedTo(ChronoUnit.DAYS)
    val index1 = new UniformDateTimeIndex(dt, 5, new DayFrequency(1))
    val data1 = DenseMatrix((11.0, 16.0), (12.0, 17.0), (13.0, 18.0), (14.0, 19.0), (15.0, 20.0))
    val ts1 = new TimeSeries(index1, data1, Array("c", "d"))

    val index2 = new UniformDateTimeIndex(dt.minusDays(1), 5, new DayFrequency(1))
    val data2 = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))
    val ts2 = new TimeSeries(index2, data2, Array("a", "b"))

    val tsIntersectOption = ts1.intersect(ts2)

    tsIntersectOption.nonEmpty should be (true)

    val tsIntersect = tsIntersectOption.get

    tsIntersect.keys should be (Array("c", "d", "a", "b"))
    tsIntersect.index.size should be (4)
    toBreeze(tsIntersect.data) should be (DenseMatrix(
      (11.0, 16.0, 2.0, 7.0),
      (12.0, 17.0, 3.0, 8.0),
      (13.0, 18.0, 4.0, 9.0),
      (14.0, 19.0, 5.0, 10.0)))
  }

  test("left and right joins") {
    val dt = ZonedDateTime.now().truncatedTo(ChronoUnit.DAYS)
    val index1 = new UniformDateTimeIndex(dt, 5, new DayFrequency(1))
    val data1 = DenseMatrix((11.0, 16.0), (12.0, 17.0), (13.0, 18.0), (14.0, 19.0), (15.0, 20.0))
    val ts1 = new TimeSeries(index1, data1, Array("c", "d"))

    val index2 = new UniformDateTimeIndex(dt.minusDays(1), 5, new DayFrequency(1))
    val data2 = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))
    val ts2 = new TimeSeries(index2, data2, Array("a", "b"))

    val defVal = -1.0

    val tsLeftJoined = ts1.leftJoin(ts2, defVal)
    val tsRightJoined = ts1.rightJoin(ts2, defVal)

    tsLeftJoined.keys should be (Array("c", "d", "a", "b"))
    tsLeftJoined.index.size should be (5)
    toBreeze(tsLeftJoined.data) should be (DenseMatrix(
      (11.0, 16.0, 2.0, 7.0),
      (12.0, 17.0, 3.0, 8.0),
      (13.0, 18.0, 4.0, 9.0),
      (14.0, 19.0, 5.0, 10.0),
      (15.0, 20.0, defVal, defVal)))

    tsRightJoined.keys should be (Array("c", "d", "a", "b"))
    tsRightJoined.index.size should be (5)
    toBreeze(tsRightJoined.data) should be (DenseMatrix(
      (defVal, defVal, 1.0, 6.0),
      (11.0, 16.0, 2.0, 7.0),
      (12.0, 17.0, 3.0, 8.0),
      (13.0, 18.0, 4.0, 9.0),
      (14.0, 19.0, 5.0, 10.0)))
  }
}
