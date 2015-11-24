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

    val laggedTimeSeries = originalTimeSeries.lags(2, true, TimeSeries.laggedStringKey)

    laggedTimeSeries.keys should be (Array("a", "lag1(a)", "lag2(a)", "b", "lag1(b)", "lag2(b)"))
    laggedTimeSeries.index.size should be (3)
    laggedTimeSeries.data should be (DenseMatrix((3.0, 2.0, 1.0, 8.0, 7.0, 6.0),
      (4.0, 3.0, 2.0, 9.0, 8.0, 7.0), (5.0, 4.0, 3.0, 10.0, 9.0, 8.0)))
  }

  test("lagsExcludingOriginals") {
    val originalIndex = new UniformDateTimeIndex(ZonedDateTime.now(), 5, new DayFrequency(1))

    val data = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))

    val originalTimeSeries = new TimeSeries(originalIndex, data, Array("a", "b"))

    val laggedTimeSeries = originalTimeSeries.lags(2, false)

    laggedTimeSeries.keys should be (Array(("a", 1), ("a", 2), ("b", 1), ("b", 2)))
    laggedTimeSeries.index.size should be (3)
    laggedTimeSeries.data should be (DenseMatrix((2.0, 1.0, 7.0, 6.0), (3.0, 2.0, 8.0, 7.0),
      (4.0, 3.0, 9.0, 8.0)))
  }

  test("customLags") {
    val originalIndex = new UniformDateTimeIndex(ZonedDateTime.now(), 5, new DayFrequency(1))

    val data = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))

    val originalTimeSeries = new TimeSeries(originalIndex, data, Array("a", "b"))

    val lagMap = Map[String, (Boolean, Int)](("a" -> (true, 0)), ("b" -> (false, 2)))
    val laggedTimeSeries = originalTimeSeries.lagsPerColumn[String](lagMap.toMap, (key, lagOrder) =>
      if (lagOrder == 0)
      {
        key
      } else {
        "lag" + lagOrder.toString() + "(" + key + ")"
      })

    laggedTimeSeries.keys should be (Array("a", "lag1(b)", "lag2(b)"))
    laggedTimeSeries.index.size should be (3)
    laggedTimeSeries.data should be (DenseMatrix((3.0, 7.0, 6.0), (4.0, 8.0, 7.0), (5.0, 9.0, 8.0)))
  }

  test("irregular daily lags Basic") {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val index = irregular(Array(
      "2015-04-14 00:00:00",
      "2015-04-15 00:00:00",
      "2015-04-17 00:00:00",
      "2015-04-22 00:00:00",
      "2015-04-25 00:00:00"
    ).map(text => LocalDateTime.parse(text, formatter).atZone(ZoneId.of("Z"))))

    val data = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))

    val originalTimeSeries = new TimeSeries(index, data, Array("a", "b"))

    def avgInterpol(prev: Double, next: Double): Double = {
      (prev + next) / 2.0
    }

    val laggedTimeSeries = originalTimeSeries.toUniform(2,
      new DayFrequency(1),
      false,
      TimeSeries.laggedStringKey,
      avgInterpol)

    laggedTimeSeries.keys should be (Array("lag1(a)", "lag2(a)", "lag1(b)", "lag2(b)"))
    laggedTimeSeries.index.size should be (10)
    laggedTimeSeries.index.first.getYear() should be (2015)
    laggedTimeSeries.index.first.getMonthValue() should be (4)
    laggedTimeSeries.index.first.getDayOfMonth() should be (16)
    laggedTimeSeries.index.last.getYear() should be (2015)
    laggedTimeSeries.index.last.getMonthValue() should be (4)
    laggedTimeSeries.index.last.getDayOfMonth() should be (25)

    laggedTimeSeries.data should be (DenseMatrix(
      (2.0, 1.0, 7.0, 6.0),
      (2.5, 2.0, 7.5, 7.0),
      (3.0, 2.5, 8.0, 7.5),
      (3.5, 3.0, 8.5, 8.0),
      (3.5, 3.5, 8.5, 8.5),
      (3.5, 3.5, 8.5, 8.5),
      (3.5, 3.5, 8.5, 8.5),
      (4.0, 3.5, 9.0, 8.5),
      (4.5, 4.0, 9.5, 9.0),
      (4.5, 4.5, 9.5, 9.5)
    ))
  }

  test("irregular hourly lags with DST") {
    val index = irregular(Array(
      ZonedDateTime.of(2015, 10, 31, 23, 0, 0, 0, ZoneId.of("America/New_York")),
      ZonedDateTime.of(2015, 11, 1, 1, 0, 0, 0, ZoneId.of("America/New_York")),              // this is 2015-11-01 01:00-04GMT
      ZonedDateTime.of(2015, 11, 1, 1, 0, 0, 0, ZoneId.of("America/New_York")).plusHours(1), // this is 2015-11-01 01:00-05GMT
      ZonedDateTime.of(2015, 11, 1, 2, 0, 0, 0, ZoneId.of("America/New_York")),
      ZonedDateTime.of(2015, 11, 1, 5, 0, 0, 0, ZoneId.of("America/New_York"))
    ))
    val data = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))

    val originalTimeSeries = new TimeSeries(index, data, Array("a", "b"))

    def avgInterpol(prev: Double, next: Double): Double = {
      (prev + next) / 2.0
    }

    val laggedTimeSeries = originalTimeSeries.toUniform(2,
      new HourFrequency(1),
      false,
      TimeSeries.laggedStringKey,
      avgInterpol)

    laggedTimeSeries.keys should be (Array("lag1(a)", "lag2(a)", "lag1(b)", "lag2(b)"))
    laggedTimeSeries.index.size should be (6)
    laggedTimeSeries.index.first.getYear() should be (2015)
    laggedTimeSeries.index.first.getMonthValue() should be (11)
    laggedTimeSeries.index.first.getDayOfMonth() should be (1)
    laggedTimeSeries.index.first.getHour() should be (1)
    laggedTimeSeries.index.last.getYear() should be (2015)
    laggedTimeSeries.index.last.getMonthValue() should be (11)
    laggedTimeSeries.index.last.getDayOfMonth() should be (1)
    laggedTimeSeries.index.last.getHour() should be (5)

    laggedTimeSeries.data should be (DenseMatrix(
      (1.5, 1.0, 6.5, 6.0),
      (2.0, 1.5, 7.0, 6.5),
      (3.0, 2.0, 8.0, 7.0),
      (4.0, 3.0, 9.0, 8.0),
      (4.5, 4.0, 9.5, 9.0),
      (4.5, 4.5, 9.5, 9.5)
    ))
  }

  test("irregular daily lagsPerColumn basic") {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val index = irregular(Array(
      "2015-04-14 00:00:00",
      "2015-04-15 00:00:00",
      "2015-04-17 00:00:00",
      "2015-04-22 00:00:00",
      "2015-04-25 00:00:00"
    ).map(text => LocalDateTime.parse(text, formatter).atZone(ZoneId.of("Z"))))
    val data = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))

    val originalTimeSeries = new TimeSeries(index, data, Array("a", "b"))

    def avgInterpol(prev: Double, next: Double): Double = {
      (prev + next) / 2.0
    }

    val lagMap = Map[String, (Boolean, Int)](("a" -> (true, 1)), ("b" -> (false, 2)))
    val laggedTimeSeries = originalTimeSeries.toUniformPerColumn(
      lagMap.toMap,
      new DayFrequency(1),
      TimeSeries.laggedStringKey,
      avgInterpol)

    laggedTimeSeries.keys should be (Array("a", "lag1(a)", "lag1(b)", "lag2(b)"))
    laggedTimeSeries.index.size should be (10)
    laggedTimeSeries.index.first.getYear() should be (2015)
    laggedTimeSeries.index.first.getMonthValue() should be (4)
    laggedTimeSeries.index.first.getDayOfMonth() should be (16)
    laggedTimeSeries.index.last.getYear() should be (2015)
    laggedTimeSeries.index.last.getMonthValue() should be (4)
    laggedTimeSeries.index.last.getDayOfMonth() should be (25)

    laggedTimeSeries.data should be (DenseMatrix(
      (2.5, 2.0, 7.0, 6.0),
      (3.0, 2.5, 7.5, 7.0),
      (3.5, 3.0, 8.0, 7.5),
      (3.5, 3.5, 8.5, 8.0),
      (3.5, 3.5, 8.5, 8.5),
      (3.5, 3.5, 8.5, 8.5),
      (4.0, 3.5, 8.5, 8.5),
      (4.5, 4.0, 9.0, 8.5),
      (4.5, 4.5, 9.5, 9.0),
      (5.0, 4.5, 9.5, 9.5)
    ))
  }

  test("irregular hourly lagsPerColumn with DST") {
    val index = irregular(Array(
      ZonedDateTime.of(2015, 11, 1, 0, 0, 0, 0, ZoneId.of("America/New_York")),
      ZonedDateTime.of(2015, 11, 1, 1, 0, 0, 0, ZoneId.of("America/New_York")),              // this is 2015-11-01 01:00-04GMT
      ZonedDateTime.of(2015, 11, 1, 1, 0, 0, 0, ZoneId.of("America/New_York")).plusHours(1), // this is 2015-11-01 01:00-05GMT
      ZonedDateTime.of(2015, 11, 1, 2, 0, 0, 0, ZoneId.of("America/New_York")),
      ZonedDateTime.of(2015, 11, 1, 5, 0, 0, 0, ZoneId.of("America/New_York"))
    ))
    val data = DenseMatrix((1.0, 6.0), (2.0, 7.0), (3.0, 8.0), (4.0, 9.0), (5.0, 10.0))

    val originalTimeSeries = new TimeSeries(index, data, Array("a", "b"))

    def avgInterpol(prev: Double, next: Double): Double = {
      (prev + next) / 2.0
    }

    val lagMap = Map[String, (Boolean, Int)](("a" -> (true, 1)), ("b" -> (false, 2)))
    val laggedTimeSeries = originalTimeSeries.toUniformPerColumn(
      lagMap.toMap,
      new HourFrequency(1),
      TimeSeries.laggedStringKey,
      avgInterpol)

    laggedTimeSeries.keys should be (Array("a", "lag1(a)", "lag1(b)", "lag2(b)"))
    laggedTimeSeries.index.size should be (5)
    laggedTimeSeries.index.first.getYear() should be (2015)
    laggedTimeSeries.index.first.getMonthValue() should be (11)
    laggedTimeSeries.index.first.getDayOfMonth() should be (1)
    laggedTimeSeries.index.first.getHour() should be (1)
    laggedTimeSeries.index.last.getYear() should be (2015)
    laggedTimeSeries.index.last.getMonthValue() should be (11)
    laggedTimeSeries.index.last.getDayOfMonth() should be (1)
    laggedTimeSeries.index.last.getHour() should be (5)

    laggedTimeSeries.data should be (DenseMatrix(
      (3.0, 2.0, 7.0, 6.0),
      (4.0, 3.0, 8.0, 7.0),
      (4.5, 4.0, 9.0, 8.0),
      (4.5, 4.5, 9.5, 9.0),
      (5.0, 4.5, 9.5, 9.5)
    ))
  }

}
