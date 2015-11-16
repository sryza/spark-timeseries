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

import java.time.DayOfWeek

import codes.reactive.scalatime.format.DateTimeFormatter
import org.scalatest.{FunSuite, ShouldMatchers}
import codes.reactive.scalatime._
import com.cloudera.sparkts.DateTimeIndex._
import org.threeten.extra.Interval

class DateTimeIndexSuite extends FunSuite with ShouldMatchers {

  test("to / from string") {
    val uniformIndex = uniform(
      java.time.ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, ZoneId.system),
      5,
      new BusinessDayFrequency(2))
    val uniformStr = uniformIndex.toString
    fromString(uniformStr) should be (uniformIndex)

    val irregularIndex = irregular(
      Array(ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, ZoneId.UTC),
        ZonedDateTime.of(1990, 4, 12, 0, 0, 0, 0, ZoneId.UTC),
        ZonedDateTime.of(1990, 4, 13, 0, 0, 0, 0, ZoneId.UTC)))
    val irregularStr = irregularIndex.toString
    fromString(irregularStr) should be (irregularIndex)
  }

  test("to / from string with time zone") {
    val zone = ZoneId("", ZoneOffset(4))
    val uniformIndex = uniform(ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, zone), 5, 2.businessDays)
    val uniformStr = uniformIndex.toString
    fromString(uniformStr) should be (uniformIndex)

    val irregularIndex = irregular(
      Array(ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, zone),
        ZonedDateTime.of(1990, 4, 12, 0, 0, 0, 0, zone),
        ZonedDateTime.of(1990, 4, 13, 0, 0, 0, 0, zone)))
    val irregularStr = irregularIndex.toString
    fromString(irregularStr) should be (irregularIndex)
  }

  test("uniform") {
    val index: DateTimeIndex = uniform(
      ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, ZoneId.UTC),
      5,
      new DayFrequency(2))
    index.size should be (5)
    index.first should be (ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, ZoneId.UTC))
    index.last should be (ZonedDateTime.of(2015, 4, 18, 0, 0, 0, 0, ZoneId.UTC))

    def verifySlice(index: DateTimeIndex) = {
      index.size should be (2)
      index.first should be (ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, ZoneId.UTC))
      index.last should be (ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, ZoneId.UTC))
    }

    verifySlice(index.slice(ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, ZoneId.UTC),
      ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, ZoneId.UTC)))
    verifySlice(index.slice(Interval.of(
      ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, ZoneId.UTC).toInstant(),
      ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, ZoneId.UTC).toInstant())))
    verifySlice(index.islice(2, 4))
    verifySlice(index.islice(2 until 4))
    verifySlice(index.islice(2 to 3))
  }

  test("irregular") {
    val formatter = DateTimeFormatter("yyyy-MM-dd HH:mm:ss")
    val index = irregular(Array(
      "2015-04-14 00:00:00",
      "2015-04-15 00:00:00",
      "2015-04-17 00:00:00",
      "2015-04-22 00:00:00",
      "2015-04-25 00:00:00"
    ).map(text => LocalDateTime.parse(text, formatter).atZone(ZoneId.UTC)))
    index.size should be (5)
    index.first should be (ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, ZoneId.UTC))
    index.last should be (ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, ZoneId.UTC))

    def verifySlice(index: DateTimeIndex) = {
      index.size should be (3)
      index.first should be (ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, ZoneId.UTC))
      index.last should be (ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, ZoneId.UTC))
    }

    verifySlice(index.slice(ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, ZoneId.UTC),
      ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, ZoneId.UTC)))
    verifySlice(index.slice(Interval.of(
      ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, ZoneId.UTC).toInstant(),
      ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, ZoneId.UTC).toInstant())))
    verifySlice(index.islice(1, 4))
    verifySlice(index.islice(1 until 4))
    verifySlice(index.islice(1 to 3))

    // TODO: test bounds that aren't members of the index
  }

  test("rebased day of week") {
    val firstDayOfWeekSunday = DayOfWeek.SUNDAY.getValue
    rebaseDayOfWeek(DayOfWeek.SUNDAY.getValue, firstDayOfWeekSunday) should be (1)
    rebaseDayOfWeek(DayOfWeek.MONDAY.getValue, firstDayOfWeekSunday) should be (2)
    rebaseDayOfWeek(DayOfWeek.TUESDAY.getValue, firstDayOfWeekSunday) should be (3)
    rebaseDayOfWeek(DayOfWeek.WEDNESDAY.getValue, firstDayOfWeekSunday) should be (4)
    rebaseDayOfWeek(DayOfWeek.THURSDAY.getValue, firstDayOfWeekSunday) should be (5)
    rebaseDayOfWeek(DayOfWeek.FRIDAY.getValue, firstDayOfWeekSunday) should be (6)
    rebaseDayOfWeek(DayOfWeek.SATURDAY.getValue, firstDayOfWeekSunday) should be (7)

    val firstDayOfWeekMonday = DayOfWeek.MONDAY
    rebaseDayOfWeek(DayOfWeek.SUNDAY.getValue, firstDayOfWeekMonday.getValue()) should be (7)
    rebaseDayOfWeek(DayOfWeek.MONDAY.getValue, firstDayOfWeekMonday.getValue()) should be (1)
    rebaseDayOfWeek(DayOfWeek.TUESDAY.getValue, firstDayOfWeekMonday.getValue()) should be (2)
    rebaseDayOfWeek(DayOfWeek.WEDNESDAY.getValue, firstDayOfWeekMonday.getValue()) should be (3)
    rebaseDayOfWeek(DayOfWeek.THURSDAY.getValue, firstDayOfWeekMonday.getValue()) should be (4)
    rebaseDayOfWeek(DayOfWeek.FRIDAY.getValue, firstDayOfWeekMonday.getValue()) should be (5)
    rebaseDayOfWeek(DayOfWeek.SATURDAY.getValue, firstDayOfWeekMonday.getValue()) should be (6)
  }

  test("locAtDatetime returns -1") {
    val formatter = DateTimeFormatter("yyyy-MM-dd HH:mm:ss")
    val index = irregular(Array(
      "2015-04-14 00:00:00",
      "2015-04-15 00:00:00",
      "2015-04-17 00:00:00",
      "2015-04-22 00:00:00",
      "2015-04-25 00:00:00"
    ).map(text => LocalDateTime.parse(text, formatter).atZone(ZoneId.UTC)))
    index.size should be (5)
    index.first should be (ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, ZoneId.UTC))
    index.last should be (ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, ZoneId.UTC))

    val loc1 = index.locAtDateTime(ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, ZoneId.UTC))
    loc1 should be (1)

    val loc2 = index.locAtDateTime(ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, ZoneId.UTC))
    loc2 should be (-1)

    val loc3 = index.locAtDateTime(ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, ZoneId.UTC))
    loc3 should be (4)

    val loc4 = index.locAtDateTime(ZonedDateTime.of(2015, 4, 24, 0, 0, 0, 0, ZoneId.UTC))
    loc4 should be (-1)
  }

  test("locAtOrBeforeDatetime returns previous loc") {
    val formatter = DateTimeFormatter("yyyy-MM-dd HH:mm:ss")
    val index = irregular(Array(
      "2015-04-14 00:00:00",
      "2015-04-15 00:00:00",
      "2015-04-17 00:00:00",
      "2015-04-22 00:00:00",
      "2015-04-25 00:00:00"
    ).map(text => LocalDateTime.parse(text, formatter).atZone(ZoneId.UTC)))
    index.size should be (5)
    index.first should be (ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, ZoneId.UTC))
    index.last should be (ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, ZoneId.UTC))

    val loc1 = index.locAtOrBeforeDateTime(ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, ZoneId.UTC))
    loc1 should be (1)

    val loc2 = index.locAtOrBeforeDateTime(ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, ZoneId.UTC))
    loc2 should be (1)

    val loc3 = index.locAtOrBeforeDateTime(ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, ZoneId.UTC))
    loc3 should be (4)

    val loc4 = index.locAtOrBeforeDateTime(ZonedDateTime.of(2015, 4, 24, 0, 0, 0, 0, ZoneId.UTC))
    loc4 should be (3)
  }
}
