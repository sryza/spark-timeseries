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

import org.scalatest.{FunSuite, ShouldMatchers}
import java.time._
import java.time.format._
import com.cloudera.sparkts.DateTimeIndex._
import org.threeten.extra.Interval

class DateTimeIndexSuite extends FunSuite with ShouldMatchers {

  val UTC = ZoneId.of("Z")

  test("LongToDateTimeZone and vice versa") {
    val zdt = ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, ZoneId.systemDefault())

    val longFromZdt = TimeSeriesUtils.zonedDateTimeToLong(zdt)
    val zdtFromLong = TimeSeriesUtils.longToZonedDateTime(longFromZdt)

    zdtFromLong should be (zdt)
  }

  test("to / from string") {
    val uniformIndex = uniform(
      ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, UTC),
      5,
      new BusinessDayFrequency(2))
    val uniformStr = uniformIndex.toString
    fromString(uniformStr) should be (uniformIndex)

    val irregularIndex = irregular(
      Array(ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, UTC),
        ZonedDateTime.of(1990, 4, 12, 0, 0, 0, 0, UTC),
        ZonedDateTime.of(1990, 4, 13, 0, 0, 0, 0, UTC)))
    val irregularStr = irregularIndex.toString
    fromString(irregularStr) should be (irregularIndex)

    val hybridIndex = hybrid(Array(uniformIndex, irregularIndex))
    val hybridStr = hybridIndex.toString
    fromString(hybridStr) should be (hybridIndex)
  }

  test("to / from string with time zone") {
    val zone = ZoneId.ofOffset("", ZoneOffset.ofHours(4))
    val uniformIndex = uniform(ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, zone), 5, 2.businessDays)
    val uniformStr = uniformIndex.toString
    fromString(uniformStr) should be (uniformIndex)

    val irregularIndex = irregular(
      Array(ZonedDateTime.of(1990, 4, 10, 0, 0, 0, 0, zone),
        ZonedDateTime.of(1990, 4, 12, 0, 0, 0, 0, zone),
        ZonedDateTime.of(1990, 4, 13, 0, 0, 0, 0, zone)))
    val irregularStr = irregularIndex.toString
    fromString(irregularStr) should be (irregularIndex)

    val hybridIndex = hybrid(Array(uniformIndex, irregularIndex))
    val hybridStr = hybridIndex.toString
    fromString(hybridStr) should be (hybridIndex)
  }

  test("uniform") {
    val index: DateTimeIndex = uniform(
      ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, UTC),
      5,
      new DayFrequency(2))
    index.size should be (5)
    index.first should be (ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, UTC))
    index.last should be (ZonedDateTime.of(2015, 4, 18, 0, 0, 0, 0, UTC))

    def verifySlice(index: DateTimeIndex): Unit = {
      index.size should be (2)
      index.first should be (ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC))
      index.last should be (ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC))
    }

    verifySlice(index.slice(ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC),
      ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC)))
    verifySlice(index.slice(Interval.of(
      ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC).toInstant(),
      ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC).toInstant())))
    verifySlice(index.islice(2, 4))
    verifySlice(index.islice(2 until 4))
    verifySlice(index.islice(2 to 3))

    index.nanosIterator.toArray should be (index.toNanosArray)
    index.zonedDateTimeIterator.toArray should be (index.toZonedDateTimeArray)
  }

  test("irregular") {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val index = irregular(Array(
      "2015-04-14 00:00:00",
      "2015-04-15 00:00:00",
      "2015-04-17 00:00:00",
      "2015-04-22 00:00:00",
      "2015-04-25 00:00:00"
    ).map(text => LocalDateTime.parse(text, formatter).atZone(UTC)), UTC)
    index.size should be (5)
    index.first should be (ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC))
    index.last should be (ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, UTC))

    def verifySlice(index: DateTimeIndex): Unit = {
      index.size should be (3)
      index.first should be (ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, UTC))
      index.last should be (ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, UTC))
    }

    verifySlice(index.slice(ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, UTC),
      ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, UTC)))
    verifySlice(index.slice(Interval.of(
      ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, UTC).toInstant(),
      ZonedDateTime.of(2015, 4, 22, 0, 0, 0, 0, UTC).toInstant())))
    verifySlice(index.islice(1, 4))
    verifySlice(index.islice(1 until 4))
    verifySlice(index.islice(1 to 3))

    index.nanosIterator.toArray should be (index.toNanosArray)
    index.zonedDateTimeIterator.toArray should be (index.toZonedDateTimeArray)
    // TODO: test bounds that aren't members of the index
  }

  test("hybrid") {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val index1 = uniform(ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, UTC),
      5, new DayFrequency(2), UTC)
    val index2 = irregular(Array(
      "2015-04-19 00:00:00",
      "2015-04-20 00:00:00",
      "2015-04-21 00:00:00",
      "2015-04-25 00:00:00",
      "2015-04-28 00:00:00"
    ).map(text => LocalDateTime.parse(text, formatter).atZone(UTC)), UTC)
    val index3 = uniform(ZonedDateTime.of(2015, 5, 10, 0, 0, 0, 0, UTC),
      5, new DayFrequency(2), UTC)

    val index = hybrid(Array(index1, index2, index3))

    index.size should be (15)
    index.first should be (ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, UTC))
    index.last should be (ZonedDateTime.of(2015, 5, 18, 0, 0, 0, 0, UTC))

    def verifySlice1(index: DateTimeIndex): Unit = {
      index.size should be (2)
      index.first should be (ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC))
      index.last should be (ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC))
    }

    verifySlice1(index.slice(ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC),
      ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC)))
    verifySlice1(index.slice(Interval.of(
      ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC).toInstant,
      ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC).toInstant)))
    verifySlice1(index.islice(2, 4))
    verifySlice1(index.islice(2 until 4))
    verifySlice1(index.islice(2 to 3))

    def verifySlice2(index: DateTimeIndex): Unit = {
      index.size should be (3)
      index.first should be (ZonedDateTime.of(2015, 4, 20, 0, 0, 0, 0, UTC))
      index.last should be (ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, UTC))
    }

    verifySlice2(index.slice(ZonedDateTime.of(2015, 4, 20, 0, 0, 0, 0, UTC),
      ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, UTC)))
    verifySlice2(index.slice(Interval.of(
      ZonedDateTime.of(2015, 4, 20, 0, 0, 0, 0, UTC).toInstant,
      ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, UTC).toInstant)))
    verifySlice2(index.islice(6, 9))
    verifySlice2(index.islice(6 until 9))
    verifySlice2(index.islice(6 to 8))

    def verifySlice3(index: DateTimeIndex): Unit = {
      index.size should be (11)
      index.first should be (ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC))
      index.last should be (ZonedDateTime.of(2015, 5, 16, 0, 0, 0, 0, UTC))
    }

    verifySlice3(index.slice(ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC),
      ZonedDateTime.of(2015, 5, 16, 0, 0, 0, 0, UTC)))
    verifySlice3(index.slice(Interval.of(
      ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC).toInstant,
      ZonedDateTime.of(2015, 5, 16, 0, 0, 0, 0, UTC).toInstant)))
    verifySlice3(index.islice(3, 14))
    verifySlice3(index.islice(3 until 14))
    verifySlice3(index.islice(3 to 13))

    index.dateTimeAtLoc(0) should be (ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, UTC))
    index.dateTimeAtLoc(4) should be (ZonedDateTime.of(2015, 4, 18, 0, 0, 0, 0, UTC))
    index.dateTimeAtLoc(5) should be (ZonedDateTime.of(2015, 4, 19, 0, 0, 0, 0, UTC))
    index.dateTimeAtLoc(7) should be (ZonedDateTime.of(2015, 4, 21, 0, 0, 0, 0, UTC))
    index.dateTimeAtLoc(9) should be (ZonedDateTime.of(2015, 4, 28, 0, 0, 0, 0, UTC))
    index.dateTimeAtLoc(10) should be (ZonedDateTime.of(2015, 5, 10, 0, 0, 0, 0, UTC))
    index.dateTimeAtLoc(14) should be (ZonedDateTime.of(2015, 5, 18, 0, 0, 0, 0, UTC))

    index.locAtDateTime(ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, UTC)) should be (0)
    index.locAtDateTime(ZonedDateTime.of(2015, 4, 18, 0, 0, 0, 0, UTC)) should be (4)
    index.locAtDateTime(ZonedDateTime.of(2015, 4, 19, 0, 0, 0, 0, UTC)) should be (5)
    index.locAtDateTime(ZonedDateTime.of(2015, 4, 21, 0, 0, 0, 0, UTC)) should be (7)
    index.locAtDateTime(ZonedDateTime.of(2015, 4, 28, 0, 0, 0, 0, UTC)) should be (9)
    index.locAtDateTime(ZonedDateTime.of(2015, 5, 10, 0, 0, 0, 0, UTC)) should be (10)
    index.locAtDateTime(ZonedDateTime.of(2015, 5, 18, 0, 0, 0, 0, UTC)) should be (14)

    index.nanosIterator.toArray should be (index.toNanosArray)
    index.zonedDateTimeIterator.toArray should be (index.toZonedDateTimeArray)
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
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val index1 = irregular(Array(
      "2015-04-14 00:00:00",
      "2015-04-15 00:00:00",
      "2015-04-17 00:00:00",
      "2015-04-22 00:00:00",
      "2015-04-25 00:00:00"
    ).map(text => LocalDateTime.parse(text, formatter).atZone(UTC)))
    val index2 = uniform(ZonedDateTime.of(2015, 5, 10, 0, 0, 0, 0, UTC),
      5, new DayFrequency(2))
    val index = hybrid(Array(index1, index2))

    index.first should be (ZonedDateTime.of(2015, 4, 14, 0, 0, 0, 0, UTC))
    index.last should be (ZonedDateTime.of(2015, 5, 18, 0, 0, 0, 0, UTC))

    val loc1 = index.locAtDateTime(ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, UTC))
    loc1 should be (1)

    val loc2 = index.locAtDateTime(ZonedDateTime.of(2015, 4, 16, 0, 0, 0, 0, UTC))
    loc2 should be (-1)

    val loc3 = index.locAtDateTime(ZonedDateTime.of(2015, 4, 25, 0, 0, 0, 0, UTC))
    loc3 should be (4)

    val loc4 = index.locAtDateTime(ZonedDateTime.of(2015, 4, 24, 0, 0, 0, 0, UTC))
    loc4 should be (-1)

    val loc5 = index.locAtDateTime(ZonedDateTime.of(2015, 5, 12, 0, 0, 0, 0, UTC))
    loc5 should be (6)

    val loc6 = index.locAtDateTime(ZonedDateTime.of(2015, 5, 13, 0, 0, 0, 0, UTC))
    loc6 should be (-1)

    val loc7 = index.locAtDateTime(ZonedDateTime.of(2015, 5, 18, 0, 0, 0, 0, UTC))
    loc7 should be (9)

    val loc8 = index.locAtDateTime(ZonedDateTime.of(2015, 5, 19, 0, 0, 0, 0, UTC))
    loc8 should be (-1)
  }
}
