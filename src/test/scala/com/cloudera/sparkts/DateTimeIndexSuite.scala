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

import org.joda.time.DateTimeConstants
import org.scalatest.{FunSuite, ShouldMatchers}

import com.github.nscala_time.time.Imports._

import com.cloudera.sparkts.DateTimeIndex._

import org.joda.time.DateTimeZone.UTC

class DateTimeIndexSuite extends FunSuite with ShouldMatchers {

  test("to / from string") {
    val uniformIndex = uniform(new DateTime("1990-04-10"), 5, 2.businessDays)
    val uniformStr = uniformIndex.toString
    fromString(uniformStr) should be (uniformIndex)

    val irregularIndex = irregular(
      Array(new DateTime("1990-04-10"), new DateTime("1990-04-12"), new DateTime("1990-04-13")))
    val irregularStr = irregularIndex.toString
    fromString(irregularStr) should be (irregularIndex)
  }

  test("to / from string with time zone") {
    val zone = DateTimeZone.forOffsetHours(4)
    val uniformIndex = uniform(new DateTime("1990-04-10", zone), 5, 2.businessDays)
    val uniformStr = uniformIndex.toString
    fromString(uniformStr) should be (uniformIndex)

    val irregularIndex = irregular(
      Array(new DateTime("1990-04-10T01:00:00.000", zone),
        new DateTime("1990-04-12T01:00:00.000", zone),
        new DateTime("1990-04-13T01:00:00.000", zone)),
      zone)
    val irregularStr = irregularIndex.toString
    fromString(irregularStr) should be (irregularIndex)
  }

  test("uniform") {
    val index: DateTimeIndex = uniform(new DateTime("2015-04-10", UTC), 5, 2.days)
    index.size should be (5)
    index.first should be (new DateTime("2015-04-10", UTC))
    index.last should be (new DateTime("2015-04-18", UTC))

    def verifySlice(index: DateTimeIndex) = {
      index.size should be (2)
      index.first should be (new DateTime("2015-04-14", UTC))
      index.last should be (new DateTime("2015-04-16", UTC))
    }

    verifySlice(index.slice(new DateTime("2015-04-14", UTC), new DateTime("2015-04-16", UTC)))
    verifySlice(index.slice(new DateTime("2015-04-14", UTC) to new DateTime("2015-04-16", UTC)))
    verifySlice(index.islice(2, 4))
    verifySlice(index.islice(2 until 4))
    verifySlice(index.islice(2 to 3))
  }

  test("irregular") {
    val index = irregular(Array(
      "2015-04-14", "2015-04-15", "2015-04-17", "2015-04-22", "2015-04-25"
    ).map(new DateTime(_, UTC)))
    index.size should be (5)
    index.first should be (new DateTime("2015-04-14", UTC))
    index.last should be (new DateTime("2015-04-25", UTC))

    def verifySlice(index: DateTimeIndex) = {
      index.size should be (3)
      index.first should be (new DateTime("2015-04-15", UTC))
      index.last should be (new DateTime("2015-04-22", UTC))
    }

    verifySlice(index.slice(new DateTime("2015-04-15", UTC), new DateTime("2015-04-22", UTC)))
    verifySlice(index.slice(new DateTime("2015-04-15", UTC) to new DateTime("2015-04-22", UTC)))
    verifySlice(index.islice(1, 4))
    verifySlice(index.islice(1 until 4))
    verifySlice(index.islice(1 to 3))

    // TODO: test bounds that aren't members of the index
  }

  test("rebased day of week") {
    val firstDayOfWeekSunday = DateTimeConstants.SUNDAY
    rebaseDayOfWeek(DateTimeConstants.SUNDAY, firstDayOfWeekSunday) should be (1)
    rebaseDayOfWeek(DateTimeConstants.MONDAY, firstDayOfWeekSunday) should be (2)
    rebaseDayOfWeek(DateTimeConstants.TUESDAY, firstDayOfWeekSunday) should be (3)
    rebaseDayOfWeek(DateTimeConstants.WEDNESDAY, firstDayOfWeekSunday) should be (4)
    rebaseDayOfWeek(DateTimeConstants.THURSDAY, firstDayOfWeekSunday) should be (5)
    rebaseDayOfWeek(DateTimeConstants.FRIDAY, firstDayOfWeekSunday) should be (6)
    rebaseDayOfWeek(DateTimeConstants.SATURDAY, firstDayOfWeekSunday) should be (7)

    val firstDayOfWeekMonday = DateTimeConstants.MONDAY
    rebaseDayOfWeek(DateTimeConstants.SUNDAY, firstDayOfWeekMonday) should be (7)
    rebaseDayOfWeek(DateTimeConstants.MONDAY, firstDayOfWeekMonday) should be (1)
    rebaseDayOfWeek(DateTimeConstants.TUESDAY, firstDayOfWeekMonday) should be (2)
    rebaseDayOfWeek(DateTimeConstants.WEDNESDAY, firstDayOfWeekMonday) should be (3)
    rebaseDayOfWeek(DateTimeConstants.THURSDAY, firstDayOfWeekMonday) should be (4)
    rebaseDayOfWeek(DateTimeConstants.FRIDAY, firstDayOfWeekMonday) should be (5)
    rebaseDayOfWeek(DateTimeConstants.SATURDAY, firstDayOfWeekMonday) should be (6)
  }

  test("locAtDatetime returns -1") {
    val index = irregular(Array(
      "2015-04-14", "2015-04-15", "2015-04-17", "2015-04-22", "2015-04-25"
    ).map(new DateTime(_, UTC)))
    index.size should be (5)
    index.first should be (new DateTime("2015-04-14", UTC))
    index.last should be (new DateTime("2015-04-25", UTC))

    val loc1 = index.locAtDateTime(new DateTime(2015, 4, 15, 0, 0, UTC))
    loc1 should be (1)

    val loc2 = index.locAtDateTime(new DateTime(2015, 4, 16, 0, 0, UTC))
    loc2 should be (-1)

    val loc3 = index.locAtDateTime(new DateTime(2015, 4, 25, 0, 0, UTC))
    loc3 should be (4)

    val loc4 = index.locAtDateTime(new DateTime(2015, 4, 24, 0, 0, UTC))
    loc4 should be (-1)
  }

}
