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

import java.time._
import com.cloudera.sparkts.DateTimeIndex._

import org.scalatest.{FunSuite, ShouldMatchers}

class BusinessDayFrequencySuite extends FunSuite with ShouldMatchers {
  test("business days") {
    def caseOf(aDay: ZonedDateTime, firstDayOfWeek: Int): Unit = {
      // don't cross a weekend
      (1, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay.plusDays(1))
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(1)) should be(1)
      (2, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay.plusDays(2))
      (1, firstDayOfWeek).businessDays.advance(aDay, 2) should be(aDay.plusDays(2))
      (2, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(2)) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(2)) should be(2)
      // go exactly a week ahead
      (5, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay.plusDays(7))
      (1, firstDayOfWeek).businessDays.advance(aDay, 5) should be(aDay.plusDays(7))
      (5, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(7)) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(7)) should be(5)
      // cross a weekend but go less than a week ahead
      (4, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay.plusDays(6))
      (1, firstDayOfWeek).businessDays.advance(aDay, 4) should be(aDay.plusDays(6))
      (4, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(6)) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(6)) should be(4)
      // go more than a week ahead
      (6, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay.plusDays(8))
      (1, firstDayOfWeek).businessDays.advance(aDay, 6) should be(aDay.plusDays(8))
      (6, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(8)) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(8)) should be(6)
      // go exactly two weeks ahead
      (10, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay.plusDays(14))
      (1, firstDayOfWeek).businessDays.advance(aDay, 10) should be(aDay.plusDays(14))
      (10, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(14)) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(14)) should be(10)
      // go more than two weeks ahead
      (12, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay.plusDays(16))
      (1, firstDayOfWeek).businessDays.advance(aDay, 12) should be(aDay.plusDays(16))
      (12, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(16)) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay.plusDays(16)) should be(12)
    }

    // got the club goin' up, on
    val aTuesday = ZonedDateTime.of(2015, 4, 7, 0, 0, 0, 0, ZoneId.of("Z"))
    caseOf(aTuesday, DayOfWeek.MONDAY.getValue)

    val aWednesday = ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, ZoneId.of("Z"))
    caseOf(aWednesday, DayOfWeek.TUESDAY.getValue)

    val aThursday = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    caseOf(aThursday, DayOfWeek.WEDNESDAY.getValue)

    val aFriday = ZonedDateTime.of(2015, 4, 10, 0, 0, 0, 0, ZoneId.of("Z"))
    caseOf(aFriday, DayOfWeek.THURSDAY.getValue)

    val aSaturday = ZonedDateTime.of(2015, 4, 11, 0, 0, 0, 0, ZoneId.of("Z"))
    caseOf(aSaturday, DayOfWeek.FRIDAY.getValue)

    val aSunday = ZonedDateTime.of(2015, 4, 12, 0, 0, 0, 0, ZoneId.of("Z"))
    caseOf(aSunday, DayOfWeek.SATURDAY.getValue)

    val aMonday = ZonedDateTime.of(2015, 4, 13, 0, 0, 0, 0, ZoneId.of("Z"))
    caseOf(aMonday, DayOfWeek.SUNDAY.getValue)
  }
}
