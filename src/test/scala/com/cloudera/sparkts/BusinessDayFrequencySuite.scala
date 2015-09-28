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

import com.cloudera.sparkts.DateTimeIndex._

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTimeConstants

import org.scalatest.{FunSuite, ShouldMatchers}

class BusinessDayFrequencySuite extends FunSuite with ShouldMatchers {
  test("business days") {
    def caseOf(aDay: DateTime, firstDayOfWeek: Int) {
      // don't cross a weekend
      (1, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay + 1.days)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay + 1.days) should be(1)
      (2, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay + 2.days)
      (1, firstDayOfWeek).businessDays.advance(aDay, 2) should be(aDay + 2.days)
      (2, firstDayOfWeek).businessDays.difference(aDay, aDay + 2.days) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay + 2.days) should be(2)
      // go exactly a week ahead
      (5, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay + 7.days)
      (1, firstDayOfWeek).businessDays.advance(aDay, 5) should be(aDay + 7.days)
      (5, firstDayOfWeek).businessDays.difference(aDay, aDay + 7.days) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay + 7.days) should be(5)
      // cross a weekend but go less than a week ahead
      (4, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay + 6.days)
      (1, firstDayOfWeek).businessDays.advance(aDay, 4) should be(aDay + 6.days)
      (4, firstDayOfWeek).businessDays.difference(aDay, aDay + 6.days) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay + 6.days) should be(4)
      // go more than a week ahead
      (6, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay + 8.days)
      (1, firstDayOfWeek).businessDays.advance(aDay, 6) should be(aDay + 8.days)
      (6, firstDayOfWeek).businessDays.difference(aDay, aDay + 8.days) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay + 8.days) should be(6)
      // go exactly two weeks ahead
      (10, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay + 14.days)
      (1, firstDayOfWeek).businessDays.advance(aDay, 10) should be(aDay + 14.days)
      (10, firstDayOfWeek).businessDays.difference(aDay, aDay + 14.days) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay + 14.days) should be(10)
      // go more than two weeks ahead
      (12, firstDayOfWeek).businessDays.advance(aDay, 1) should be(aDay + 16.days)
      (1, firstDayOfWeek).businessDays.advance(aDay, 12) should be(aDay + 16.days)
      (12, firstDayOfWeek).businessDays.difference(aDay, aDay + 16.days) should be(1)
      (1, firstDayOfWeek).businessDays.difference(aDay, aDay + 16.days) should be(12)
    }

    // got the club goin' up, on
    val aTuesday = new DateTime("2015-4-7")
    caseOf(aTuesday, DateTimeConstants.MONDAY)

    val aWednesday = new DateTime("2015-4-8")
    caseOf(aWednesday, DateTimeConstants.TUESDAY)

    val aThursday = new DateTime("2015-4-9")
    caseOf(aThursday, DateTimeConstants.WEDNESDAY)

    val aFriday = new DateTime("2015-4-10")
    caseOf(aFriday, DateTimeConstants.THURSDAY)

    val aSaturday = new DateTime("2015-4-11")
    caseOf(aSaturday, DateTimeConstants.FRIDAY)

    val aSunday = new DateTime("2015-4-12")
    caseOf(aSunday, DateTimeConstants.SATURDAY)

    val aMonday = new DateTime("2015-4-13")
    caseOf(aMonday, DateTimeConstants.SUNDAY)
  }

  test("aligned day of week") {
    val businessDayFrequencyStartAtSunday = (1, DateTimeConstants.SUNDAY).businessDays
    businessDayFrequencyStartAtSunday.aligned(DateTimeConstants.SUNDAY) should be (1)
    businessDayFrequencyStartAtSunday.aligned(DateTimeConstants.MONDAY) should be (2)
    businessDayFrequencyStartAtSunday.aligned(DateTimeConstants.TUESDAY) should be (3)
    businessDayFrequencyStartAtSunday.aligned(DateTimeConstants.WEDNESDAY) should be (4)
    businessDayFrequencyStartAtSunday.aligned(DateTimeConstants.THURSDAY) should be (5)
    businessDayFrequencyStartAtSunday.aligned(DateTimeConstants.FRIDAY) should be (6)
    businessDayFrequencyStartAtSunday.aligned(DateTimeConstants.SATURDAY) should be (7)

    val businessDayFrequencyStartAtMonday = (1, DateTimeConstants.MONDAY).businessDays
    businessDayFrequencyStartAtMonday.aligned(DateTimeConstants.SUNDAY) should be (7)
    businessDayFrequencyStartAtMonday.aligned(DateTimeConstants.MONDAY) should be (1)
    businessDayFrequencyStartAtMonday.aligned(DateTimeConstants.TUESDAY) should be (2)
    businessDayFrequencyStartAtMonday.aligned(DateTimeConstants.WEDNESDAY) should be (3)
    businessDayFrequencyStartAtMonday.aligned(DateTimeConstants.THURSDAY) should be (4)
    businessDayFrequencyStartAtMonday.aligned(DateTimeConstants.FRIDAY) should be (5)
    businessDayFrequencyStartAtMonday.aligned(DateTimeConstants.SATURDAY) should be (6)
  }
}
