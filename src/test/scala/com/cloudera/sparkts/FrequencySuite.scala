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

class FrequencySuite extends FunSuite with ShouldMatchers {
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

  // Define tests to confirm that the difference function accurately deals with edge cases (such as rollover from days
  // to months
  test("business index difference") {
    // Test for single day freq
    val bDayFreq = new BusinessDayFrequency(1)
    val bDayFreq2 = new BusinessDayFrequency(2)

    val aTuesday = ZonedDateTime.of(2015, 4, 7, 0, 0, 0, 0, ZoneId.of("Z"))
    // Every 7 days we add only adds 5 business days
    // Ensure that rolling over months works
    bDayFreq.difference(aTuesday, aTuesday.plusDays(7*10)) should be(50)
    bDayFreq.difference(aTuesday.plusDays(7*10), aTuesday) should be(-50)

    //Ensure that rolling over years works
    bDayFreq.difference(aTuesday, aTuesday.plusDays(7*100)) should be(500)
    bDayFreq.difference(aTuesday.plusDays(7*100), aTuesday) should be(-500)

    //Ensure that countBy2 works
    bDayFreq2.difference(aTuesday, aTuesday.plusDays(7*10)) should be(25)
    bDayFreq2.difference(aTuesday.plusDays(7*10), aTuesday) should be(-25)

    //Ensure that rolling over years works
    bDayFreq2.difference(aTuesday, aTuesday.plusDays(7*100)) should be(250)
    bDayFreq2.difference(aTuesday.plusDays(7*100), aTuesday) should be(-250)
  }

  test("duration difference") {
    val nanoFreq = new DurationFrequency(Duration.ofNanos(1))
    val nanoFreq2 = new DurationFrequency(Duration.ofNanos(2))
    val secondsFreq = new DurationFrequency(Duration.ofSeconds(1))

    val aTime = ZonedDateTime.of(2015, 4, 7, 0, 0, 0, 100000, ZoneId.of("Z"))

    // Ensure that rolling over microseconds works
    nanoFreq.difference(aTime, aTime.plusNanos(5e3.toLong)) should be(5e3)
    nanoFreq.difference(aTime.plusNanos(5e3.toLong), aTime) should be(-5e3)

    // Ensure that rolling over milliseconds works
    nanoFreq.difference(aTime, aTime.plusNanos(5e6.toLong)) should be(5e6)
    nanoFreq.difference(aTime.plusNanos(5e6.toLong), aTime) should be(-5e6)
    
    // Ensure that rolling over seconds works
    secondsFreq.difference(aTime, aTime.plusSeconds(5e3.toLong)) should be(5e3)
    secondsFreq.difference(aTime.plusSeconds(5e3.toLong), aTime) should be(-5e3)

    //Ensure that countBy2 works
    nanoFreq2.difference(aTime, aTime.plusNanos(5e3.toLong)) should be(2.5e3)
    nanoFreq2.difference(aTime.plusNanos(5e3.toLong), aTime) should be(-2.5e3)

    // Ensure that rolling over milliseconds works
    nanoFreq2.difference(aTime, aTime.plusNanos(5e6.toLong)) should be(2.5e6)
    nanoFreq2.difference(aTime.plusNanos(5e6.toLong), aTime) should be(-2.5e6)

    //TODO This will roll-over and cause numerical errors for any difference > MAX_INT
  }

  test("month difference") {
    val monthFreq = new MonthFrequency(1)
    val monthFreq2 = new MonthFrequency(2)
    val monthFreq24 = new MonthFrequency(24)

    val aTuesday = ZonedDateTime.of(2015, 4, 7, 0, 0, 0, 0, ZoneId.of("Z"))
    // Ensure rolling over days works
    monthFreq.difference(aTuesday, aTuesday.plusDays(20)) should be(0)
    monthFreq.difference(aTuesday.plusDays(20), aTuesday) should be(0)

    monthFreq.difference(aTuesday, aTuesday.plusDays(40)) should be(1)
    monthFreq.difference(aTuesday.plusDays(40), aTuesday) should be(-1)

    // Ensure that rolling over months works
    monthFreq.difference(aTuesday, aTuesday.plusDays(70)) should be(2)
    monthFreq.difference(aTuesday.plusDays(70), aTuesday) should be(-2)

    //Ensure that rolling over years works
    monthFreq.difference(aTuesday, aTuesday.plusDays(400)) should be(13)
    monthFreq.difference(aTuesday.plusDays(400), aTuesday) should be(-13)

    // Ensure count by 2 works
    monthFreq2.difference(aTuesday, aTuesday.plusDays(40)) should be(0)
    monthFreq2.difference(aTuesday.plusDays(40), aTuesday) should be(0)

    monthFreq2.difference(aTuesday, aTuesday.plusDays(80)) should be(1)
    monthFreq2.difference(aTuesday.plusDays(80), aTuesday) should be(-1)

    // Ensure that rolling over months works
    monthFreq2.difference(aTuesday, aTuesday.plusDays(130)) should be(2)
    monthFreq2.difference(aTuesday.plusDays(130), aTuesday) should be(-2)

    //Ensure that rolling over years works
    monthFreq2.difference(aTuesday, aTuesday.plusDays(790 + 6)) should be(13)
    monthFreq2.difference(aTuesday.plusDays(790 + 6), aTuesday) should be(-13)

    // Ensure long periods works
    monthFreq24.difference(aTuesday, aTuesday.plusMonths(24*15)) should be(15)
    monthFreq24.difference(aTuesday, aTuesday.plusMonths(24*30)) should be(30)
  }

  test("day difference") {
    // Test for single day freq
    val dayFreq = new DayFrequency(1)
    val dayFreq2 = new DayFrequency(2)
    val dayFreq24 = new DayFrequency(24)

    val aTuesday = ZonedDateTime.of(2015, 4, 7, 0, 0, 0, 0, ZoneId.of("Z"))
    // Ensure rolling over days works
    dayFreq.difference(aTuesday, aTuesday.plusDays(20)) should be(20)
    dayFreq.difference(aTuesday.plusDays(20), aTuesday) should be(-20)

    dayFreq.difference(aTuesday, aTuesday.plusDays(40)) should be(40)
    dayFreq.difference(aTuesday.plusDays(40), aTuesday) should be(-40)

    // Ensure that rolling over months works
    dayFreq.difference(aTuesday, aTuesday.plusDays(70)) should be(70)
    dayFreq.difference(aTuesday.plusDays(70), aTuesday) should be(-70)

    //Ensure that rolling over years works
    dayFreq.difference(aTuesday, aTuesday.plusDays(400)) should be(400)
    dayFreq.difference(aTuesday.plusDays(400), aTuesday) should be(-400)

    // Ensure count by 2 works
    dayFreq2.difference(aTuesday, aTuesday.plusDays(40)) should be(20)
    dayFreq2.difference(aTuesday.plusDays(40), aTuesday) should be(-20)

    dayFreq2.difference(aTuesday, aTuesday.plusDays(80)) should be(40)
    dayFreq2.difference(aTuesday.plusDays(80), aTuesday) should be(-40)

    // Ensure that rolling over months works
    dayFreq2.difference(aTuesday, aTuesday.plusDays(130)) should be(65)
    dayFreq2.difference(aTuesday.plusDays(130), aTuesday) should be(-65)

    //Ensure that rolling over years works
    dayFreq2.difference(aTuesday, aTuesday.plusDays(796)) should be(398)
    dayFreq2.difference(aTuesday.plusDays(796), aTuesday) should be(-398)

    // Ensure long periods works
    dayFreq24.difference(aTuesday, aTuesday.plusDays(24 * 15)) should be(15)
    dayFreq24.difference(aTuesday, aTuesday.plusDays(24 * 30)) should be(30)
  }

  test("year difference") {
    // Test for single day freq
    val yearFreq = new YearFrequency(1)
    val yearFreq2 = new YearFrequency(2)
    val yearFrea24 = new YearFrequency(24)

    val aTuesday = ZonedDateTime.of(2015, 4, 7, 0, 0, 0, 0, ZoneId.of("Z"))
    // Ensure rolling over days works
    yearFreq.difference(aTuesday, aTuesday.plusYears(20)) should be(20)
    yearFreq.difference(aTuesday.plusYears(20), aTuesday) should be(-20)

    yearFreq.difference(aTuesday, aTuesday.plusYears(40)) should be(40)
    yearFreq.difference(aTuesday.plusYears(40), aTuesday) should be(-40)

    // Ensure that rolling over months works
    yearFreq.difference(aTuesday, aTuesday.plusYears(70)) should be(70)
    yearFreq.difference(aTuesday.plusYears(70), aTuesday) should be(-70)

    //Ensure that rolling over years works
    yearFreq.difference(aTuesday, aTuesday.plusYears(400)) should be(400)
    yearFreq.difference(aTuesday.plusYears(400), aTuesday) should be(-400)

    //Ensure that rolling over by days works
    yearFreq.difference(aTuesday, aTuesday.plusDays(796)) should be(2)
    yearFreq.difference(aTuesday.plusDays(796), aTuesday) should be(-2)

    // Ensure count by 2 works
    yearFreq2.difference(aTuesday, aTuesday.plusYears(40)) should be(20)
    yearFreq2.difference(aTuesday.plusYears(40), aTuesday) should be(-20)

    yearFreq2.difference(aTuesday, aTuesday.plusYears(80)) should be(40)
    yearFreq2.difference(aTuesday.plusYears(80), aTuesday) should be(-40)

    // Ensure that rolling over months works
    yearFreq2.difference(aTuesday, aTuesday.plusYears(130)) should be(65)
    yearFreq2.difference(aTuesday.plusYears(130), aTuesday) should be(-65)

    //Ensure that rolling over years works
    yearFreq2.difference(aTuesday, aTuesday.plusYears(796)) should be(398)
    yearFreq2.difference(aTuesday.plusYears(796), aTuesday) should be(-398)


    // Ensure long periods works
    yearFrea24.difference(aTuesday, aTuesday.plusYears(24 * 15)) should be(15)
    yearFrea24.difference(aTuesday, aTuesday.plusYears(24 * 30)) should be(30)
  }
}
