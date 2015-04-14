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

import org.scalatest.{FunSuite, ShouldMatchers}

class BusinessDayFrequencySuite extends FunSuite with ShouldMatchers {
  test("business days") {
    // got the club goin' up, on
    val aTuesday = new DateTime("2015-4-7")
    // don't cross a weekend
    1.businessDays.advance(aTuesday, 1) should be (aTuesday + 1.days)
    1.businessDays.difference(aTuesday, aTuesday + 1.days) should be (1)
    2.businessDays.advance(aTuesday, 1) should be (aTuesday + 2.days)
    1.businessDays.advance(aTuesday, 2) should be (aTuesday + 2.days)
    2.businessDays.difference(aTuesday, aTuesday + 2.days) should be (1)
    1.businessDays.difference(aTuesday, aTuesday + 2.days) should be (2)
    // go exactly a week ahead
    5.businessDays.advance(aTuesday, 1) should be (aTuesday + 7.days)
    1.businessDays.advance(aTuesday, 5) should be (aTuesday + 7.days)
    5.businessDays.difference(aTuesday, aTuesday + 7.days) should be (1)
    1.businessDays.difference(aTuesday, aTuesday + 7.days) should be (5)
    // cross a weekend but go less than a week ahead
    4.businessDays.advance(aTuesday, 1) should be (aTuesday + 6.days)
    1.businessDays.advance(aTuesday, 4) should be (aTuesday + 6.days)
    4.businessDays.difference(aTuesday, aTuesday + 6.days) should be (1)
    1.businessDays.difference(aTuesday, aTuesday + 6.days) should be (4)
    // go more than a week ahead
    6.businessDays.advance(aTuesday, 1) should be (aTuesday + 8.days)
    1.businessDays.advance(aTuesday, 6) should be (aTuesday + 8.days)
    6.businessDays.difference(aTuesday, aTuesday + 8.days) should be (1)
    1.businessDays.difference(aTuesday, aTuesday + 8.days) should be (6)
    // go exactly two weeks ahead
    10.businessDays.advance(aTuesday, 1) should be (aTuesday + 14.days)
    1.businessDays.advance(aTuesday, 10) should be (aTuesday + 14.days)
    10.businessDays.difference(aTuesday, aTuesday + 14.days) should be (1)
    1.businessDays.difference(aTuesday, aTuesday + 14.days) should be (10)
    // go more than two weeks ahead
    12.businessDays.advance(aTuesday, 1) should be (aTuesday + 16.days)
    1.businessDays.advance(aTuesday, 12) should be (aTuesday + 16.days)
    12.businessDays.difference(aTuesday, aTuesday + 16.days) should be (1)
    1.businessDays.difference(aTuesday, aTuesday + 16.days) should be (12)
  }
}
