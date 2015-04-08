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

package com.cloudera.finance.ts

import com.cloudera.finance.ts.DateTimeIndex._

import com.github.nscala_time.time.Imports._

import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers

class BusinessDayFrequencySuite extends FunSuite with ShouldMatchers {
  test("business days") {
    // got the club goin' up, on
    val aTuesday = new DateTime("2015-4-7")
    // don't cross a weekend
    1.businessDays.advance(aTuesday, 1) should be (new DateTime("2015-4-8"))
    2.businessDays.advance(aTuesday, 1) should be (new DateTime("2015-4-9"))
    1.businessDays.advance(aTuesday, 2) should be (new DateTime("2015-4-9"))
    // go exactly a week ahead
    5.businessDays.advance(aTuesday, 1) should be (new DateTime("2015-4-14"))
    1.businessDays.advance(aTuesday, 5) should be (new DateTime("2015-4-14"))
    // cross a weekend but go less than a week ahead
    4.businessDays.advance(aTuesday, 1) should be (new DateTime("2015-4-13"))
    1.businessDays.advance(aTuesday, 4) should be (new DateTime("2015-4-13"))
    // go more than a week ahead
    6.businessDays.advance(aTuesday, 1) should be (new DateTime("2015-4-15"))
    1.businessDays.advance(aTuesday, 6) should be (new DateTime("2015-4-15"))
    // go exactly two weeks ahead
    10.businessDays.advance(aTuesday, 1) should be (new DateTime("2015-4-21"))
    1.businessDays.advance(aTuesday, 10) should be (new DateTime("2015-4-21"))
    // go more than two weeks ahead
    12.businessDays.advance(aTuesday, 1) should be (new DateTime("2015-4-23"))
    1.businessDays.advance(aTuesday, 12) should be (new DateTime("2015-4-23"))
  }
}
