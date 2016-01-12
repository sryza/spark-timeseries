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

import java.time.{ZonedDateTime, ZoneId}

import com.cloudera.sparkts.DateTimeIndex._
import org.scalatest.{FunSuite, ShouldMatchers}

class DateTimeIndexUtilsSuite extends FunSuite with ShouldMatchers {
  val UTC = ZoneId.of("Z")

  test("non-overlapping sorted") {
    val index1: DateTimeIndex = uniform(dt("2015-04-10"), 5, new DayFrequency(2), UTC)
    val index2: DateTimeIndex = uniform(dt("2015-05-10"), 5, new DayFrequency(2), UTC)
    val index3: DateTimeIndex = irregular(Array(
      dt("2015-06-10"),
      dt("2015-06-13"),
      dt("2015-06-15"),
      dt("2015-06-20"),
      dt("2015-06-25")
    ), UTC)

    DateTimeIndexUtils.union(Array(index1, index2, index3), UTC) should be (
      hybrid(Array(index1, index2, index3)))
  }

  test("non-overlapping non-sorted") {
    val index1: DateTimeIndex = uniform(dt("2015-04-10"), 5, new DayFrequency(2), UTC)
    val index2: DateTimeIndex = uniform(dt("2015-05-10"), 5, new DayFrequency(2), UTC)
    val index3: DateTimeIndex = irregular(Array(
      dt("2015-06-10"),
      dt("2015-06-13"),
      dt("2015-06-15"),
      dt("2015-06-20"),
      dt("2015-06-25")
    ), UTC)

    DateTimeIndexUtils.union(Array(index3, index1, index2), UTC) should be (
      hybrid(Array(index1, index2, index3)))
  }

  test("overlapping uniform and irregular") {
    val index1: DateTimeIndex = uniform(dt("2015-04-10"), 5, new DayFrequency(2), UTC)
    val index2: DateTimeIndex = uniform(dt("2015-05-10"), 5, new DayFrequency(2), UTC)
    val index3: DateTimeIndex = irregular(Array(
      dt("2015-04-09"),
      dt("2015-04-11"),
      dt("2015-05-01"),
      dt("2015-05-10"),
      dt("2015-06-25")
    ), UTC)

    DateTimeIndexUtils.union(Array(index3, index1, index2), UTC) should be (
      hybrid(Array(
        irregular(Array(
          dt("2015-04-09"),
          dt("2015-04-10"),
          dt("2015-04-11")), UTC),
        uniform(dt("2015-04-12"), 4, new DayFrequency(2), UTC),
        irregular(Array(dt("2015-05-01"),
          dt("2015-05-10")), UTC),
        uniform(dt("2015-05-12"), 4, new DayFrequency(2), UTC),
        irregular(Array(dt("2015-06-25")), UTC)
      )))
  }

  def dt(dt: String, zone: ZoneId = UTC): ZonedDateTime = {
    val splits = dt.split("-").map(_.toInt)
    ZonedDateTime.of(splits(0), splits(1), splits(2), 0, 0, 0, 0, zone)
  }
}
