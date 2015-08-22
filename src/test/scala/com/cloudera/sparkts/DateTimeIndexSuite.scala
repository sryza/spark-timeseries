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

import org.joda.time.DateTime

import org.scalatest.{FunSuite, ShouldMatchers}

import com.cloudera.sparkts.DateTimeIndex._

class DateTimeIndexSuite extends FunSuite with ShouldMatchers {
  test("to / from string") {
    val uniformIndex = uniform(new DateTime("1990-04-10"), 5, 2 businessDays)
    val uniformStr = uniformIndex.toString
    fromString(uniformStr) should be (uniformIndex)

    val irregularIndex = irregular(
      Array(new DateTime("1990-04-10"), new DateTime("1990-04-12"), new DateTime("1990-04-13")))
    val irregularStr = irregularIndex.toString
    fromString(irregularStr) should be (irregularIndex)
  }
}
