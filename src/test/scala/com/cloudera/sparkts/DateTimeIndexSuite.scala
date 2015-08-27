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

import com.github.nscala_time.time.Imports._

import com.cloudera.sparkts.DateTimeIndex._

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

  test("uniform") {
    val index: DateTimeIndex = uniform(new DateTime("2015-04-10"), 5, 2.days)
    index.size should be (5)
    index.first should be (new DateTime("2015-04-10"))
    index.last should be (new DateTime("2015-04-18"))

    def verifySlice(index: DateTimeIndex) = {
      index.size should be (2)
      index.first should be (new DateTime("2015-04-14"))
      index.last should be (new DateTime("2015-04-16"))
    }

    verifySlice(index.slice(new DateTime("2015-04-14"), new DateTime("2015-04-16")))
    verifySlice(index.slice(new DateTime("2015-04-14") to new DateTime("2015-04-16")))
    verifySlice(index.islice(2, 4))
    verifySlice(index.islice(2 until 4))
    verifySlice(index.islice(2 to 3))
  }
}
