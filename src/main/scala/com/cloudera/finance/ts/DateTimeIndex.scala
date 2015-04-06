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

import com.github.nscala_time.time.Imports._

class DateTimeIndex(val start: DateTime, val periods: Int, val frequency: Period) {

  def end(): DateTime = {

  }

  def size: Int = periods

  def locOfDateTime(dateTime: DateTime, round: Boolean): Int = {

  }

  def apply(i: Int): DateTime = {
    start + frequency * i
  }

  def slice(start: DateTime, end: DateTime): DateTimeIndex = {
    throw new UnsupportedOperationException()
  }

  def slice(startIndex: Int, endIndex: Int): DateTimeIndex = {
    new DateTimeIndex(start + (frequency * startIndex), endIndex - startIndex, frequency)
  }

  def drop(startIndex: Int): DateTimeIndex = {
    new DateTimeIndex(start + (frequency * startIndex), periods - 1, frequency)
  }

  def union(other: DateTimeIndex): DateTimeIndex = {
//    val minStart =
//    val maxEnd =
    throw new UnsupportedOperationException()
  }

  def union(others: Seq[DateTimeIndex]): DateTimeIndex = {
    others.fold(this)(_.union(_))
  }

}
