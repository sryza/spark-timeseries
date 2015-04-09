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

import org.joda.time.Days

class BusinessDayRichInt(n: Int) {
  def businessDays(): BusinessDayFrequency = new BusinessDayFrequency(n)
}

/**
 * A frequency for a uniform index.
 */
trait Frequency {
  /**
   * Advances the given DateTime by this frequency n times.
   */
  def advance(dt: DateTime, n: Int): DateTime

  /**
   * The number of times this frequency occurs between the two DateTimes, rounded down.
   */
  def difference(dt1: DateTime, dt2: DateTime): Int
}

class DayFrequency(days: Int) extends Frequency {
  val period = days.days

  /**
   * {@inheritDoc}
   */
  def advance(dt: DateTime, n: Int): DateTime = dt + (n * period)

  /**
   * {@inheritDoc}
   */
  def difference(dt1: DateTime, dt2: DateTime): Int = Days.daysBetween(dt1, dt2).getDays / days
}

class BusinessDayFrequency(days: Int) extends Frequency {
  /**
   * Advances the given DateTime by n business days.
   */
  def advance(dt: DateTime, n: Int): DateTime = {
    val dayOfWeek = dt.getDayOfWeek
    if (dayOfWeek > 5) {
      throw new IllegalArgumentException(s"$dt is not a business day")
    }
    val totalDays = n * days
    val standardWeekendDays = (totalDays / 5) * 2
    val remaining = totalDays % 5
    val extraWeekendDays = if (dayOfWeek + remaining > 5) 2 else 0
    dt + (totalDays + standardWeekendDays + extraWeekendDays).days
  }

  /**
   * {@inheritDoc}
   */
  def difference(dt1: DateTime, dt2: DateTime): Int = {
    throw new UnsupportedOperationException()
  }
}