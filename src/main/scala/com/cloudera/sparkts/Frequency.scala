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

import com.github.nscala_time.time.Imports._

import org.joda.time.Days

class BusinessDayRichInt(n: Int) {
  def businessDays: BusinessDayFrequency = new BusinessDayFrequency(n)
}

/**
 * A frequency for a uniform index.
 */
trait Frequency extends Serializable {
  /**
   * Advances the given DateTime by this frequency n times.
   */
  def advance(dt: DateTime, n: Int): DateTime

  /**
   * The number of times this frequency occurs between the two DateTimes, rounded down.
   */
  def difference(dt1: DateTime, dt2: DateTime): Int
}

class DayFrequency(val days: Int) extends Frequency {
  val period = days.days

  def advance(dt: DateTime, n: Int): DateTime = dt + (n * period)

  def difference(dt1: DateTime, dt2: DateTime): Int = Days.daysBetween(dt1, dt2).getDays / days

  override def equals(other: Any): Boolean = {
    other match {
      case frequency: DayFrequency => frequency.days == days
      case _ => false
    }
  }

  override def toString: String = s"days $days"
}

class BusinessDayFrequency(val days: Int) extends Frequency {
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

  def difference(dt1: DateTime, dt2: DateTime): Int = {
    if (dt2 < dt1) {
      return -difference(dt2, dt1)
    }
    val daysBetween = Days.daysBetween(dt1, dt2).getDays
    val dayOfWeek1 = dt1.getDayOfWeek
    if (dayOfWeek1 > 5) {
      throw new IllegalArgumentException(s"$dt1 is not a business day")
    }
    val standardWeekendDays = (daysBetween / 7) * 2
    val remaining  = daysBetween % 7
    val extraWeekendDays = if (dayOfWeek1 + remaining > 5) 2 else 0
    (daysBetween - standardWeekendDays - extraWeekendDays) / days
  }

  override def equals(other: Any): Boolean = {
    other match {
      case frequency: BusinessDayFrequency => frequency.days == days
      case _ => false
    }
  }

  override def toString: String = s"businessDays $days"
}