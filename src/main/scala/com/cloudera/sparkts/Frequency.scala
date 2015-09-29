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

import org.joda.time.{DateTimeConstants, Days, Hours}

class BusinessDayRichInt(n: Int, firstDayOfWeek: Int = DateTimeConstants.MONDAY) {
  def businessDays: BusinessDayFrequency = new BusinessDayFrequency(n, firstDayOfWeek)
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


abstract class PeriodFrequency(val period: Period) extends Frequency {
  def advance(dt: DateTime, n: Int): DateTime = dt + (n * period)

  override def equals(other: Any): Boolean = {
    other match {
      case frequency: PeriodFrequency => frequency.period == period
      case _ => false
    }
  }
}

class DayFrequency(val days: Int) extends PeriodFrequency(days.days) {

  def difference(dt1: DateTime, dt2: DateTime): Int = Days.daysBetween(dt1, dt2).getDays / days

  override def toString: String = s"days $days"
}

class HourFrequency(val hours: Int) extends PeriodFrequency(hours.hours) {

  def difference(dt1: DateTime, dt2: DateTime): Int = Hours.hoursBetween(dt1, dt2).getHours / hours

  override def toString: String = s"hours $hours"
}

class BusinessDayFrequency(val days: Int,
                           val firstDayOfWeek: Int = DateTimeConstants.MONDAY)
  extends Frequency {
  /**
   * Advances the given DateTime by (n * days) business days.
   */
  def advance(dt: DateTime, n: Int): DateTime = {
    val dayOfWeek = dt.getDayOfWeek
    val alignedDayOfWeek = aligned(dayOfWeek)
    if (alignedDayOfWeek > 5) {
      throw new IllegalArgumentException(s"$dt is not a business day")
    }
    val totalDays = n * days
    val standardWeekendDays = (totalDays / 5) * 2
    val remaining = totalDays % 5
    val extraWeekendDays = if (alignedDayOfWeek + remaining > 5) 2 else 0
    dt + (totalDays + standardWeekendDays + extraWeekendDays).days
  }

  def difference(dt1: DateTime, dt2: DateTime): Int = {
    if (dt2 < dt1) {
      return -difference(dt2, dt1)
    }
    val daysBetween = Days.daysBetween(dt1, dt2).getDays
    val dayOfWeek1 = dt1.getDayOfWeek
    val alignedDayOfWeek1 = aligned(dayOfWeek1)
    if (alignedDayOfWeek1 > 5) {
      throw new IllegalArgumentException(s"$dt1 is not a business day")
    }
    val standardWeekendDays = (daysBetween / 7) * 2
    val remaining  = daysBetween % 7
    val extraWeekendDays = if (alignedDayOfWeek1 + remaining > 5) 2 else 0
    (daysBetween - standardWeekendDays - extraWeekendDays) / days
  }

  override def equals(other: Any): Boolean = {
    other match {
      case frequency: BusinessDayFrequency => frequency.days == days
      case _ => false
    }
  }

  /**
   * Given the ISO index of the day of week, the method returns the day
   * of week index relative to the first day of week i.e. assuming the
   * the first day of week is the base index and has the value of 1
   *
   * For example, if the first day of week is set to be Sunday,
   * the week would be indexed:
   *   Sunday   : 1
   *   Monday   : 2
   *   Tuesday  : 3
   *   Wednesday: 4
   *   Thursday : 5
   *   Friday   : 6 <-- Weekend
   *   Saturday : 7 <-- Weekend
   *
   * If the first day of week is set to be Monday,
   * the week would be indexed:
   *   Monday   : 1
   *   Tuesday  : 2
   *   Wednesday: 3
   *   Thursday : 4
   *   Friday   : 5
   *   Saturday : 6 <-- Weekend
   *   Sunday   : 7 <-- Weekend
   *
   * @param dayOfWeek the ISO index of the day of week
   * @return the day of week index aligned w.r.t the first day of week
   */
  private[sparkts] def aligned(dayOfWeek: Int) : Int = {
    (dayOfWeek - firstDayOfWeek + DateTimeConstants.MONDAY +
      DateTimeConstants.DAYS_PER_WEEK - 1) % DateTimeConstants.DAYS_PER_WEEK + 1
  }

  override def toString: String = s"businessDays $days"
}
