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

package com.cloudera.sparkts.api.java;

import com.cloudera.sparkts.*;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public final class DateTimeIndexFactory {
    private static final DateTimeIndex$ DATE_TIME_INDEX = DateTimeIndex$.MODULE$;

    private DateTimeIndexFactory() {}

    /**
     * Creates a UniformDateTimeIndex with the given start time, number of periods, and frequency.
     */
    public static UniformDateTimeIndex uniform(long start, int periods, Frequency frequency) {
        return DATE_TIME_INDEX.uniform(start, periods, frequency);
    }

    /**
     * Creates a UniformDateTimeIndex with the given start time, number of periods, frequency
     * and time zone.
     */
    public static UniformDateTimeIndex uniform(long start, int periods, Frequency frequency,
            ZoneId zone) {
        return DATE_TIME_INDEX.uniform(start, periods, frequency, zone);
    }

    /**
     * Creates a UniformDateTimeIndex with the given start time, number of periods, and frequency.
     */
    public static UniformDateTimeIndex uniform(ZonedDateTime start, int periods,
            Frequency frequency) {
        return DATE_TIME_INDEX.uniform(start, periods, frequency);
    }

    /**
     * Creates a UniformDateTimeIndex with the given start time, number of periods, frequency
     * and time zone.
     */
    public static UniformDateTimeIndex uniform(ZonedDateTime start, int periods,
            Frequency frequency, ZoneId zone) {
        return DATE_TIME_INDEX.uniform(start, periods, frequency, zone);
    }

    /**
     * Creates a UniformDateTimeIndex with the given start time and end time (inclusive) and
     * frequency.
     */
    public static UniformDateTimeIndex uniformFromInterval(
            long start,
            long end,
            Frequency frequency) {
        return DATE_TIME_INDEX.uniformFromInterval(start, end, frequency);
    }

    /**
     * Creates a UniformDateTimeIndex with the given start time and end time (inclusive),
     * frequency and time zone.
     */
    public static UniformDateTimeIndex uniformFromInterval(
            long start,
            long end,
            Frequency frequency,
            ZoneId zone) {
        return DATE_TIME_INDEX.uniformFromInterval(start, end, frequency, zone);
    }

    /**
     * Creates a UniformDateTimeIndex with the given start time and end time (inclusive) and
     * frequency.
     */
    public static UniformDateTimeIndex uniformFromInterval(ZonedDateTime start, ZonedDateTime end,
            Frequency frequency) {
        return DATE_TIME_INDEX.uniformFromInterval(start, end, frequency);
    }

    /**
     * Creates a UniformDateTimeIndex with the given start time and end time (inclusive), frequency
     * and time zone
     */
    public static UniformDateTimeIndex uniform(ZonedDateTime start, ZonedDateTime end,
            Frequency frequency, ZoneId zone) {
        return DATE_TIME_INDEX.uniformFromInterval(start, end, frequency, zone);
    }

    /**
     * Creates an IrregularDateTimeIndex composed of the given date-times using the time zone
     * of the first date-time in dts array.
     */
    public static IrregularDateTimeIndex irregular(ZonedDateTime[] dts) {
        return DATE_TIME_INDEX.irregular(dts);
    }

    /**
     * Creates an IrregularDateTimeIndex composed of the given date-times and zone
     */
    public static IrregularDateTimeIndex irregular(ZonedDateTime[] dts, ZoneId zone) {
        return DATE_TIME_INDEX.irregular(dts, zone);
    }

    /**
     * Creates an IrregularDateTimeIndex composed of the given date-times, as nanos from the epoch
     * using the default date-time zone.
     */
    public static IrregularDateTimeIndex irregular(long[] dts) {
        return DATE_TIME_INDEX.irregular(dts);
    }

    /**
     * Creates an IrregularDateTimeIndex composed of the given date-times, as nanos from the epoch
     * using the provided date-time zone.
     */
    public static IrregularDateTimeIndex irregular(long[] dts, ZoneId zone) {
        return DATE_TIME_INDEX.irregular(dts, zone);
    }

    /**
     * Creates a HybridDateTimeIndex composed of the given indices.
     * All indices should have the same zone.
     */
    public static HybridDateTimeIndex hybrid(DateTimeIndex[] indices) {
        return DATE_TIME_INDEX.hybrid(indices);
    }

    /**
     * Unions the given indices into a single index at the default date-time zone
     */
    public static DateTimeIndex union(DateTimeIndex[] indices) {
        return DATE_TIME_INDEX.union(indices);
    }

    /**
     * Unions the given indices into a single index at the given date-time zone
     */
    public static DateTimeIndex union(DateTimeIndex[] indices, ZoneId zone) {
        return DATE_TIME_INDEX.union(indices, zone);
    }

    /**
     * Finds the next business day occurring at or after the given date-time.
     */
    public static ZonedDateTime nextBusinessDay(ZonedDateTime dt, int firstDayOfWeek) {
        return DATE_TIME_INDEX.nextBusinessDay(dt, firstDayOfWeek);
    }

    /**
     * Parses a DateTimeIndex from the output of its toString method
     */
    public static DateTimeIndex fromString(String str) {
        return DATE_TIME_INDEX.fromString(str);
    }
}
