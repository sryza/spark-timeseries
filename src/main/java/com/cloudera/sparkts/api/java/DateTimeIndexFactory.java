package com.cloudera.sparkts.api.java;

import com.cloudera.sparkts.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public final class DateTimeIndexFactory {
    private static final DateTimeIndex$ DATE_TIME_INDEX = DateTimeIndex$.MODULE$;

    private DateTimeIndexFactory() {}

    /**
     * Create a UniformDateTimeIndex with the given start time, number of periods, and frequency.
     */
    public static UniformDateTimeIndex uniform(long start, int periods, Frequency frequency) {
        return DATE_TIME_INDEX.uniform(start, periods, frequency);
    }

    /**
     * Create a UniformDateTimeIndex with the given start time, number of periods, frequency
     * and time zone.
     */
    public static UniformDateTimeIndex uniform(long start, int periods, Frequency frequency,
            DateTimeZone zone) {
        return DATE_TIME_INDEX.uniform(start, periods, frequency, zone);
    }

    /**
     * Create a UniformDateTimeIndex with the given start time, number of periods, and frequency.
     */
    public static UniformDateTimeIndex uniform(DateTime start, int periods, Frequency frequency) {
        return DATE_TIME_INDEX.uniform(start, periods, frequency);
    }

    /**
     * Create a UniformDateTimeIndex with the given start time, number of periods, frequency
     * and time zone.
     */
    public static UniformDateTimeIndex uniform(DateTime start, int periods, Frequency frequency,
           DateTimeZone zone) {
        return DATE_TIME_INDEX.uniform(start, periods, frequency, zone);
    }

    /**
     * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency.
     */
    public static UniformDateTimeIndex uniform(long start, long end, Frequency frequency) {
        return DATE_TIME_INDEX.uniform(start, end, frequency);
    }

    /**
     * Create a UniformDateTimeIndex with the given start time and end time (inclusive), frequency
     * and time zone.
     */
    public static UniformDateTimeIndex uniform(long start, long end, Frequency frequency,
            DateTimeZone zone) {
        return DATE_TIME_INDEX.uniform(start, end, frequency, zone);
    }

    /**
     * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency.
     */
    public static UniformDateTimeIndex uniform(DateTime start, DateTime end, Frequency frequency) {
        return DATE_TIME_INDEX.uniform(start, end, frequency);
    }

    /**
     * Create a UniformDateTimeIndex with the given start time and end time (inclusive), frequency
     * and time zone
     */
    public static UniformDateTimeIndex uniform(DateTime start, DateTime end, Frequency frequency,
           DateTimeZone zone) {
        return DATE_TIME_INDEX.uniform(start, end, frequency, zone);
    }

    /**
     * Create an IrregularDateTimeIndex composed of the given date-times using the time zone
     * of the first date-time in dts array.
     */
    public static IrregularDateTimeIndex irregular(DateTime[] dts) {
        return DATE_TIME_INDEX.irregular(dts);
    }

    /**
     * Create an IrregularDateTimeIndex composed of the given date-times and zone
     */
    public static IrregularDateTimeIndex irregular(DateTime[] dts, DateTimeZone zone) {
        return DATE_TIME_INDEX.irregular(dts, zone);
    }

    /**
     * Create an IrregularDateTimeIndex composed of the given date-times, as millis from the epoch
     * using the default date-time zone.
     */
    public static IrregularDateTimeIndex irregular(long[] dts) {
        return DATE_TIME_INDEX.irregular(dts);
    }

    /**
     * Create an IrregularDateTimeIndex composed of the given date-times, as millis from the epoch
     * using the provided date-time zone.
     */
    public static IrregularDateTimeIndex irregular(long[] dts, DateTimeZone zone) {
        return DATE_TIME_INDEX.irregular(dts, zone);
    }

    /**
     * Finds the next business day occurring at or after the given date-time.
     */
    public static DateTime nextBusinessDay(DateTime dt, int firstDayOfWeek) {
        return DATE_TIME_INDEX.nextBusinessDay(dt, firstDayOfWeek);
    }

    /**
     * Parses a DateTimeIndex from the output of its toString method
     */
    public static DateTimeIndex fromString(String str) {
        return DATE_TIME_INDEX.fromString(str);
    }
}
