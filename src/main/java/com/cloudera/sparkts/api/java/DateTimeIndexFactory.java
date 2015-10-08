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
     * Create a UniformDateTimeIndex with the given start time, number of periods, and frequency.
     */
    public static UniformDateTimeIndex uniform(DateTime start, int periods, Frequency frequency) {
        return DATE_TIME_INDEX.uniform(start.getMillis(), periods, frequency);
    }

    /**
     * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency.
     */
    public static UniformDateTimeIndex uniform(long start, long end, Frequency frequency) {
        return uniform(start, frequency.difference(new DateTime(start, DateTimeZone.UTC),
                new DateTime(end, DateTimeZone.UTC)) + 1, frequency);
    }

    /**
     * Create a UniformDateTimeIndex with the given start time and end time (inclusive) and frequency.
     */
    public static UniformDateTimeIndex uniform(DateTime start, DateTime end, Frequency frequency) {
        return uniform(start, frequency.difference(start, end) + 1, frequency);
    }

    /**
     * Create an IrregularDateTimeIndex composed of the given date-times.
     */
    public static IrregularDateTimeIndex irregular(DateTime[] dts) {
        return DATE_TIME_INDEX.irregular(dts);
    }

    /**
     * Create an IrregularDateTimeIndex composed of the given date-times, as millis from the epoch.
     */
    public static IrregularDateTimeIndex irregular(long[] dts) {
        return DATE_TIME_INDEX.irregular(dts);
    }

    /**
     * Finds the next business day occurring at or after the given date-time.
     */
    public static DateTime nextBusinessDay(DateTime dt) {
        return DATE_TIME_INDEX.nextBusinessDay(dt);
    }

    /**
     * Parses a DateTimeIndex from the output of its toString method
     */
    public static DateTimeIndex fromString(String str) {
        return DATE_TIME_INDEX.fromString(str);
    }
}
