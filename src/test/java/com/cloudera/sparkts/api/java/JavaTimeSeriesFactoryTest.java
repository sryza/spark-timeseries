package com.cloudera.sparkts.api.java;

import com.cloudera.sparkts.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JavaTimeSeriesFactoryTest {
    @Test
    public void testTimeSeriesFromIrregularSamples() {
        DateTime dt = new DateTime("2015-4-8");
        List<Tuple2<DateTime, double[]>> samples = new ArrayList<>();
        samples.add(new Tuple2<>(dt, new double[]{1.0, 2.0, 3.0}));
        samples.add(new Tuple2<>(dt.plusDays(1), new double[]{4.0, 5.0, 6.0}));
        samples.add(new Tuple2<>(dt.plusDays(2), new double[]{7.0, 8.0, 9.0}));
        samples.add(new Tuple2<>(dt.plusDays(4), new double[]{10.0, 11.0, 12.0}));

        String[] labels = new String[]{"a", "b", "c", "d"};
        JavaTimeSeries<String> ts = JavaTimeSeriesFactory.timeSeriesFromIrregularSamples(
                samples, labels, null, String.class);
        assertArrayEquals(new double[]{1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, 11d, 12d},
                ts.dataAsArray(), 0);
    }

    @Test
    public void testLagsIncludingOriginals() {
        UniformDateTimeIndex originalIndex = new UniformDateTimeIndex(0, 5, new DayFrequency(1),
                DateTimeZone.getDefault());

        List<double[]> samples = new ArrayList<>();
        samples.add(new double[] { 1.0, 6.0 });
        samples.add(new double[] { 2.0, 7.0 });
        samples.add(new double[] { 3.0, 8.0 });
        samples.add(new double[] { 4.0, 9.0 });
        samples.add(new double[] { 5.0, 10.0 });

        JavaTimeSeries<String> originalTimeSeries = JavaTimeSeriesFactory.timeSeriesFromUniformSamples(
                samples, originalIndex, new String[] { "a", "b" }, String.class);

        JavaTimeSeries<String> laggedTimeSeries = originalTimeSeries.lags(
                2, true, new JavaTimeSeries.laggedStringKey());

        String laggedKeysExpected[] = new String[] { "a", "lag1(a)", "lag2(a)",
                "b", "lag1(b)", "lag2(b)" };

        assertArrayEquals(laggedKeysExpected, (String[]) laggedTimeSeries.keys());
        assertEquals(3, laggedTimeSeries.index().size());

        assertArrayEquals(new double[] { 3.0, 2.0, 1.0, 8.0, 7.0, 6.0,
                        4.0, 3.0, 2.0, 9.0, 8.0, 7.0,
                        5.0, 4.0, 3.0, 10.0, 9.0, 8.0},
                laggedTimeSeries.dataAsArray(), 0);
    }

    @Test
    public void testLagsExcludingOriginals() {
        UniformDateTimeIndex originalIndex = new UniformDateTimeIndex(0, 5, new DayFrequency(1),
                DateTimeZone.getDefault());

        List<double[]> samples = new ArrayList<>();
        samples.add(new double[] { 1.0, 6.0 });
        samples.add(new double[] { 2.0, 7.0 });
        samples.add(new double[] { 3.0, 8.0 });
        samples.add(new double[] { 4.0, 9.0 });
        samples.add(new double[] { 5.0, 10.0 });

        JavaTimeSeries<String> originalTimeSeries = JavaTimeSeriesFactory.timeSeriesFromUniformSamples(
                samples, originalIndex, new String[] { "a", "b" }, String.class);

        JavaTimeSeries<Tuple2<String, Integer>> laggedTimeSeries = originalTimeSeries.lags(2, false);

        Tuple2<String, Integer> laggedKeysExpected[] = new Tuple2[4];
        laggedKeysExpected[0] = new Tuple2<>("a", 1);
        laggedKeysExpected[1] = new Tuple2<>("a", 2);
        laggedKeysExpected[2] = new Tuple2<>("b", 1);
        laggedKeysExpected[3] = new Tuple2<>("b", 2);

        assertArrayEquals(laggedKeysExpected, (Tuple2<String, Integer>[]) laggedTimeSeries.keys());
        assertEquals(3, laggedTimeSeries.index().size());

        assertArrayEquals(new double[]{2.0, 1.0, 7.0, 6.0,
                        3.0, 2.0, 8.0, 7.0,
                        4.0, 3.0, 9.0, 8.0},
                laggedTimeSeries.dataAsArray(), 0);
    }

    @Test
    public void testCustomLags() {
        UniformDateTimeIndex originalIndex = new UniformDateTimeIndex(0, 5, new DayFrequency(1),
                DateTimeZone.getDefault());

        List<double[]> samples = new ArrayList<>();
        samples.add(new double[] { 1.0, 6.0 });
        samples.add(new double[] { 2.0, 7.0 });
        samples.add(new double[] { 3.0, 8.0 });
        samples.add(new double[] { 4.0, 9.0 });
        samples.add(new double[] { 5.0, 10.0 });

        JavaTimeSeries<String> originalTimeSeries = JavaTimeSeriesFactory.timeSeriesFromUniformSamples(
                samples, originalIndex, new String[] { "a", "b" }, String.class);

        Map<String, Tuple2<Boolean, Integer>> lagMap = new HashMap<>();
        lagMap.put("a", new Tuple2<>(true, 0));
        lagMap.put("b", new Tuple2<>(false, 2));

        JavaTimeSeries<String> laggedTimeSeries = originalTimeSeries.lags(
                lagMap, new JavaTimeSeries.laggedStringKey());

        String laggedKeysExpected[] = new String[] { "a", "lag1(b)", "lag2(b)" };

        assertArrayEquals(laggedKeysExpected, (String[]) laggedTimeSeries.keys());
        assertEquals(3, laggedTimeSeries.index().size());

        assertArrayEquals(new double[]{3.0, 7.0, 6.0,
                        4.0, 8.0, 7.0,
                        5.0, 9.0, 8.0},
                laggedTimeSeries.dataAsArray(), 0);
    }
}
