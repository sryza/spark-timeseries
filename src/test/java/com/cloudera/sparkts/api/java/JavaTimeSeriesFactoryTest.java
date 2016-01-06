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

import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import scala.Tuple2;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JavaTimeSeriesFactoryTest {
    @Test
    public void testTimeSeriesFromIrregularSamples() {
        ZonedDateTime dt = ZonedDateTime.of(2015, 4, 8, 0, 0, 0, 0, ZoneId.systemDefault());
        List<Tuple2<ZonedDateTime, double[]>> samples = new ArrayList<>();
        samples.add(new Tuple2<>(dt, new double[]{1.0, 2.0, 3.0}));
        samples.add(new Tuple2<>(dt.plusDays(1), new double[]{4.0, 5.0, 6.0}));
        samples.add(new Tuple2<>(dt.plusDays(2), new double[]{7.0, 8.0, 9.0}));
        samples.add(new Tuple2<>(dt.plusDays(4), new double[]{10.0, 11.0, 12.0}));

        String[] labels = {"a", "b", "c", "d"};
        JavaTimeSeries<String> ts = JavaTimeSeriesFactory.javaTimeSeriesFromIrregularSamples(
                samples, labels, null);
        assertArrayEquals(
                new double[]{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0},
                ts.dataAsArray(), 0);
    }

    @Test
    public void testLagsIncludingOriginals() {
        UniformDateTimeIndex originalIndex = new UniformDateTimeIndex(ZonedDateTime.now(), 5, new DayFrequency(1),
                ZoneId.systemDefault());

        List<double[]> samples = new ArrayList<>();
        samples.add(new double[] { 1.0, 6.0 });
        samples.add(new double[] { 2.0, 7.0 });
        samples.add(new double[] { 3.0, 8.0 });
        samples.add(new double[] { 4.0, 9.0 });
        samples.add(new double[] { 5.0, 10.0 });

        JavaTimeSeries<String> originalTimeSeries = JavaTimeSeriesFactory
                .javaTimeSeriesFromUniformSamples(
                    samples, originalIndex, new String[]{"a", "b"});

        JavaTimeSeries<String> laggedTimeSeries = originalTimeSeries.lags(
                2, true, new JavaTimeSeries.laggedStringKey());

        String[] laggedKeysExpected = {"a", "lag1(a)", "lag2(a)",
          "b", "lag1(b)", "lag2(b)"};

        @SuppressWarnings("unchecked")
        String[] actualKeys = (String[]) laggedTimeSeries.keys();
        assertArrayEquals(laggedKeysExpected, actualKeys);
        assertEquals(3, laggedTimeSeries.index().size());

        assertArrayEquals(new double[] { 3.0, 2.0, 1.0, 8.0, 7.0, 6.0,
                        4.0, 3.0, 2.0, 9.0, 8.0, 7.0,
                        5.0, 4.0, 3.0, 10.0, 9.0, 8.0},
                laggedTimeSeries.dataAsArray(), 0);
    }

    @Test
    public void testLagsExcludingOriginals() {
        UniformDateTimeIndex originalIndex = new UniformDateTimeIndex(ZonedDateTime.now(), 5, new DayFrequency(1),
                ZoneId.systemDefault());

        List<double[]> samples = new ArrayList<>();
        samples.add(new double[] { 1.0, 6.0 });
        samples.add(new double[] { 2.0, 7.0 });
        samples.add(new double[] { 3.0, 8.0 });
        samples.add(new double[] { 4.0, 9.0 });
        samples.add(new double[] { 5.0, 10.0 });

        JavaTimeSeries<String> originalTimeSeries = JavaTimeSeriesFactory
                .javaTimeSeriesFromUniformSamples(
                    samples, originalIndex, new String[]{"a", "b"});

        JavaTimeSeries<Tuple2<String, Integer>> laggedTimeSeries =
                originalTimeSeries.lags(2, false);

        @SuppressWarnings("unchecked")
        Tuple2<String, Integer>[] laggedKeysExpected = new Tuple2[4];
        laggedKeysExpected[0] = new Tuple2<>("a", 1);
        laggedKeysExpected[1] = new Tuple2<>("a", 2);
        laggedKeysExpected[2] = new Tuple2<>("b", 1);
        laggedKeysExpected[3] = new Tuple2<>("b", 2);

        @SuppressWarnings("unchecked")
        Tuple2<String, Integer>[] actualKeys = (Tuple2<String, Integer>[]) laggedTimeSeries.keys();
        assertArrayEquals(laggedKeysExpected, actualKeys);
        assertEquals(3, laggedTimeSeries.index().size());

        assertArrayEquals(new double[]{2.0, 1.0, 7.0, 6.0,
                        3.0, 2.0, 8.0, 7.0,
                        4.0, 3.0, 9.0, 8.0},
                laggedTimeSeries.dataAsArray(), 0);
    }

    @Test
    public void testCustomLags() {
        UniformDateTimeIndex originalIndex = new UniformDateTimeIndex(
          ZonedDateTime.now(),
          5,
          new DayFrequency(1),
          ZoneId.systemDefault());

        List<double[]> samples = new ArrayList<>();
        samples.add(new double[] { 1.0, 6.0 });
        samples.add(new double[] { 2.0, 7.0 });
        samples.add(new double[] { 3.0, 8.0 });
        samples.add(new double[] { 4.0, 9.0 });
        samples.add(new double[] { 5.0, 10.0 });

        JavaTimeSeries<String> originalTimeSeries = JavaTimeSeriesFactory
                .javaTimeSeriesFromUniformSamples(
                    samples, originalIndex, new String[]{"a", "b"});

        Map<String, Tuple2<Boolean, Integer>> lagMap = new HashMap<>();
        lagMap.put("a", new Tuple2<>(true, 0));
        lagMap.put("b", new Tuple2<>(false, 2));

        JavaTimeSeries<String> laggedTimeSeries = originalTimeSeries.lags(
                lagMap, new JavaTimeSeries.laggedStringKey());

        String[] laggedKeysExpected = {"a", "lag1(b)", "lag2(b)"};

        @SuppressWarnings("unchecked")
        String[] actualKeys = (String[]) laggedTimeSeries.keys();
        assertArrayEquals(laggedKeysExpected, actualKeys);
        assertEquals(3, laggedTimeSeries.index().size());

        assertArrayEquals(new double[]{3.0, 7.0, 6.0,
                        4.0, 8.0, 7.0,
                        5.0, 9.0, 8.0},
                laggedTimeSeries.dataAsArray(), 0);
    }
}
