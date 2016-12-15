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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
import org.apache.spark.sql.SQLContext;

import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConversions;
import scala.collection.JavaConversions$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.RichInt;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JavaTimeSeriesRDDFactoryTest implements Serializable {
    private static double[] until(int a, int b) {
        Collection<Object> collection = JavaConversions
                .asJavaCollection(new RichInt(a).until(b));
        double[] res = new double[collection.size()];
        Iterator<Object> iter = collection.iterator();
        int i = 0;
        while (iter.hasNext()) {
            res[i++] = (int) iter.next();
        }
        return res;
    }

    private static double[] untilBy(int a, int b, int step) {
        Collection<Object> collection = JavaConversions
                .asJavaCollection(new RichInt(a).until(b).by(step));
        double[] res = new double[collection.size()];
        Iterator<Object> iter = collection.iterator();
        int i = 0;
        while (iter.hasNext()) {
            res[i++] = (int) iter.next();
        }
        return res;
    }

    private static Row rowFrom(Timestamp timestamp, double[] data) {
        List<Object> list = new ArrayList<>(data.length + 1);
        list.add(timestamp);
        for(double d: data) {
            list.add(d);
        }
        return Row$.MODULE$.fromSeq(JavaConversions$.MODULE$.asScalaBuffer(list).toSeq());
    }

    private static <T> ClassTag<T> classTagOf(Class<T> clazz) {
        return ClassTag$.MODULE$.apply(clazz);
    }

    private JavaSparkContext init() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(getClass().getName());
        TimeSeriesKryoRegistrator.registerKryoClasses(conf);
        return new JavaSparkContext(conf);
    }

    @Test
    public void testSlice() {
        try (JavaSparkContext sc = init()) {
            ZonedDateTime start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.systemDefault());
            UniformDateTimeIndex index =
                DateTimeIndexFactory.uniform(start, 10, new DayFrequency(1));
            List<Tuple3<String, UniformDateTimeIndex, Vector>> list = new ArrayList<>();
            list.add(new Tuple3<>("0.0", index, (Vector) new DenseVector(until(0, 10))));
            list.add(new Tuple3<>("10.0", index, (Vector) new DenseVector(until(10, 20))));
            list.add(new Tuple3<>("20.0", index, (Vector) new DenseVector(until(20, 30))));

            JavaTimeSeriesRDD<String> rdd = JavaTimeSeriesRDDFactory.timeSeriesRDD(
                index, sc.parallelize(list));
            JavaTimeSeriesRDD<String> slice = rdd.slice(start.plusDays(1), start.plusDays(6));

            assertEquals(DateTimeIndexFactory.uniform(start.plusDays(1), 6, new DayFrequency(1)),
                         slice.index());

            Map<String, Vector> contents = slice.collectAsMap();
            assertEquals(3, contents.size());
            assertEquals(new DenseVector(until(1, 7)), contents.get("0.0"));
            assertEquals(new DenseVector(until(11, 17)), contents.get("10.0"));
            assertEquals(new DenseVector(until(21, 27)), contents.get("20.0"));
        }
    }

    @Test
    public void testFilterEndingAfter() {
        try (JavaSparkContext sc = init()) {
            ZonedDateTime start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.systemDefault());
            UniformDateTimeIndex index =
                DateTimeIndexFactory.uniform(start, 10, new DayFrequency(1));
            List<Tuple3<String, UniformDateTimeIndex, Vector>> list = new ArrayList<>();
            list.add(new Tuple3<>("0.0", index, (Vector) new DenseVector(until(0, 10))));
            list.add(new Tuple3<>("10.0", index, (Vector) new DenseVector(until(10, 20))));
            list.add(new Tuple3<>("20.0", index, (Vector) new DenseVector(until(20, 30))));

            JavaTimeSeriesRDD<String> rdd = JavaTimeSeriesRDDFactory.timeSeriesRDD(
                index, sc.parallelize(list));
            assertEquals(3, rdd.filterEndingAfter(start).count());
        }
    }

    @Test
    public void testToInstants() {
        try (JavaSparkContext sc = init()) {
            ZonedDateTime start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.systemDefault());
            String[] labels = {"a", "b", "c", "d", "e"};
            double[] seeds = untilBy(0, 20, 4);
            List<Tuple2<String, Vector>> list = new ArrayList<>();
            for (int i = 0; i < seeds.length; i++) {
              double seed = seeds[i];
              list.add(new Tuple2<>(labels[i],
                                    (Vector) new DenseVector(until((int) seed, (int) seed + 4))));
            }
            UniformDateTimeIndex index =
                DateTimeIndexFactory.uniform(start, 4, new DayFrequency(1));

            JavaPairRDD<String, Vector> rdd = sc.parallelizePairs(list, 3);
            JavaTimeSeriesRDD<String> tsRdd = JavaTimeSeriesRDDFactory.timeSeriesRDD(
                index, rdd);

            List<Tuple2<ZonedDateTime, Vector>> samples = tsRdd.toInstants().collect();
            assertEquals(
                Arrays.asList(new Tuple2<>(start, new DenseVector(untilBy(0, 20, 4))),
                              new Tuple2<>(start.plusDays(1), new DenseVector(untilBy(1, 20, 4))),
                              new Tuple2<>(start.plusDays(2), new DenseVector(untilBy(2, 20, 4))),
                              new Tuple2<>(start.plusDays(3), new DenseVector(untilBy(3, 20, 4)))),
                samples);
        }
    }

    @Test
    public void testToInstantsDataFrame() {
        JavaSparkContext sc = init();

        SQLContext sqlContext = new SQLContext(sc);

        ZonedDateTime start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.systemDefault());
        String[] labels = { "a", "b", "c", "d", "e" };
        double[] seeds = untilBy(0, 20, 4);
        List<Tuple2<String, Vector>> list = new ArrayList<>();
        for(int i = 0; i < seeds.length; i++) {
            double seed = seeds[i];
            list.add(new Tuple2<>(labels[i],
                    (Vector) new DenseVector(until((int) seed, (int) seed + 4))));
        }
        UniformDateTimeIndex index = DateTimeIndexFactory.uniform(start, 4, new DayFrequency(1));

        JavaPairRDD<String, Vector> rdd = sc.parallelizePairs(list, 3);
        JavaTimeSeriesRDD<String> tsRdd = JavaTimeSeriesRDDFactory.timeSeriesRDD(
                index, rdd);

        Dataset<Row> samplesDF = tsRdd.toInstantsDataFrame(sqlContext);
        Row[] sampleRows = (Row[])samplesDF.collect();
        String[] columnNames = samplesDF.columns();
        String[] columnNamesTail = new String[columnNames.length - 1];
        System.arraycopy(columnNames, 1, columnNamesTail, 0, columnNamesTail.length);

        assertEquals(labels.length + 1 /*labels + timestamp*/, columnNames.length);
        assertEquals("instant", columnNames[0]);
        assertArrayEquals(labels, columnNamesTail);

        assertArrayEquals(new Row[] {
          rowFrom(Timestamp.from(start.toInstant()), untilBy(0, 20, 4)),
          rowFrom(Timestamp.from(start.plusDays(1).toInstant()), untilBy(1, 20, 4)),
          rowFrom(Timestamp.from(start.plusDays(2).toInstant()), untilBy(2, 20, 4)),
          rowFrom(Timestamp.from(start.plusDays(3).toInstant()), untilBy(3, 20, 4))
        }, (Row[])sampleRows);

        sc.close();
    }

    @Test
    public void testSaveLoad() throws Exception {
        JavaSparkContext sc = init();

        ZonedDateTime start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.systemDefault());
        UniformDateTimeIndex index = DateTimeIndexFactory.uniform(start, 10, new DayFrequency(1));
        List<Tuple3<String, UniformDateTimeIndex, Vector>> list = new ArrayList<>();
        list.add(new Tuple3<>("0.0", index, (Vector) new DenseVector(until(0, 10))));
        list.add(new Tuple3<>("10.0", index, (Vector) new DenseVector(until(10, 20))));
        list.add(new Tuple3<>("20.0", index, (Vector) new DenseVector(until(20, 30))));

        JavaTimeSeriesRDD<String> rdd = JavaTimeSeriesRDDFactory.timeSeriesRDD(
                index, sc.parallelize(list));

        Path tempDir = null;

        try {
            tempDir = Files.createTempDirectory("saveload");
            Files.deleteIfExists(tempDir);
            String path = tempDir.toAbsolutePath().toString();
            rdd.saveAsCsv(path);
            JavaTimeSeriesRDD<String> loaded = JavaTimeSeriesRDDFactory
                    .timeSeriesRDDFromCsv(path, sc);
            assertEquals(rdd.index(), loaded.index());
        } finally {
            if (tempDir != null) {
                File[] files = tempDir.toFile().listFiles();
                for (File tempFile: files) {
                    tempFile.delete();
                }
                Files.deleteIfExists(tempDir);
            }

            sc.close();
        }
    }

    @Test
    public void testToIndexedRowMatrix() {
        try (JavaSparkContext sc = init()) {
            ZonedDateTime start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.systemDefault());
            String[] labels = {"a", "b", "c", "d", "e"};
            double[] seeds = untilBy(0, 20, 4);
            List<Tuple2<String, Vector>> list = new ArrayList<>();
            for (int i = 0; i < seeds.length; i++) {
              double seed = seeds[i];
              list.add(new Tuple2<>(labels[i],
                                    (Vector) new DenseVector(until((int) seed, (int) seed + 4))));
            }
            UniformDateTimeIndex index =
                DateTimeIndexFactory.uniform(start, 4, new DayFrequency(1));

            JavaPairRDD<String, Vector> rdd = sc.parallelizePairs(list, 3);
            JavaTimeSeriesRDD<String> tsRdd = JavaTimeSeriesRDDFactory.timeSeriesRDD(
                index, rdd);

            IndexedRowMatrix indexedMatrix = tsRdd.toIndexedRowMatrix();
            JavaPairRDD<Long, double[]> indicesDataRDD =
                new JavaRDD<>(indexedMatrix.rows(), classTagOf(IndexedRow.class))
                    .mapToPair((PairFunction<IndexedRow, Long, double[]>) ir ->
                        new Tuple2<>(ir.index(), ir.vector().toArray())
                    );
            List<double[]> rowData = indicesDataRDD.values().collect();
            Long[] rowIndices = indicesDataRDD.keys().collect().toArray(new Long[0]);

            assertArrayEquals(Arrays.asList(untilBy(0, 20, 4),
                                            untilBy(1, 20, 4),
                                            untilBy(2, 20, 4),
                                            untilBy(3, 20, 4)).toArray(),
                              rowData.toArray());

            assertArrayEquals(new Long[]{0L, 1L, 2L, 3L}, rowIndices);
        }
    }

    @Test
    public void testToRowMatrix() {
        try (JavaSparkContext sc = init()) {
            ZonedDateTime start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.systemDefault());
            String[] labels = {"a", "b", "c", "d", "e"};
            double[] seeds = untilBy(0, 20, 4);
            List<Tuple2<String, Vector>> list = new ArrayList<>();
            for (int i = 0; i < seeds.length; i++) {
              double seed = seeds[i];
              list.add(new Tuple2<>(labels[i],
                                    (Vector) new DenseVector(until((int) seed, (int) seed + 4))));
            }
            UniformDateTimeIndex index =
                DateTimeIndexFactory.uniform(start, 4, new DayFrequency(1));

            JavaPairRDD<String, Vector> rdd = sc.parallelizePairs(list, 3);
            JavaTimeSeriesRDD<String> tsRdd = JavaTimeSeriesRDDFactory.timeSeriesRDD(
                index, rdd);

            RowMatrix matrix = tsRdd.toRowMatrix();
            List<double[]> rowData = new JavaRDD<>(matrix.rows(), classTagOf(Vector.class))
                .map((Function<Vector, double[]>) Vector::toArray).collect();

            assertArrayEquals(Arrays.asList(untilBy(0, 20, 4),
                                            untilBy(1, 20, 4),
                                            untilBy(2, 20, 4),
                                            untilBy(3, 20, 4)).toArray(),
                              rowData.toArray());
        }
    }

    @Test
    public void testTimeSeriesRDDFromObservationsDataFrame() {
        JavaSparkContext sc = init();

        SQLContext sqlContext = new SQLContext(sc);

        ZonedDateTime start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"));
        String[] labels = { "a", "b", "c", "d", "e" };
        double[] seeds = untilBy(0, 20, 4);
        List<Tuple2<String, Vector>> list = new ArrayList<>();
        for(int i = 0; i < seeds.length; i++) {
            double seed = seeds[i];
            list.add(new Tuple2<>(labels[i],
                    (Vector) new DenseVector(until((int) seed, (int) seed + 4))));
        }
        UniformDateTimeIndex index = DateTimeIndexFactory.uniform(start, 4, new DayFrequency(1));

        JavaPairRDD<String, Vector> rdd = sc.parallelizePairs(list, 3);
        JavaTimeSeriesRDD<String> tsRdd = JavaTimeSeriesRDDFactory.timeSeriesRDD(
                index, rdd);

        Dataset<Row> obsDF = tsRdd.toObservationsDataFrame(sqlContext, "timestamp", "key", "value");
        JavaTimeSeriesRDD<String> tsRddFromDF = JavaTimeSeriesRDDFactory
                .timeSeriesRDDFromObservations(
                    index, obsDF, "timestamp", "key", "value");


        assertArrayEquals(
                tsRdd.sortByKey().collect().toArray(),
                tsRddFromDF.sortByKey().collect().toArray()
        );

        Row[] df1 = (Row[])obsDF.collect();
        Row[] df2 = (Row[])tsRddFromDF.toObservationsDataFrame(
                sqlContext, "timestamp", "key", "value").collect();

        Comparator<Row> comparator = (r1, r2) -> {
            int c = r1.<Double>getAs(2).compareTo(r2.<Double>getAs(2));
            if (c == 0) {
                c = r1.<String>getAs(1).compareTo(r2.<String>getAs(1));
                if (c == 0) {
                    c = r1.<Timestamp>getAs(0).compareTo(r2.<Timestamp>getAs(0));
                }
            }
            return c;
        };

        Arrays.sort(df1, comparator);
        Arrays.sort(df2, comparator);

        assertEquals(df1.length, df2.length);
        assertArrayEquals(df1, df2);

        sc.close();
    }

    @Test
    public void testRemoveInstantsWithNaNs() {
        try (JavaSparkContext sc = init()) {
            ZonedDateTime start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.systemDefault());
            UniformDateTimeIndex index =
                DateTimeIndexFactory.uniform(start, 4, new DayFrequency(1));
            List<Tuple3<String, UniformDateTimeIndex, Vector>> list = new ArrayList<>();
            list.add(new Tuple3<>(
                "1.0", index, (Vector) new DenseVector(until(1, 5))));
            list.add(new Tuple3<>(
                "5.0", index, (Vector) new DenseVector(new double[]{5.0, Double.NaN, 7.0, 8.0})));
            list.add(new Tuple3<>(
                "9.0", index, (Vector) new DenseVector(new double[]{9.0, 10.0, 11.0, Double.NaN})));

            JavaTimeSeriesRDD<String> rdd = JavaTimeSeriesRDDFactory.timeSeriesRDD(
                index, sc.parallelize(list));

            JavaTimeSeriesRDD<String> rdd2 = rdd.removeInstantsWithNaNs();

            assertEquals(DateTimeIndexFactory.irregular(new ZonedDateTime[]{
                             ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.systemDefault()),
                             ZonedDateTime.of(2015, 4, 11, 0, 0, 0, 0, ZoneId.systemDefault())
                         }),
                         rdd2.index());

            assertArrayEquals(new Vector[]{new DenseVector(new double[]{1.0, 3.0}),
                                  new DenseVector(new double[]{5.0, 7.0}),
                                  new DenseVector(new double[]{9.0, 11.0})},
                              rdd2.values().collect().toArray());
        }
    }
}
