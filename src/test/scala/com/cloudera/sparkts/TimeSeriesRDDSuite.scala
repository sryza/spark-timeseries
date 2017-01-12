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

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.time._

import org.apache.spark.mllib.linalg.{DenseVector, Vector}

import scala.Double.NaN
import com.cloudera.sparkts.DateTimeIndex._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.scalatest.{FunSuite, ShouldMatchers}

import scala.collection.Map

class TimeSeriesRDDSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  test("slice") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val vecs = Array(0 until 10, 10 until 20, 20 until 30)
      .map(_.map(x => x.toDouble).toArray)
      .map(new DenseVector(_))
      .map(x => (x(0).toString, x))
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    val index = uniform(start, 10, new DayFrequency(1))
    val rdd = new TimeSeriesRDD[String](index, sc.parallelize(vecs))
    val slice = rdd.slice(start.plusDays(1), start.plusDays(6))
    slice.index should be (uniform(start.plusDays(1), 6, new DayFrequency(1)))
    val contents = slice.collectAsMap()
    contents.size should be (3)
    contents("0.0") should be (new DenseVector((1 until 7).map(_.toDouble).toArray))
    contents("10.0") should be (new DenseVector((11 until 17).map(_.toDouble).toArray))
    contents("20.0") should be (new DenseVector((21 until 27).map(_.toDouble).toArray))
  }

  test("filterEndingAfter") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val vecs = Array(0 until 10, 10 until 20, 20 until 30)
      .map(_.map(x => x.toDouble).toArray)
      .map(new DenseVector(_))
      .map(x => (x(0).toString, x))
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    val index = uniform(start, 10, 1.businessDays)
    val rdd = new TimeSeriesRDD[String](index, sc.parallelize(vecs))
    rdd.filterEndingAfter(start).count() should be (3)
  }

  test("toInstants") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val seriesVecs = (0 until 20 by 4).map(
      x => new DenseVector((x until x + 4).map(_.toDouble).toArray))
    val labels = Array("a", "b", "c", "d", "e")
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.systemDefault())
    val index = uniform(start, 4, new DayFrequency(1))
    val rdd = sc.parallelize(labels.zip(seriesVecs.map(_.asInstanceOf[Vector])), 3)
    val tsRdd = new TimeSeriesRDD[String](index, rdd)
    val samples = tsRdd.toInstants().collect()
    samples should be (Array(
      (start, new DenseVector((0.0 until 20.0 by 4.0).toArray)),
      (start.plusDays(1), new DenseVector((1.0 until 20.0 by 4.0).toArray)),
      (start.plusDays(2), new DenseVector((2.0 until 20.0 by 4.0).toArray)),
      (start.plusDays(3), new DenseVector((3.0 until 20.0 by 4.0).toArray)))
    )
  }

  test("toInstantsDataFrame") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val seriesVecs = (0 until 20 by 4).map(
      x => new DenseVector((x until x + 4).map(_.toDouble).toArray))
    val labels = Array("a", "b", "c", "d", "e")
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    val index = uniform(start, 4, new DayFrequency(1))

    val rdd = sc.parallelize(labels.zip(seriesVecs.map(_.asInstanceOf[Vector])), 3)
    val tsRdd = new TimeSeriesRDD[String](index, rdd)

    val samplesDF = tsRdd.toInstantsDataFrame(sqlContext)
    val sampleRows = samplesDF.collect()
    val columnNames = samplesDF.columns

    columnNames.length should be (labels.length + 1) // labels + timestamp
    columnNames.head should be ("instant")
    columnNames.tail should be (labels)

    sampleRows should be (Array(
      Row.fromSeq(Timestamp.from(start.toInstant) :: (0.0 until 20.0 by 4.0).toList),
      Row.fromSeq(Timestamp.from(start.plusDays(1).toInstant) :: (1.0 until 20.0 by 4.0).toList),
      Row.fromSeq(Timestamp.from(start.plusDays(2).toInstant) :: (2.0 until 20.0 by 4.0).toList),
      Row.fromSeq(Timestamp.from(start.plusDays(3).toInstant) :: (3.0 until 20.0 by 4.0).toList)
    ))
  }

  test("save / load") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val vecs = Array(0 until 10, 10 until 20, 20 until 30)
      .map(_.map(x => x.toDouble).toArray)
      .map(new DenseVector(_))
      .map(x => (x(0).toString, x))
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    val index = uniform(start, 10, 1.businessDays)
    val rdd = new TimeSeriesRDD[String](index, sc.parallelize(vecs))

    val tempDir = Files.createTempDirectory("saveload")
    val path = tempDir.toFile.getAbsolutePath
    new File(path).delete()
    try {
      rdd.saveAsCsv(path)
      val loaded = TimeSeriesRDD.timeSeriesRDDFromCsv(path, sc)
      loaded.index should be (rdd.index)
    } finally {
      new File(path).listFiles().foreach(_.delete())
      new File(path).delete()
    }
  }

  test("toIndexedRowMatrix") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val seriesVecs = (0 until 20 by 4).map(
      x => new DenseVector((x until x + 4).map(_.toDouble).toArray))
    val labels = Array("a", "b", "c", "d", "e")
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    val index = uniform(start, 4, 1.businessDays)
    val rdd = sc.parallelize(labels.zip(seriesVecs.map(_.asInstanceOf[Vector])), 3)
    val tsRdd = new TimeSeriesRDD[String](index, rdd)
    val indexedMatrix = tsRdd.toIndexedRowMatrix()
    val (rowIndices, rowData) = indexedMatrix.rows.collect().map { case IndexedRow(ix, data) =>
      (ix, data.toArray)
    }.unzip
    rowData.toArray should be ((0.0 to 3.0 by 1.0).map(x => (x until 20.0 by 4.0).toArray).toArray)
    rowIndices.toArray should be (Array(0, 1, 2, 3))
  }

  test("toRowMatrix") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val seriesVecs = (0 until 20 by 4).map(
      x => new DenseVector((x until x + 4).map(_.toDouble).toArray))
    val labels = Array("a", "b", "c", "d", "e")
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    val index = uniform(start, 4, 1.businessDays)
    val rdd = sc.parallelize(labels.zip(seriesVecs.map(_.asInstanceOf[Vector])), 3)
    val tsRdd = new TimeSeriesRDD[String](index, rdd)
    val matrix = tsRdd.toRowMatrix()
    val rowData = matrix.rows.collect().map(_.toArray)
    rowData.toArray should be ((0.0 to 3.0 by 1.0).map(x => (x until 20.0 by 4.0).toArray).toArray)
  }

  test("timeSeriesRDDFromObservations DataFrame") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val seriesVecs = (0 until 20 by 4).map(
      x => new DenseVector((x until x + 4).map(_.toDouble).toArray))
    val labels = Array("a", "b", "c", "d", "e")
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    val index = uniform(start, 4, new DayFrequency(1))
    val rdd = sc.parallelize(labels.zip(seriesVecs.map(_.asInstanceOf[Vector])), 3)
    val tsRdd = new TimeSeriesRDD[String](index, rdd)

    val obsDF = tsRdd.toObservationsDataFrame(sqlContext)
    val tsRddFromDF = TimeSeriesRDD.timeSeriesRDDFromObservations(
      index, obsDF, "timestamp", "key", "value")

    val ts1 = tsRdd.collect().sortBy(_._1)
    val ts2 = tsRddFromDF.collect().sortBy(_._1)
    ts1 should be (ts2)

    val df1 = obsDF.collect()
    val df2 = tsRddFromDF.toObservationsDataFrame(sqlContext).collect()
    df1.length should be (df2.length)
    df1.toSet should be (df2.toSet)
  }

  test("removeInstantsWithNaNs") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)

    val vecs = Array(
      Array(1.0, 2.0, 3.0, 4.0), Array(5.0, NaN, 7.0, 8.0), Array(9.0, 10.0, 11.0, NaN))
      .map(new DenseVector(_))
      .map(x => (x(0).toString, x))
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    val index = uniform(start, 4, new DayFrequency(1))
    val rdd = new TimeSeriesRDD[String](index, sc.parallelize(vecs))
    val rdd2 = rdd.removeInstantsWithNaNs()
    rdd2.index should be (irregular(Array(ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z")),
      ZonedDateTime.of(2015, 4, 11, 0, 0, 0, 0, ZoneId.of("Z")))))
    rdd2.map(_._2).collect() should be (Array(
      new DenseVector(Array(1.0, 3.0)),
      new DenseVector(Array(5.0, 7.0)),
      new DenseVector(Array(9.0, 11.0))
    ))
  }

  test("lagWithIncludeOriginalsLaggedStringKey") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val vecs = Array(0 until 10, 10 until 20, 20 until 30)
      .map(_.map(x => x.toDouble).toArray)
      .map(new DenseVector(_))
      .map(x => (x(0).toString, x))
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))

    val index = uniform(start, 10, new DayFrequency(1))
    val rdd = new TimeSeriesRDD[String](index, sc.parallelize(vecs))
    val lagTsrdd = rdd.lags(2, true, TimeSeries.laggedStringKey)

    val cts = lagTsrdd.collectAsTimeSeries()

    cts.keys.toSet should be (Array("0.0", "10.0", "20.0", "lag1(0.0)", "lag1(10.0)", "lag1(20.0)",
      "lag2(0.0)", "lag2(10.0)", "lag2(20.0)").toSet)
    lagTsrdd.index should be (uniform(start.plusDays(2), 8, new DayFrequency(1)))

    lagTsrdd.keys.length should be (9)
    val lagTsMap: Map[String, Vector] = lagTsrdd.collectAsMap()

    lagTsMap.getOrElse("0.0", null) should be (new DenseVector((2 to 9).toArray.map(_.toDouble)))
    lagTsMap.getOrElse("10.0", null) should be (new DenseVector((12 to 19).toArray.map(_.toDouble)))
    lagTsMap.getOrElse("20.0", null) should be (new DenseVector((22 to 29).toArray.map(_.toDouble)))

    lagTsMap.getOrElse("lag1(0.0)", null) should be (
      new DenseVector((1 to 8).toArray.map(_.toDouble)))
    lagTsMap.getOrElse("lag1(10.0)", null) should be (
      new DenseVector((11 to 18).toArray.map(_.toDouble)))
    lagTsMap.getOrElse("lag1(20.0)", null) should be (
      new DenseVector((21 to 28).toArray.map(_.toDouble)))

    lagTsMap.getOrElse("lag2(0.0)", null) should be (
      new DenseVector((0 to 7).toArray.map(_.toDouble)))
    lagTsMap.getOrElse("lag2(10.0)", null) should be (
      new DenseVector((10 to 17).toArray.map(_.toDouble)))
    lagTsMap.getOrElse("lag2(20.0)", null) should be (
      new DenseVector((20 to 27).toArray.map(_.toDouble)))
  }

  test("lagWithExcludeOriginalsLaggedStringKey") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val vecs = Array(0 until 10, 10 until 20, 20 until 30)
    .map(_.map(x => x.toDouble).toArray)
    .map(new DenseVector(_))
    .map(x => (x(0).toString, x))
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))

    val index = uniform(start, 10, new DayFrequency(1))
    val rdd = new TimeSeriesRDD[String](index, sc.parallelize(vecs))
    val lagTsrdd = rdd.lags(2, false, TimeSeries.laggedStringKey)

    val cts = lagTsrdd.collectAsTimeSeries()

    lagTsrdd.keys.length should be(6)
    cts.keys.toSet should be(Array("lag1(0.0)", "lag1(10.0)", "lag1(20.0)", "lag2(0.0)",
      "lag2(10.0)", "lag2(20.0)").toSet)
    lagTsrdd.index should be(uniform(start.plusDays(2), 8, new DayFrequency(1)))

    val lagTsMap: Map[String, Vector] = lagTsrdd.collectAsMap()

    lagTsMap.getOrElse("lag1(0.0)", null) should be(
      new DenseVector((1 to 8).toArray.map(_.toDouble)))
    lagTsMap.getOrElse("lag1(10.0)", null) should be(
      new DenseVector((11 to 18).toArray.map(_.toDouble)))
    lagTsMap.getOrElse("lag1(20.0)", null) should be(
      new DenseVector((21 to 28).toArray.map(_.toDouble)))

    lagTsMap.getOrElse("lag2(0.0)", null) should be(
      new DenseVector((0 to 7).toArray.map(_.toDouble)))
    lagTsMap.getOrElse("lag2(10.0)", null) should be(
      new DenseVector((10 to 17).toArray.map(_.toDouble)))
    lagTsMap.getOrElse("lag2(20.0)", null) should be(
      new DenseVector((20 to 27).toArray.map(_.toDouble)))
  }

  test("rollmean") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val vecs = Array(0 until 10, 10 until 20, 20 until 30)
      .map(_.map(x => x.toDouble).toArray)
      .map(new DenseVector(_))
      .map(x => (x(0).toString, x))
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    val index = uniform(start, 10, 1.businessDays)
    val rdd = new TimeSeriesRDD[String](index, sc.parallelize(vecs))
    val rdd_right = rdd.rollMean(5, "Right")
    val rdd_center = rdd.rollMean(5, "Center")
    val rdd_left = rdd.rollMean(5, "Left")

    // Check index
    val start_right = ZonedDateTime.of(2015, 4, 15, 0, 0, 0, 0, ZoneId.of("Z"))
    rdd_right.index should be (uniform(start_right, 6, 1.businessDays))
    val start_center = ZonedDateTime.of(2015, 4, 13, 0, 0, 0, 0, ZoneId.of("Z"))
    rdd_center.index should be (uniform(start_center, 6, 1.businessDays))
    val start_left = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    rdd_left.index should be (uniform(start_left, 6, 1.businessDays))

    // Check actual rolling mean
    val contents = rdd_left.collectAsMap()
    contents.size should be (3)
    contents("0.0") should be (new DenseVector((2 to 7).map(_.toDouble).toArray))
    contents("10.0") should be (new DenseVector((12 to 17).map(_.toDouble).toArray))
    contents("20.0") should be (new DenseVector((22 to 27).map(_.toDouble).toArray))
  }

  test("filterByInstant") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val vecs = Array(0 until 10, 10 until 20, 20 until 30)
      .map(_.map(x => x.toDouble).toArray)
      .map(new DenseVector(_))
      .map(x => (x(0).toString, x))
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    val index = uniform(start, 10, new DayFrequency(1))

    val rdd = new TimeSeriesRDD[String](index, sc.parallelize(vecs))
    val rddFiltered = rdd.filterByInstant(v => v % 2 == 1, Array("20.0"))

    val instants = rddFiltered.toInstants()
    instants.persist()

    instants.count() should be (5)

    val collected = instants.collect()
    collected(0)._2(0) should be (1.0)
    collected(0)._2(1) should be (11.0)
    collected(0)._2(2) should be (21.0)

    collected(1)._2(0) should be (3.0)
    collected(1)._2(1) should be (13.0)
    collected(1)._2(2) should be (23.0)

    collected(4)._2(0) should be (9.0)
    collected(4)._2(1) should be (19.0)
    collected(4)._2(2) should be (29.0)
  }

}
