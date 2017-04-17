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

import java.time._

import breeze.linalg.{DenseMatrix => BDM}
import java.time.format._
import org.apache.spark.mllib.linalg.{DenseMatrix, Vectors}
import MatrixUtil._

import java.time._
import com.cloudera.sparkts.DateTimeIndex._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, ShouldMatchers}

class TransformsSuite extends FunSuite with ShouldMatchers {
  test("timeDerivative") {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val index = irregular(Array(
      "2015-04-14 00:00:00",
      "2015-04-15 00:00:00",
      "2015-04-17 00:00:00",
      "2015-04-22 00:00:00",
      "2015-04-26 00:00:00"
    ).map(text => LocalDateTime.parse(text, formatter).atZone(ZoneId.of("Z"))))

    val data = BDM((1.0, 6.0), (2.0, 7.0), (3.0, 9.0), (8.0, 9.0), (5.0, 10.0))

    val originalTimeSeries = new TimeSeries(index, data, Array("a", "b"))

    val differencedTimeSeries = Transforms.timeDerivative(originalTimeSeries, new DayFrequency(1))

    differencedTimeSeries.index.first.getYear() should be (2015)
    differencedTimeSeries.index.first.getMonthValue() should be (4)
    differencedTimeSeries.index.first.getDayOfMonth() should be (15)
    differencedTimeSeries.index.last.getYear() should be (2015)
    differencedTimeSeries.index.last.getMonthValue() should be (4)
    differencedTimeSeries.index.last.getDayOfMonth() should be (26)
    differencedTimeSeries.index.size should be (4)

    val expected: DenseMatrix = BDM(
      (1.0, 1.0),
      (0.5, 1.0),
      (1.0, -0.0),
      (-0.75, 0.25)
    )
    differencedTimeSeries.data should be (expected)
  }

  test("timeDerivative - RDD version") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    val sc = new SparkContext(conf)

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val index = irregular(Array(
      "2015-04-14 00:00:00",
      "2015-04-15 00:00:00",
      "2015-04-17 00:00:00",
      "2015-04-22 00:00:00",
      "2015-04-26 00:00:00"
    ).map(text => LocalDateTime.parse(text, formatter).atZone(ZoneId.of("Z"))))

    val vectors = Array(("a", Vectors.dense(Array(1.0, 2.0, 3.0, 8.0, 5.0))),
      ("b", Vectors.dense(Array(6.0, 7.0, 9.0, 9.0, 10.0))))

    val originalTimeSeries = new TimeSeriesRDD[String](index, sc.parallelize(vectors))

    val differencedTimeSeries = Transforms.timeDerivative(originalTimeSeries, new DayFrequency(1), sc)

    differencedTimeSeries.index.first.getYear() should be (2015)
    differencedTimeSeries.index.first.getMonthValue() should be (4)
    differencedTimeSeries.index.first.getDayOfMonth() should be (15)
    differencedTimeSeries.index.last.getYear() should be (2015)
    differencedTimeSeries.index.last.getMonthValue() should be (4)
    differencedTimeSeries.index.last.getDayOfMonth() should be (26)
    differencedTimeSeries.index.size should be (4)

    val expected: DenseMatrix = BDM(
      (1.0, 1.0),
      (0.5, 1.0),
      (1.0, -0.0),
      (-0.75, 0.25)
    )
    differencedTimeSeries.collectAsTimeSeries().data should be (expected)

    sc.stop()
  }

}
