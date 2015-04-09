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

import breeze.linalg._

import com.cloudera.sparkts.DateTimeIndex._

import com.github.nscala_time.time.Imports._

import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest.{FunSuite, ShouldMatchers}

class TimeSeriesRDDSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  test("slice") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    sc = new SparkContext(conf)
    val vecs = Array(0 until 10, 10 until 20, 20 until 30)
      .map(_.map(x => x.toDouble).toArray)
      .map(new DenseVector(_))
      .map(x => (x(0).toString, x))
    val start = new DateTime("2015-4-9")
    val index = uniform(start, 10, 1.days)
    val rdd = new TimeSeriesRDD[String](index, sc.parallelize(vecs))
    val slice = rdd.slice(start + 1.days, start + 6.days)
    slice.index should be (uniform(start + 1.days, 6, 1.days))
    val contents = slice.collectAsMap()
    contents.size should be (3)
    contents("0.0") should be (new DenseVector((1 until 7).map(_.toDouble).toArray))
    contents("10.0") should be (new DenseVector((11 until 17).map(_.toDouble).toArray))
    contents("20.0") should be (new DenseVector((21 until 27).map(_.toDouble).toArray))
  }
}
