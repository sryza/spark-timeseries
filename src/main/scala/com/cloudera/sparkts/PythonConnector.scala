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

import java.nio.ByteBuffer

import breeze.linalg.{DenseVector, Vector}

import org.apache.spark.api.java.function.Function

import java.time._

/**
 * This file contains utilities used by the spark-timeseries Python bindings to communicate with
 * the JVM.  BytesToKeyAndSeries and KeyAndSeriesToBytes write and read bytes in the format
 * read and written by the Python TimeSeriesSerializer class.
 */

private object PythonConnector {
  val INT_SIZE = 4
  val DOUBLE_SIZE = 8
  val LONG_SIZE = 8

  def putVector(buf: ByteBuffer, vec: Vector[Double]): Unit = {
    buf.putInt(vec.length)
    var i = 0
    while (i < vec.length) {
      buf.putDouble(vec(i))
      i += 1
    }
  }
}

private class BytesToKeyAndSeries extends Function[Array[Byte], (String, Vector[Double])] {
  override def call(arr: Array[Byte]): (String, Vector[Double]) = {
    val buf = ByteBuffer.wrap(arr)
    val keySize = buf.getInt()
    val keyBytes = new Array[Byte](keySize)
    buf.get(keyBytes)

    val seriesSize = buf.getInt()
    val series = new Array[Double](seriesSize)
    var i = 0
    while (i < seriesSize) {
      series(i) = buf.getDouble()
      i += 1
    }
    (new String(keyBytes, "UTF8"), new DenseVector[Double](series))
  }
}

private class KeyAndSeriesToBytes extends Function[(String, Vector[Double]), Array[Byte]] {
  import PythonConnector._

  override def call(keyVec: (String, Vector[Double])): Array[Byte] = {
    val keyBytes = keyVec._1.getBytes("UTF-8")
    val vec = keyVec._2
    val arr = new Array[Byte](INT_SIZE + keyBytes.length + INT_SIZE + DOUBLE_SIZE * vec.length)
    val buf = ByteBuffer.wrap(arr)
    buf.putInt(keyBytes.length)
    buf.put(keyBytes)
    putVector(buf, vec)
    arr
  }
}

private class InstantToBytes extends Function[(ZonedDateTime, Vector[Double]), Array[Byte]] {
  import PythonConnector._

  override def call(instant: (ZonedDateTime, Vector[Double])): Array[Byte] = {
    val arr = new Array[Byte](LONG_SIZE + INT_SIZE + DOUBLE_SIZE * instant._2.length)
    val buf = ByteBuffer.wrap(arr)
    buf.putLong(TimeSeriesUtils.ZonedDateTimeToLong(instant._1))
    putVector(buf, instant._2)
    arr
  }
}
