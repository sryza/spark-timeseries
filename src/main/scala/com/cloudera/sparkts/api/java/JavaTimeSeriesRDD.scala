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

package com.cloudera.sparkts.api.java

import java.time.ZonedDateTime

import com.cloudera.sparkts._
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.api.java.{JavaSparkContext, JavaRDD, JavaPairRDD}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, IndexedRowMatrix}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.util.StatCounter

import scala.reflect.ClassTag

/**
 * A lazy distributed collection of univariate series with a conformed time dimension. Lazy in the
 * sense that it is an RDD: it encapsulates all the information needed to generate its elements,
 * but doesn't materialize them upon instantiation. Distributed in the sense that different
 * univariate series within the collection can be stored and processed on different nodes. Within
 * each univariate series, observations are not distributed. The time dimension is conformed in the
 * sense that a single DateTimeIndex applies to all the univariate series. Each univariate series
 * within the RDD has a String key to identify it.
 *
 */
class JavaTimeSeriesRDD[K](tsrdd: TimeSeriesRDD[K])(implicit override val kClassTag: ClassTag[K])
  extends JavaPairRDD[K, Vector](tsrdd) {

  def index: DateTimeIndex = tsrdd.index

  /**
   * Collects the RDD as a local JavaTimeSeries
   */
  def collectAsTimeSeries(): JavaTimeSeries[K] = new JavaTimeSeries[K](tsrdd.collectAsTimeSeries)

  /**
   * Finds a series in the JavaTimeSeriesRDD with the given key.
   */
  def findSeries(key: K): Vector = tsrdd.findSeries(key)

  /**
   * Returns a JavaTimeSeriesRDD where each time series is differenced with the given order. The new
   * RDD will be missing the first n date-times.
   */
  def differences(n: Int): JavaTimeSeriesRDD[K] = new JavaTimeSeriesRDD[K](tsrdd.differences(n))

  /**
   * Returns a JavaTimeSeriesRDD where each time series is quotiented with the given order. The new
   * RDD will be missing the first n date-times.
   */
  def quotients(n: Int): JavaTimeSeriesRDD[K] = new JavaTimeSeriesRDD[K](tsrdd.quotients(n))

  /**
   * Returns a return rate series for each time series. Assumes periodic (as opposed to continuously
   * compounded) returns.
   */
  def returnRates(): JavaTimeSeriesRDD[K] = new JavaTimeSeriesRDD[K](tsrdd.returnRates)

  override def filter(f: JFunction[(K, Vector), java.lang.Boolean]): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](tsrdd.filter((t) => f.call(t)))

  /**
   * Keep only time series whose first observation is before or equal to the given start date.
   */
  def filterStartingBefore(dt: ZonedDateTime): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](tsrdd.filterStartingBefore(dt))

  /**
   * Keep only time series whose last observation is after or equal to the given end date.
   */
  def filterEndingAfter(dt: ZonedDateTime): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](tsrdd.filterEndingAfter(dt))

  /**
   * Return a JavaTimeSeriesRDD with all instants removed that have a NaN in one of the series.
   */
  def removeInstantsWithNaNs(): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](tsrdd.removeInstantsWithNaNs)

  /**
   * Returns a JavaTimeSeriesRDD that's a sub-slice of the given series.
   * @param start The start date the for slice.
   * @param end The end date for the slice (inclusive).
   */
  def slice(start: ZonedDateTime, end: ZonedDateTime): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](tsrdd.slice(start, end))

  /**
   * Returns a JavaTimeSeriesRDD that's a sub-slice of the given series.
   * @param start The start date the for slice.
   * @param end The end date for the slice (inclusive).
   */
  def slice(start: Long, end: Long): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](tsrdd.slice(start, end))

  /**
   * Fills in missing data (NaNs) in each series according to a given imputation method.
   *
   * @param method "linear", "nearest", "next", or "previous"
   * @return A JavaTimeSeriesRDD with missing observations filled in.
   */
  def fill(method: String): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](tsrdd.fill(method))

  /**
   * Applies a transformation to each time series that preserves the time index of this
   * JavaTimeSeriesRDD.
   */
  def mapSeries(f: JFunction[Vector, Vector]): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](tsrdd.mapSeries((v) => f.call(v)))

  /**
   * Applies a transformation to each time series and returns a JavaTimeSeriesRDD with
   * the given index. The caller is expected to ensure that the time series produced line
   * up with the given index.
   */
  def mapSeries(f: JFunction[Vector, Vector], index: DateTimeIndex): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](tsrdd.mapSeries((v) => f.call(v), index))

  /**
   * Gets stats like min, max, mean, and standard deviation for each time series.
   */
  def seriesStats(): JavaRDD[StatCounter] =
    new JavaRDD[StatCounter](tsrdd.seriesStats)

  /**
   * Essentially transposes the time series matrix to create a JavaPairRDD where each
   * record contains a single instant in time as the key and all the values that correspond to
   * it as the value. Involves a shuffle operation.
   *
   * In the returned JavaPairRDD, the ordering of values within each record corresponds
   * to the ordering of the time series records in the original RDD. The records are ordered
   * by time.
   */
  def toInstants(nPartitions: Int): JavaPairRDD[ZonedDateTime, Vector] =
    new JavaPairRDD[ZonedDateTime, Vector](tsrdd.toInstants(nPartitions))

  /**
   * Equivalent to toInstants(-1)
   */
  def toInstants(): JavaPairRDD[ZonedDateTime, Vector] = toInstants(-1)

  /**
   * Performs the same operations as toInstants but returns a DataFrame instead.
   *
   * The schema of the DataFrame returned will be a java.sql.Timestamp column named "instant"
   * and Double columns named identically to their keys in the JavaTimeSeriesRDD
   */
  def toInstantsDataFrame(sqlContext: SQLContext, nPartitions: Int): DataFrame =
    tsrdd.toInstantsDataFrame(sqlContext, nPartitions)

  /**
   * Equivalent to toInstantsDataFrame(sqlContext, -1)
   */
  def toInstantsDataFrame(sqlContext: SQLContext): DataFrame = toInstantsDataFrame(sqlContext, -1)

  /**
   * Returns a DataFrame where each row is an observation containing a timestamp, a key, and a
   * value.
   */
  def toObservationsDataFrame(
     sqlContext: SQLContext,
     tsCol: String = "timestamp",
     keyCol: String = "key",
     valueCol: String = "value"): DataFrame =
    tsrdd.toObservationsDataFrame(sqlContext, tsCol, keyCol, valueCol)

  /**
   * Converts a JavaTimeSeriesRDD into a distributed IndexedRowMatrix, useful to take advantage
   * of Spark MLlib's statistic functions on matrices in a distributed fashion. This is only
   * supported for cases with a uniform time series index. See
   * [[http://spark.apache.org/docs/latest/mllib-data-types.html]] for more information on the
   * matrix data structure
   * @param nPartitions number of partitions, default to -1, which represents the same number
   *                    as currently used for the TimeSeriesRDD
   * @return an equivalent IndexedRowMatrix
   */
  def toIndexedRowMatrix(nPartitions: Int): IndexedRowMatrix =
    tsrdd.toIndexedRowMatrix(nPartitions)

  /**
   * Equivalent to toIndexedRowMatrix(-1)
   */
  def toIndexedRowMatrix(): IndexedRowMatrix = toIndexedRowMatrix(-1)

  /**
   * Converts a JavaTimeSeriesRDD into a distributed RowMatrix, note that indices in
   * a RowMatrix are not significant, and thus this is a valid operation regardless
   * of the type of time index.  See
   * [[http://spark.apache.org/docs/latest/mllib-data-types.html]] for more information on the
   * matrix data structure
   * @return an equivalent RowMatrix
   */
  def toRowMatrix(nPartitions: Int): RowMatrix =
    tsrdd.toRowMatrix(nPartitions)

  /**
   * Equivalent to toRowMatrix(-1)
   */
  def toRowMatrix(): RowMatrix = toRowMatrix(-1)

  def compute(split: Partition, context: TaskContext): java.util.Iterator[(K, Vector)] =
    iterator(split, context)

  /**
   * Writes out the contents of this JavaTimeSeriesRDD to a set of CSV files in the given directory,
   * with an accompanying file in the same directory including the time index.
   */
  def saveAsCsv(path: String): Unit = tsrdd.saveAsCsv(path)

  /**
   * Returns a JavaTimeSeriesRDD rebased on top of a new index.  Any timestamps that exist
   * in the new index but not in the existing index will be filled in with NaNs.
   *
   * @param newIndex The DateTimeIndex for the new RDD
   */
  def withIndex(newIndex: DateTimeIndex): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](tsrdd.withIndex(newIndex))
}

object JavaTimeSeriesRDD {
  /**
   * Instantiates a JavaTimeSeriesRDD.
   *
   * @param index DateTimeIndex
   * @param seriesRDD JavaPairRDD of time series
   */
  def javaTimeSeriesRDD[K](
    index: DateTimeIndex,
    seriesRDD: JavaPairRDD[K, Vector])
    (implicit kClassTag: ClassTag[K]): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](new TimeSeriesRDD[K](index, seriesRDD.rdd))

  /**
   * Instantiates a JavaTimeSeriesRDD.
   *
   * @param targetIndex DateTimeIndex to conform all the indices to.
   * @param seriesRDD JavaRDD of time series, each with their own DateTimeIndex.
   */
  def javaTimeSeriesRDD[K](
    targetIndex: UniformDateTimeIndex,
    seriesRDD: JavaRDD[(K, UniformDateTimeIndex, Vector)])
    (implicit kClassTag: ClassTag[K]): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](TimeSeriesRDD.timeSeriesRDD(targetIndex, seriesRDD.rdd))

  /**
   * Instantiates a JavaTimeSeriesRDD from an RDD of TimeSeries.
   *
   * @param targetIndex DateTimeIndex to conform all the indices to.
   * @param seriesRDD RDD of time series, each with their own DateTimeIndex.
   */
  def javaTimeSeriesRDD[K](targetIndex: DateTimeIndex, seriesRDD: JavaRDD[JavaTimeSeries[K]])
    (implicit kClassTag: ClassTag[K]): JavaTimeSeriesRDD[K] =
    new JavaTimeSeriesRDD[K](
      TimeSeriesRDD.timeSeriesRDD(targetIndex, seriesRDD.rdd.map(jts => jts.ts)))

  /**
   * Instantiates a JavaTimeSeriesRDD from a DataFrame of observations.
   *
   * @param targetIndex DateTimeIndex to conform all the series to.
   * @param df The DataFrame.
   * @param tsCol The Timestamp column telling when the observation occurred.
   * @param keyCol The string column labeling which string key the observation belongs to..
   * @param valueCol The observed value..
   */
  def javaTimeSeriesRDDFromObservations(
     targetIndex: DateTimeIndex,
     df: Dataset[Row],
     tsCol: String,
     keyCol: String,
     valueCol: String): JavaTimeSeriesRDD[String] =
    new JavaTimeSeriesRDD[String](TimeSeriesRDD.timeSeriesRDDFromObservations(
      targetIndex, df, tsCol, keyCol, valueCol))

  /**
   * Loads a JavaTimeSeriesRDD from a directory containing a set of CSV files and a date-time index.
   */
  def javaTimeSeriesRDDFromCsv(path: String, sc: JavaSparkContext): JavaTimeSeriesRDD[String] =
    new JavaTimeSeriesRDD[String](TimeSeriesRDD.timeSeriesRDDFromCsv(path, sc.sc))

  /**
   * Creates a JavaTimeSeriesRDD from rows in a binary format that Python can write to.
   * Not a public API. For use only by the Python API.
   */
  def javaTimeSeriesRDDFromPython(index: DateTimeIndex, pyRdd: JavaRDD[Array[Byte]])
    : JavaTimeSeriesRDD[String] =
    new JavaTimeSeriesRDD[String](TimeSeriesRDD.timeSeriesRDDFromPython(index, pyRdd.rdd))
}
