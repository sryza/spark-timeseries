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

import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time._
import java.util.Arrays

import breeze.linalg.{diff, DenseMatrix => BDM, DenseVector => BDV}
import com.cloudera.sparkts.MatrixUtil._
import com.cloudera.sparkts.TimeSeriesUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.util.StatCounter

import scala.collection.mutable.ArrayBuffer
import scala.math._
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
 * @param index The DateTimeIndex shared by all the time series.
 */
class TimeSeriesRDD[K](val index: DateTimeIndex, parent: RDD[(K, Vector)])
    (implicit val kClassTag: ClassTag[K])
  extends RDD[(K, Vector)](parent) {

  lazy val keys = parent.map(_._1).collect()

  /**
   * Collects the RDD as a local TimeSeries
   */
  def collectAsTimeSeries(): TimeSeries[K] = {
    val elements = collect()
    if (elements.isEmpty) {
      new TimeSeries(index, DenseMatrix.zeros(0, 0), new Array[K](0))
    } else {
      val mat = new BDM[Double](elements.head._2.size, elements.length)
      val labels = new Array[K](elements.length)
      for (i <- elements.indices) {
        val (label, vec) = elements(i)
        mat(::, i) := toBreeze(vec)
        labels(i) = label
      }
      new TimeSeries[K](index, mat, labels)
    }
  }

  /**
   * Lags each time series in the RDD
   *
   * @param maxLag maximum Lag
   * @param includeOriginals include original time series
   * @param laggedKey function to generate lagged keys
   * @tparam U type of keys
   * @return RDD of lagged time series
   */
  def lags[U: ClassTag](maxLag: Int, includeOriginals: Boolean, laggedKey: (K, Int) => U)
    : TimeSeriesRDD[U] = {
    val newDateTimeIndex = index.islice(maxLag, index.size)

    val laggedTimeSeriesRDD: RDD[(U, Vector)] = flatMap { t =>
      val tseries: TimeSeries[K] =
        new TimeSeries[K](index, new BDM[Double](t._2.length, 1, t._2.toArray), Array[K](t._1))
      val laggedTseries = tseries.lags(maxLag, includeOriginals, laggedKey)
      val laggedDataBreeze: BDM[Double] = laggedTseries.data
      laggedTseries.keys.indices.map { c =>
        (laggedTseries.keys(c), new DenseVector(laggedDataBreeze(::, c).toArray))
      }
    }
    new TimeSeriesRDD[U](newDateTimeIndex, laggedTimeSeriesRDD)
  }

  /**
   * Finds a series in the TimeSeriesRDD with the given key.
   */
  def findSeries(key: K): Vector = {
    filter(_._1 == key).first()._2
  }

  /**
   * Returns a TimeSeriesRDD where each time series is differenced with the given order. The new
   * RDD will be missing the first n date-times.
   */
  def differences(n: Int): TimeSeriesRDD[K] = {
    mapSeries(vec => diff(toBreeze(vec).toDenseVector, n), index.islice(n, index.size))
  }

  /**
   * Returns a TimeSeriesRDD where each time series is quotiented with the given order. The new
   * RDD will be missing the first n date-times.
   */
  def quotients(n: Int): TimeSeriesRDD[K] = {
    mapSeries(UnivariateTimeSeries.quotients(_, n), index.islice(n, index.size))
  }

  /**
   * Returns a return rate series for each time series. Assumes periodic (as opposed to continuously
   * compounded) returns.
   */
  def returnRates(): TimeSeriesRDD[K] = {
    mapSeries(vec => UnivariateTimeSeries.price2ret(vec, 1), index.islice(1, index.size))
  }

  override def filter(f: ((K, Vector)) => Boolean): TimeSeriesRDD[K] = {
    new TimeSeriesRDD[K](index, super.filter(f))
  }

  /**
   * Keep only time series whose first observation is before or equal to the given start date.
   */
  def filterStartingBefore(dt: ZonedDateTime): TimeSeriesRDD[K] = {
    val startLoc = index.locAtDateTime(dt)
    filter { case (key, ts) => UnivariateTimeSeries.firstNotNaN(ts) <= startLoc }
  }

  /**
   * Keep only time series whose last observation is after or equal to the given end date.
   */
  def filterEndingAfter(dt: ZonedDateTime): TimeSeriesRDD[K] = {
    val endLoc = index.locAtDateTime(dt)
    filter { case (key, ts) => UnivariateTimeSeries.lastNotNaN(ts) >= endLoc}
  }

  /**
    * This function applies the filterExpression to each element of each row (instant)
    * of the TimeSeriesRDD, keeping only the rows for which the condition
    * is true on the specified columns (in filterColumns).
    */
  def filterByInstant(filterExpression: (Double) => Boolean,
                      filterColumns: Array[K]): TimeSeriesRDD[K] = {

    val zero = new Array[Boolean](index.size)
    def merge(arr: Array[Boolean], rec: (K, Vector)): Array[Boolean] = {
      if (filterColumns.contains(rec._1)) {
        var i = 0
        while (i < arr.length) {
          if (filterExpression(rec._2(i)) == false)
            arr(i) |= true
          i += 1
        }
      }
      arr
    }
    def comb(arr1: Array[Boolean], arr2: Array[Boolean]): Array[Boolean] = {
      arr1.zip(arr2).map(x => x._1 || x._2)
    }
    val filteredOut = aggregate(zero)(merge, comb)

    val activeIndices = filteredOut.zipWithIndex.filter(!_._1).map(_._2)
    val newDates = activeIndices.map(index.dateTimeAtLoc)
    val newIndex = DateTimeIndex.irregular(newDates, index.zone)
    mapSeries(series => {
      new DenseVector(activeIndices.map(x => series(x)))
    }, newIndex)
  }

  /**
   * Return a TimeSeriesRDD with all instants removed that have a NaN in one of the series.
   */
  def removeInstantsWithNaNs(): TimeSeriesRDD[K] = {
    val zero = new Array[Boolean](index.size)
    def merge(arr: Array[Boolean], rec: (K, Vector)): Array[Boolean] = {
      var i = 0
      while (i < arr.length) {
        arr(i) |= rec._2(i).isNaN
        i += 1
      }
      arr
    }
    def comb(arr1: Array[Boolean], arr2: Array[Boolean]): Array[Boolean] = {
      arr1.zip(arr2).map(x => x._1 || x._2)
    }
    val nans = aggregate(zero)(merge, comb)

    val activeIndices = nans.zipWithIndex.filter(!_._1).map(_._2)
    val newDates = activeIndices.map(index.dateTimeAtLoc)
    val newIndex = DateTimeIndex.irregular(newDates, index.zone)
    mapSeries(series => {
      new DenseVector(activeIndices.map(x => series(x)))
    }, newIndex)
  }

  /**
   * Returns a TimeSeriesRDD that's a sub-slice of the given series.
 *
   * @param start The start date the for slice.
   * @param end The end date for the slice (inclusive).
   */
  def slice(start: ZonedDateTime, end: ZonedDateTime): TimeSeriesRDD[K] = {
    val targetIndex = index.slice(start, end)
    val rebaserFunction = rebaser(index, targetIndex, Double.NaN)
    new TimeSeriesRDD[K](targetIndex, mapSeries(rebaserFunction))
  }

  /**
   * Returns a TimeSeriesRDD that's a sub-slice of the given series.
 *
   * @param start The start date the for slice.
   * @param end The end date for the slice (inclusive).
   */
  def slice(start: Long, end: Long): TimeSeriesRDD[K] = {
    slice(longToZonedDateTime(start, ZoneId.systemDefault()),
          longToZonedDateTime(end, ZoneId.systemDefault()))
  }

  /**
   * Fills in missing data (NaNs) in each series according to a given imputation method.
   *
   * @param method "linear", "nearest", "next", or "previous"
   * @return A TimeSeriesRDD with missing observations filled in.
   */
  def fill(method: String): TimeSeriesRDD[K] = {
    mapSeries(UnivariateTimeSeries.fillts(_, method))
  }

  /**
   * Applies a transformation to each time series that preserves the time index of this
   * TimeSeriesRDD.
   */
  def mapSeries[U](f: (Vector) => Vector): TimeSeriesRDD[K] = {
    new TimeSeriesRDD[K](index, map(kt => (kt._1, f(kt._2))))
  }

  /**
   * Applies a transformation to each time series and returns a TimeSeriesRDD with the given index.
   * The caller is expected to ensure that the time series produced line up with the given index.
   */
  def mapSeries[U](f: (Vector) => Vector, index: DateTimeIndex)
    : TimeSeriesRDD[K] = {
    new TimeSeriesRDD[K](index, map(kt => (kt._1, f(kt._2))))
  }

  /**
   * Gets stats like min, max, mean, and standard deviation for each time series.
   */
  def seriesStats(): RDD[StatCounter] = {
    map(kt => new StatCounter(kt._2.valuesIterator))
  }

  /**
   * Essentially transposes the time series matrix to create an RDD where each record contains a
   * single instant in time and all the values that correspond to it. Involves a shuffle operation.
   *
   * In the returned RDD, the ordering of values within each record corresponds to the ordering of
   * the time series records in the original RDD. The records are ordered by time.
   */
  def toInstants(nPartitions: Int = -1): RDD[(ZonedDateTime, Vector)] = {
    // Construct a pre-shuffle RDD where each element is a snippet of a sample, i.e. a set of
    // observations that all correspond to the same timestamp.  Each key is a tuple of
    // (date-time loc in date-time index, original partition ID, chunk ID)
    val maxChunkSize = 20
    val dividedOnMapSide = mapPartitionsWithIndex { case (partitionId, iter) =>
      new Iterator[((Int, Int, Int), Vector)] {
        // Each chunk is a buffer of time series
        var chunk = new ArrayBuffer[Vector]()
        // Current date time.  Gets reset for every chunk.
        var dtLoc: Int = _
        var chunkId: Int = -1

        override def hasNext: Boolean = iter.hasNext || dtLoc < index.size
        override def next(): ((Int, Int, Int), Vector) = {
          if (chunkId == -1 || dtLoc == index.size) {
            chunk.clear()
            while (chunk.size < maxChunkSize && iter.hasNext) {
              chunk += iter.next()._2
            }
            dtLoc = 0
            chunkId += 1
          }

          val arr = new Array[Double](chunk.size)
          var i = 0
          while (i < chunk.size) {
            arr(i) = chunk(i)(dtLoc)
            i += 1
          }
          dtLoc += 1
          ((dtLoc - 1, partitionId, chunkId), new DenseVector(arr))
        }
      }
    }

    // Carry out a secondary sort.  I.e. repartition the data so that all snippets corresponding
    // to the same timestamp end up in the same partition, and timestamps are lines up contiguously.
    val nPart = if (nPartitions == -1) parent.partitions.length else nPartitions
    val denom = index.size / nPart + (if (index.size % nPart == 0) 0 else 1)
    val partitioner = new Partitioner() {
      override def numPartitions: Int = nPart
      override def getPartition(key: Any): Int = key.asInstanceOf[(Int, _, _)]._1 / denom
    }
    implicit val ordering = new Ordering[(Int, Int, Int)] {
      override def compare(a: (Int, Int, Int), b: (Int, Int, Int)): Int = {
        val diff1 = a._1 - b._1
        if (diff1 != 0) {
          diff1
        } else {
          val diff2 = a._2 - b._2
          if (diff2 != 0) {
            diff2
          } else {
            a._3 - b._3
          }
        }
      }
    }
    val repartitioned = dividedOnMapSide.repartitionAndSortWithinPartitions(partitioner)
    repartitioned.mapPartitions { iter0: Iterator[((Int, Int, Int), Vector)] =>
      new Iterator[(ZonedDateTime, Vector)] {
        var snipsPerSample = -1
        var elementsPerSample = -1
        var iter: Iterator[((Int, Int, Int), Vector)] = _

        // Read the first sample specially so that we know the number of elements and snippets
        // for succeeding samples.
        def firstSample(): ArrayBuffer[((Int, Int, Int), Vector)] = {
          var snip = iter0.next()
          val snippets = new ArrayBuffer[((Int, Int, Int), Vector)]()
          val firstDtLoc = snip._1._1

          while (snip != null && snip._1._1 == firstDtLoc) {
            snippets += snip
            snip = if (iter0.hasNext) iter0.next() else null
          }
          iter = if (snip == null) iter0 else Iterator(snip) ++ iter0
          snippets
        }

        def assembleSnips(snips: Iterator[((Int, Int, Int), Vector)])
          : (ZonedDateTime, Vector) = {
          val resVec = BDV.zeros[Double](elementsPerSample)
          var dtLoc = -1
          var i = 0
          for (j <- 0 until snipsPerSample) {
            val ((loc, _, _), snipVec) = snips.next()
            dtLoc = loc
            resVec(i until i + snipVec.length) := toBreeze(snipVec)
            i += snipVec.length
          }
          (index.dateTimeAtLoc(dtLoc), resVec)
        }

        override def hasNext: Boolean = {
          if (iter == null) {
            iter0.hasNext
          } else {
            iter.hasNext
          }
        }

        override def next(): (ZonedDateTime, Vector) = {
          if (snipsPerSample == -1) {
            val firstSnips = firstSample()
            snipsPerSample = firstSnips.length
            elementsPerSample = firstSnips.map(_._2.length).sum
            assembleSnips(firstSnips.toIterator)
          } else {
            assembleSnips(iter)
          }
        }
      }
    }
  }

  /**
   * Performs the same operations as toInstants but returns a DataFrame instead.
   *
   * The schema of the DataFrame returned will be a java.sql.Timestamp column named "instant"
   * and Double columns named identically to their keys in the TimeSeriesRDD
   */
  def toInstantsDataFrame(sqlContext: SQLContext, nPartitions: Int = -1): DataFrame = {
    val instantsRDD = toInstants(nPartitions)

    import sqlContext.implicits._

    val result = instantsRDD.map { case (dt, v) =>
      val timestamp = Timestamp.from(dt.toInstant)
      (timestamp, v.toArray)
    }.toDF()

    val dataColExpr = keys.zipWithIndex.map { case (key, i) => s"_2[$i] AS `$key`" }
    val allColsExpr = "_1 AS instant" +: dataColExpr

    result.selectExpr(allColsExpr: _*)
  }

  /**
   * Returns a DataFrame where each row is an observation containing a timestamp, a key, and a
   * value.
   */
  def toObservationsDataFrame(
      sqlContext: SQLContext,
      tsCol: String = "timestamp",
      keyCol: String = "key",
      valueCol: String = "value"): DataFrame = {

    val rowRdd = flatMap { case (key, series) =>
      series.iterator.flatMap { case (i, value) =>
        if (value.isNaN) {
          None
        } else {
          val inst = index.dateTimeAtLoc(i).toInstant()
          Some(Row(Timestamp.from(inst), key.toString, value))
        }
      }
    }

    val schema = new StructType(Array(
      new StructField(tsCol, TimestampType),
      new StructField(keyCol, StringType),
      new StructField(valueCol, DoubleType)
    ))

    sqlContext.createDataFrame(rowRdd, schema)
  }

  /**
   * Converts a TimeSeriesRDD into a distributed IndexedRowMatrix, useful to take advantage
   * of Spark MLlib's statistic functions on matrices in a distributed fashion. This is only
   * supported for cases with a uniform time series index. See
   * [[http://spark.apache.org/docs/latest/mllib-data-types.html]] for more information on the
   * matrix data structure
 *
   * @param nPartitions number of partitions, default to -1, which represents the same number
   *                    as currently used for the TimeSeriesRDD
   * @return an equivalent IndexedRowMatrix
   */
  def toIndexedRowMatrix(nPartitions: Int = -1): IndexedRowMatrix = {
    if (!index.isInstanceOf[UniformDateTimeIndex]) {
      throw new UnsupportedOperationException("only supported for uniform indices")
    }
    // each record contains a value per time series, in original order
    // and records are ordered by time
    val uniformIndex = index.asInstanceOf[UniformDateTimeIndex]
    val instants = toInstants(nPartitions)
    val start = uniformIndex.first
    val rows = instants.map { x =>
      val rowIndex = uniformIndex.frequency.difference(start, x._1)
      val rowData = Vectors.dense(x._2.toArray)
      IndexedRow(rowIndex, rowData)
    }
    new IndexedRowMatrix(rows)
  }

  /**
   * Converts a TimeSeriesRDD into a distributed RowMatrix, note that indices in
   * a RowMatrix are not significant, and thus this is a valid operation regardless
   * of the type of time index.  See
   * [[http://spark.apache.org/docs/latest/mllib-data-types.html]] for more information on the
   * matrix data structure
 *
   * @return an equivalent RowMatrix
   */
  def toRowMatrix(nPartitions: Int = -1): RowMatrix = {
    val instants = toInstants(nPartitions)
    val rows = instants.map { x => Vectors.dense(x._2.toArray) }
    new RowMatrix(rows)
  }

  def compute(split: Partition, context: TaskContext): Iterator[(K, Vector)] = {
    parent.iterator(split, context)
  }

  protected def getPartitions: Array[Partition] = parent.partitions

  /**
   * Writes out the contents of this TimeSeriesRDD to a set of CSV files in the given directory,
   * with an accompanying file in the same directory including the time index.
   */
  def saveAsCsv(path: String): Unit = {
    // Write out contents
    parent.map { case (key, vec) => key + "," + vec.valuesIterator.mkString(",") }
      .saveAsTextFile(path)

    // Write out time index
    val fs = FileSystem.get(new Configuration())
    val os = fs.create(new Path(path + "/timeIndex"))
    val ps = new PrintStream(os)
    ps.println(index.toString)
    ps.close()
  }

  /**
    * Writes out the contents of this TimeSeriesRDD to a parquet file in the provided path
    * with an accompanying file in the same directory including the time index.
    *
    * Because TimeSeriesRDDs are structured such that each element of the RDD is a (key, column)
    * pair, the parquet file will have the same column-based representation.
    *
    * To instantiate a TimeSeriesRDD from a file written using this method, call
    * TimeSeriesRDD.timeSeriesRDDFromParquet().
    *
    * @param path: full HDFS path to the file to save to. The datetime index file will be
    *            saved to the same path, but with a ".idx" extension appended to it.
    *
    * @param spark: your current SparkSession instance.
    */
  def saveAsParquetDataFrame(path: String, spark: SparkSession): Unit = {
    // Write out contents
    import spark.implicits._
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec.", "snappy")

    // NOTE: toDF() doesn't work with generic types, need to force String type (which should
    // encompass most other types, even complex ones if toString() is implemented properly)
    val df = parent.map(pair => {
      if (pair == null) {
        ("", Vectors.dense(Array[Double]()))
      } else {
        if (pair._1 == null) {
          ("", Vectors.dense(Array[Double]()))
        } else if (pair._2 == null) {
          ("", Vectors.dense(Array[Double]()))
        } else {
          (pair._1.toString(), Vectors.dense(pair._2.toArray))
        }
      }
    }).toDF()

    df.write.mode(SaveMode.Overwrite).parquet(path)

    // Write out time index
    spark.sparkContext.parallelize(Array(index.toString())).saveAsTextFile(path + ".idx")
  }

  /**
   * Returns a TimeSeriesRDD rebased on top of a new index.  Any timestamps that exist in the new
   * index but not in the existing index will be filled in with NaNs.  [[resample]] offers similar
   * functionality with richer semantics for aggregating values within windows.
   *
   * @param newIndex The DateTimeIndex for the new RDD
   */
  def withIndex(newIndex: DateTimeIndex): TimeSeriesRDD[K] = {
    val rebaser = TimeSeriesUtils.rebaser(index, newIndex, Double.NaN)
    mapSeries(rebaser, newIndex)
  }

  /**
   * Returns a TimeSeriesRDD with each series resampled to a new date-time index. Resampling
   * provides flexible semantics for specifying which date-times in each input series correspond to
   * which date-times in the output series, and for aggregating observations when downsampling.
   *
   * Based on the closedRight and stampRight parameters, resampling partitions time into non-
   * overlapping intervals, each corresponding to a date-time in the target index. Each resulting
   * value in the output series is determined by applying an aggregation function over all the
   * values that fall within the corresponding window in the input series. If no values in the
   * input series fall within the window, a NaN is used.
   *
   * Compare with the equivalent functionality in Pandas:
   * http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.resample.html
   *
   * @param targetIndex The date-time index of the resulting series.
   * @param aggr Function for aggregating multiple points that fall within a window.
   * @param closedRight If true, the windows are open on the left and closed on the right. Otherwise
   *                    the windows are closed on the left and open on the right.
   * @param stampRight If true, each date-time in the resulting series marks the end of a window.
   *                   This means that all observations after the end of the last window will be
   *                   ignored. Otherwise, each date-time in the resulting series marks the start of
   *                   a window. This means that all observations after the end of the last window
   *                   will be ignored.
   * @return The values of the resampled series.
   */

  def resample(
      targetIndex: DateTimeIndex,
      aggr: (Array[Double], Int, Int) => Double,
      closedRight: Boolean,
      stampRight: Boolean): TimeSeriesRDD[K] = {
    mapSeries(Resample.resample(_, index, targetIndex, aggr, closedRight, stampRight), targetIndex)
  }

  /**
    * Returns a TimeSeriesRDD where each time series is summed with a running n-window.
    * Align specifies whether the index of the result should be left- or right-aligned
    * or centered (default) compared to the rolling window of observations.
    *
    * Right alignment means that, in the output series, the value at each time point
    * will be computed using the values in the input series at the previous N time points.
    * Left alignment is similar, but uses the values in the following N time points.
    *
    * @param n Rolling window size
    * @param align Alignment
    */
  def rollSum(n: Int, align: String = "Center"): TimeSeriesRDD[K] = {
    mapSeries(
      UnivariateTimeSeries.rollSum(_, n),
      align match {
        case "Right" => index.islice(n - 1, index.size)
        case "Center" => index.islice(floor((n - 1) / 2).toInt,
          floor((n - 1) / 2).toInt + index.size - n + 1)
        case "Left" => index.islice(0, index.size - n + 1)
      }
    )
  }

  /**
    * Returns a TimeSeriesRDD where each time series is summed.
    */
  def sum(): TimeSeriesRDD[K] = rollSum(index.size)

  /**
    * Returns a TimeSeriesRDD where each time series is averaged with a running n-window.
    * Align specifies whether the index of the result should be left- or right-aligned
    * or centered (default) compared to the rolling window of observations.
    *
    * Right alignment means that, in the output series, the value at each time point
    * will be computed using the values in the input series at the previous N time points.
    * Left alignment is similar, but uses the values in the following N time points.
    *
    * @param n Rolling window size
    * @param align Alignment
    */
  def rollMean(n: Int, align: String = "Center"): TimeSeriesRDD[K] = {
    rollSum(n, align).mapSeries(vec => new DenseVector(vec.toArray.map(_ / n)))
  }

  /**
    * Returns a TimeSeriesRDD where each time series is averaged.
    */
  def mean(): TimeSeriesRDD[K] = rollMean(index.size)
}

object TimeSeriesRDD {
  /**
   * Instantiates a TimeSeriesRDD.
   *
   * @param targetIndex DateTimeIndex to conform all the indices to.
   * @param seriesRDD RDD of time series, each with their own DateTimeIndex.
   */
  def timeSeriesRDD[K](
      targetIndex: UniformDateTimeIndex,
      seriesRDD: RDD[(K, UniformDateTimeIndex, Vector)])
      (implicit kClassTag: ClassTag[K]): TimeSeriesRDD[K] = {
    val rdd = seriesRDD.map { case (key, index, vec) =>
      val newVec: Vector = TimeSeriesUtils.rebase(index, targetIndex, vec, Double.NaN)
      (key, newVec)
    }
    new TimeSeriesRDD[K](targetIndex, rdd)
  }

  /**
   * Instantiates a TimeSeriesRDD from an RDD of TimeSeries.
   *
   * @param targetIndex DateTimeIndex to conform all the indices to.
   * @param seriesRDD RDD of time series, each with their own DateTimeIndex.
   */
  def timeSeriesRDD[K](targetIndex: DateTimeIndex, seriesRDD: RDD[TimeSeries[K]])
      (implicit kClassTag: ClassTag[K]): TimeSeriesRDD[K] = {
    val rdd = seriesRDD.flatMap { series =>
      series.univariateKeyAndSeriesIterator().map { case (key, vec) =>
        val newVec: Vector = TimeSeriesUtils.rebase(series.index, targetIndex, vec, Double.NaN)
        (key, newVec)
      }
    }
    new TimeSeriesRDD[K](targetIndex, rdd)
  }

  /**
   * Instantiates a TimeSeriesRDD from a DataFrame of observations.
   *
   * @param targetIndex DateTimeIndex to conform all the series to.
   * @param df The DataFrame.
   * @param tsCol The Timestamp column telling when the observation occurred.
   * @param keyCol The string column labeling which string key the observation belongs to..
   * @param valueCol The observed value..
   */
  def timeSeriesRDDFromObservations(
      targetIndex: DateTimeIndex,
      df: DataFrame,
      tsCol: String,
      keyCol: String,
      valueCol: String): TimeSeriesRDD[String] = {
    val rdd = df.select(tsCol, keyCol, valueCol).rdd.map { row =>
      ((row.getString(1), row.getAs[Timestamp](0)), row.getDouble(2))
    }
    implicit val ordering = new Ordering[(String, Timestamp)] {
      override def compare(a: (String, Timestamp), b: (String, Timestamp)): Int = {
        val strCompare = a._1.compareTo(b._1)
        if (strCompare != 0) strCompare else a._2.compareTo(b._2)
      }
    }

    val shuffled = rdd.repartitionAndSortWithinPartitions(new Partitioner() {
      val hashPartitioner = new HashPartitioner(rdd.partitions.length)
      override def numPartitions: Int = hashPartitioner.numPartitions
      override def getPartition(key: Any): Int =
        hashPartitioner.getPartition(key.asInstanceOf[(Any, Any)]._1)
    })
    new TimeSeriesRDD[String](targetIndex, shuffled.mapPartitions { iter =>
      val bufferedIter = iter.buffered
      new Iterator[(String, DenseVector)]() {
        override def hasNext: Boolean = bufferedIter.hasNext

        override def next(): (String, DenseVector) = {
          // TODO: this will be slow for Irregular DateTimeIndexes because it will result in an
          // O(log n) lookup for each element.
          val series = new Array[Double](targetIndex.size)
          Arrays.fill(series, Double.NaN)
          val first = bufferedIter.next()
          val firstLoc = targetIndex.locAtDateTime(
            ZonedDateTime.ofInstant(first._1._2.toInstant, targetIndex.zone))
          if (firstLoc >= 0) {
            series(firstLoc) = first._2
          }
          val key = first._1._1
          while (bufferedIter.hasNext && bufferedIter.head._1._1 == key) {
            val sample = bufferedIter.next()
            val sampleLoc = targetIndex.locAtDateTime(
              ZonedDateTime.ofInstant(sample._1._2.toInstant, targetIndex.zone))
            if (sampleLoc >= 0) {
              series(sampleLoc) = sample._2
            }
          }
          (key, new DenseVector(series))
        }
      }
    })
  }

  /**
   * Loads a TimeSeriesRDD from a directory containing a set of CSV files and a date-time index.
   */
  def timeSeriesRDDFromCsv(path: String, sc: SparkContext)
    : TimeSeriesRDD[String] = {
    val rdd = sc.textFile(path).map { line =>
      val tokens = line.split(",")
      val series = new DenseVector(tokens.tail.map(_.toDouble))
      (tokens.head, series.asInstanceOf[Vector])
    }

    val fs = FileSystem.get(new Configuration())
    val is = fs.open(new Path(path + "/timeIndex"))
    val dtIndex = DateTimeIndex.fromString(new BufferedReader(new InputStreamReader(is)).readLine())
    is.close()

    new TimeSeriesRDD[String](dtIndex, rdd)
  }

  /**
    * Loads a TimeSeriesRDD from a parquet file and a date-time index.
    */
  def timeSeriesRDDFromParquet(path: String, spark: SparkSession) = {
    val df = spark.read.parquet(path)

    import spark.implicits._
    val parent = df.map(row => (row(0).toString(), Vectors.dense(row(1).asInstanceOf[Vector].toArray)))

    // Write out time index
    val textDateTime: String = spark.sparkContext.textFile(path + ".idx").collect().head
    val index = DateTimeIndex.fromString(textDateTime)

    new TimeSeriesRDD[String](index, parent.rdd)
  }

  /**
   * Creates a TimeSeriesRDD from rows in a binary format that Python can write to.
   * Not a public API. For use only by the Python API.
   */
  def timeSeriesRDDFromPython(index: DateTimeIndex, pyRdd: RDD[Array[Byte]])
    : TimeSeriesRDD[String] = {
    new TimeSeriesRDD[String](index, pyRdd.map { arr =>
      val buf = ByteBuffer.wrap(arr)
      val numChars = buf.getInt()
      val keyChars = new Array[Char](numChars)
      var i = 0
      while (i < numChars) {
        keyChars(i) = buf.getChar()
        i += 1
      }

      val seriesSize = buf.getInt()
      val series = new Array[Double](seriesSize)
      i = 0
      while (i < seriesSize) {
        series(i) = buf.getDouble()
        i += 1
      }
      (new String(keyChars), new DenseVector(series))
    })
  }
}
