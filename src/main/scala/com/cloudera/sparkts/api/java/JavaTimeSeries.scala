package com.cloudera.sparkts.api.java

import com.cloudera.sparkts.MatrixUtil._
import com.cloudera.sparkts.{UniformDateTimeIndex, DateTimeIndex, TimeSeries}
import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.linalg.{Vector, DenseMatrix}
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2}
import org.joda.time.DateTime

import scala.collection.JavaConversions
import scala.reflect.ClassTag

class JavaTimeSeries[K](val ts: TimeSeries[K])(implicit val kClassTag: ClassTag[K])
  extends Serializable {

  def this(index: DateTimeIndex, data: DenseMatrix, keys: Array[K])(implicit kClassTag: ClassTag[K]) {
    this(new TimeSeries[K](index, data, keys))
  }

  def this(index: DateTimeIndex, data: DenseMatrix, keys: List[K])(implicit kClassTag: ClassTag[K]) {
    this(new TimeSeries[K](index, data, keys.toArray))
  }

  def this(index: DateTimeIndex, data: DenseMatrix, keys: java.util.List[K])
    (implicit kClassTag: ClassTag[K]) {
    this(index, data, JavaConversions.asScalaBuffer(keys).toArray)
  }

  def index = ts.index

  def data = ts.data

  def dataAsArray = ts.data.valuesIterator.toArray

  def keys = ts.keys

  /**
   * IMPORTANT: currently this assumes that the DateTimeIndex is a UniformDateTimeIndex, not an
   * Irregular one. This means that this function won't work (yet) on TimeSeries built using
   * timeSeriesFromIrregularSamples().
   *
   * Lags all individual time series of the TimeSeries instance by up to maxLag amount.
   *
   * Example input TimeSeries:
   *   time 	a 	b
   *   4 pm 	1 	6
   *   5 pm 	2 	7
   *   6 pm 	3 	8
   *   7 pm 	4 	9
   *   8 pm 	5 	10
   *
   * With maxLag 2 and includeOriginals = true, we would get:
   *   time 	a 	lag1(a) 	lag2(a)  b 	lag1(b)  lag2(b)
   *   6 pm 	3 	2 	      1         8 	7 	      6
   *   7 pm 	4 	3 	      2         9 	8 	      7
   *   8 pm   5 	4 	      3         10	9 	      8
   *
   */
  def lags(maxLag: Int, includeOriginals: Boolean, laggedKey: JFunction2[K, java.lang.Integer, K])
    : JavaTimeSeries[K] = {
    new JavaTimeSeries[K](ts.lags(maxLag, includeOriginals)
      ((k: K, i: Int) => laggedKey.call(k, new java.lang.Integer(i))))
  }

  def slice(range: Range): JavaTimeSeries[K] = new JavaTimeSeries[K](ts.slice(range))

  def union(vec: Vector, key: K): JavaTimeSeries[K] = new JavaTimeSeries[K](ts.union(vec, key))

  /**
   * Returns a TimeSeries where each time series is differenced with the given order. The new
   * TimeSeries will be missing the first n date-times.
   */
  def differences(lag: Int): JavaTimeSeries[K] = new JavaTimeSeries[K](ts.differences(lag))

  /**
   * Returns a TimeSeries where each time series is differenced with order 1. The new TimeSeries
   * will be missing the first date-time.
   */
  def differences(): JavaTimeSeries[K] = new JavaTimeSeries[K](ts.differences())

  /**
   * Returns a TimeSeries where each time series is quotiented with the given order. The new
   * TimeSeries will be missing the first n date-times.
   */
  def quotients(lag: Int): JavaTimeSeries[K] = new JavaTimeSeries[K](ts.quotients(lag))

  /**
   * Returns a TimeSeries where each time series is quotiented with order 1. The new TimeSeries will
   * be missing the first date-time.
   */
  def quotients(): JavaTimeSeries[K] = new JavaTimeSeries[K](ts.quotients())

  /**
   * Returns a return series for each time series. Assumes periodic (as opposed to continuously
   * compounded) returns.
   */
  def price2ret(): JavaTimeSeries[K] = new JavaTimeSeries[K](ts.price2ret())

  def univariateSeriesIterator(): java.util.Iterator[Vector] =
    JavaConversions.asJavaIterator(ts.univariateSeriesIterator)

  def univariateKeyAndSeriesIterator(): java.util.Iterator[(K, Vector)] =
    JavaConversions.asJavaIterator(ts.univariateKeyAndSeriesIterator)

  /**
   * Applies a transformation to each series that preserves the time index.
   */
  def mapSeries(f: JFunction[Vector, Vector]): JavaTimeSeries[K] =
    new JavaTimeSeries[K](ts.mapSeries(index, (v: Vector) => f.call(v)))

  /**
   * Applies a transformation to each series that preserves the time index. Passes the key along
   * with each series.
   */
  def mapSeriesWithKey(f: JFunction2[K, Vector, Vector]): JavaTimeSeries[K] =
    new JavaTimeSeries[K](ts.mapSeriesWithKey((k: K, v: Vector) => f.call(k, v)))

  /**
   * Applies a transformation to each series such that the resulting series align with the given
   * time index.
   */
  def mapSeries(newIndex: DateTimeIndex, f: JFunction[Vector, Vector]): JavaTimeSeries[K] = {
    new JavaTimeSeries[K](ts.mapSeries(newIndex, (v: Vector) => f.call(v)))
  }

  def mapValues[U](f: JFunction[Vector, U]): java.util.List[(K, U)] =
    JavaConversions.seqAsJavaList(ts.mapValues((v: Vector) => f.call(v)))

  /**
   * Gets the first univariate series and its key.
   */
  def head(): (K, Vector) = ts.head

}

object JavaTimeSeries {
  def javaTimeSeriesFromIrregularSamples[K](
      samples: Seq[(DateTime, Array[Double])],
      keys: Array[K],
      zone: DateTimeZone = DateTimeZone.getDefault())
      (implicit kClassTag: ClassTag[K])
    : JavaTimeSeries[K] =
    new JavaTimeSeries[K](TimeSeries.timeSeriesFromIrregularSamples[K](samples, keys, zone))

  /**
   * This function should only be called when you can safely make the assumption that the time
   * samples are uniform (monotonously increasing) across time.
   */
  def javaTimeSeriesFromUniformSamples[K](
      samples: Seq[Array[Double]],
      index: UniformDateTimeIndex,
      keys: Array[K])
      (implicit kClassTag: ClassTag[K])
    : JavaTimeSeries[K] =
    new JavaTimeSeries[K](TimeSeries.timeSeriesFromUniformSamples[K](samples, index, keys))
}