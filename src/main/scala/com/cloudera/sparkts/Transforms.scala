package com.cloudera.sparkts

import java.time.ZonedDateTime

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.SparkContext

import MatrixUtil._

import scala.reflect.ClassTag

/**
  * Created by Simon on 11/17/2015.
  */
object Transforms {

  def timeDerivative[K: ClassTag](ts: TimeSeries[K], baseFrequency: Frequency): TimeSeries[K] = {
    val newIndex = ts.index.islice(1, ts.index.size)
    ts.mapSeries(newIndex, vec => {
      val timeDataVec = ts.index.toZonedDateTimeArray.zip(vec.toArray)
      val zippedPairs = timeDataVec.zip(timeDataVec.drop(1))

      val output = zippedPairs.map(pair => {
        val valueDiff = pair._1._2 - pair._2._2
        val timeDiff = baseFrequency.difference(pair._2._1, pair._1._1)

        valueDiff / timeDiff
      })

      breeze.linalg.Vector(output)
    })
  }

  def timeDerivative[K: ClassTag](
    ts: TimeSeriesRDD[K],
    baseFrequency: Frequency,
    sc: SparkContext)
    : TimeSeriesRDD[K] = {

    val bIndex = sc.broadcast(ts.index)
    val bBaseFreq = sc.broadcast(baseFrequency)

    val newIndex = ts.index.islice(1, ts.index.size)
    ts.mapSeries(vec => {
      val timeDataVec = bIndex.value.toZonedDateTimeArray.zip(vec.toArray)
      val zippedPairs = timeDataVec.zip(timeDataVec.drop(1))

      val output = zippedPairs.map(pair => {
        val valueDiff = pair._1._2 - pair._2._2
        val timeDiff = bBaseFreq.value.difference(pair._2._1, pair._1._1)

        valueDiff / timeDiff
      })

      breeze.linalg.Vector(output)
    }, newIndex)
  }

}
