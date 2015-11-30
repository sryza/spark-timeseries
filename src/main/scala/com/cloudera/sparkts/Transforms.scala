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
    val instants = ts.toInstants()

    val pairs = instants.drop(1).zip(instants)

    val diffedDataBreeze = new BDM[Double](instants.length - 1, instants.head._2.size)
    for (rowIndex <- 0 until instants.length - 1) {
      val pair = pairs(rowIndex)
      val timeDiff = baseFrequency.difference(pair._2._1, pair._1._1)

      for (i <- 0 until pair._1._2.size) {
        val tmp = pair._1._2(i) - pair._2._2(i)
        val diffValue = tmp / timeDiff

        diffedDataBreeze(rowIndex, i) = diffValue
      }
    }

    new TimeSeries[K](ts.index.islice(1, ts.index.size), diffedDataBreeze, ts.keys)
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
