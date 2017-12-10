package com.cloudera.sparkts.models

import org.apache.spark.mllib.KernelType._
import org.apache.spark.mllib.Kernel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable.ArrayBuffer
import java.util.PriorityQueue
import java.util.Comparator
import scala.collection.JavaConverters._

class BoundedPriorityQueue[E](k: Int,
                              comp: Comparator[E]) extends PriorityQueue[E](k, comp) {
  override def add(elem: E): Boolean = {
    if (size() < k) {
      offer(elem)
      true
    } else {
      val head = peek()
      if (head != null && comp.compare(elem, head) > 0) {
        poll()
        offer(elem)
        return true
      }
      return false
    }
  }
}

/**
  * @author debasish83, xiangzhe, santanu.das
  */
//TODO: history and query can be combined into a vector with offset giving queries
//TODO: distance and point both should be covered by Neighbor
case class Neighbor(history: Vector, query: Double)

class NeighborOrder extends Ordering[(Int, Double)] {
  override def compare(x: (Int, Double), y: (Int, Double)): Int = {
    x._2 compare y._2
  }
}

class KNNRegressionModel extends TimeSeriesModel {
  override def addTimeDependentEffects(ts: Vector, dest: Vector) = ???

  override def removeTimeDependentEffects(ts: Vector, dest: Vector) = ???
}

object KNNRegression {
  def maxnorm(timeseries: Array[Double]): Double = {
    val max = timeseries.max
    var i = 0
    while (i < timeseries.length) {
      timeseries(i) /= max
      i += 1
    }
    return max
  }

  def nearestNeighbors(timeseries: Array[Double],
                       featureDim: Int,
                       queryLength: Int,
                       kernel: Kernel,
                       topk: Int): Array[Neighbor] = {
    val targetArray = new Array[Neighbor](queryLength)
    var i = 0
    while (i < queryLength) {
      var j = i
      val regressorEnd = timeseries.size - queryLength - featureDim - 1 + i
      val queryStart = regressorEnd + 1
      val regressor = new Array[Double](featureDim)
      val query = new Array[Double](featureDim)

      if (queryStart > 0) {
        Array.copy(timeseries, queryStart, query, 0, featureDim)
      }

      val ord = new NeighborOrder()
      val minHeap = new BoundedPriorityQueue[(Int, Double)](topk, ord.reverse)

      while (j <= regressorEnd) {
        Array.copy(timeseries, j, regressor, 0, featureDim)
        // Generate feature matrix for linear model generation
        val distance = kernel.compute(Vectors.dense(regressor), 0, Vectors.dense(query), 0)
        val targetIndex = j + featureDim
        if (minHeap.size == topk) {
          if (minHeap.peek()._2 > distance) {
            minHeap.poll()
            minHeap.add((targetIndex, distance))
          }
        }
        else {
          minHeap.add((targetIndex, distance))
        }
        j = j + 1
      }
      val indexArray = new Array[Int](minHeap.size)
      val matchedVector = new Array[Double](minHeap.size)
      val heapToArray = minHeap.iterator().asScala.toArray
      var k = 0
      while (k < minHeap.size) {
        val index = heapToArray(k)._1
        indexArray(k) = index
        matchedVector(k) = timeseries(index)
        k += 1
      }
      val matchedTarget = Vectors.dense(matchedVector)
      val queryPoint = timeseries(queryStart + featureDim)
      minHeap.clear()
      targetArray.update(i, Neighbor(matchedTarget, queryPoint))
      i = i + 1
    }
    targetArray
  }

  def predict(timeseries: Array[Double],
              topk: Int,
              featureDim: Int,
              normalize: Boolean,
              multiStep: Int,
              metric: KernelType = Euclidean): Array[Double] = {
    val kernel = Kernel(metric)
    val max =
      if (normalize) maxnorm(timeseries)
      else 1.0

    val historyBuf = new ArrayBuffer[Double](timeseries.length + multiStep)
    timeseries.foreach(historyBuf += _)

    val multiPredict = (0 until multiStep).toArray.map { case (_) =>
      val neighbors = nearestNeighbors(
        historyBuf.toArray,
        featureDim,
        queryLength = 1,
        kernel,
        topk)
      require(neighbors.length == 1, s"neighbors ${neighbors.length} higher than 1")
      val point = neighbors(0).query
      val history = neighbors(0).history.toArray
      val predicted = history.foldLeft(point)(_ + _) / (history.length + 1)
      historyBuf += predicted
      predicted
    }

    var i = 0
    while (i < timeseries.length) {
      timeseries.update(i, timeseries(i) * max)
      i += 1
    }
    i = 0
    while (i < multiPredict.length) {
      multiPredict.update(i, multiPredict(i) * max)
      i += 1
    }
    multiPredict
  }
}
