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
import breeze.plot._

import org.apache.commons.math3.distribution.NormalDistribution


object EasyPlot {
  def ezplot(vec: Vector[Double], style: Char): Unit = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot((0 until vec.length).map(_.toDouble).toArray, vec, style = style)
  }

  def ezplot(vec: Vector[Double]): Unit = ezplot(vec, '-')

  def ezplot(arr: Array[Double], style: Char): Unit = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot((0 until arr.length).map(_.toDouble).toArray, arr, style = style)
  }

  def ezplot(arr: Array[Double]): Unit = ezplot(arr, '-')

  def ezplot(vecs: Seq[Vector[Double]], style: Char): Unit = {
    val f = Figure()
    val p = f.subplot(0)
    val first = vecs.head
    vecs.foreach { vec =>
      p += plot((0 until first.length).map(_.toDouble).toArray, vec, style)
    }
  }

  def ezplot(vecs: Seq[Vector[Double]]): Unit = ezplot(vecs, '-')

  def acfPlot(data: Array[Double], maxLag: Int, conf: Double = 0.95): Unit = {
    val autoCorrs = UnivariateTimeSeries.autocorr(data, maxLag)
    val nCorrs = autoCorrs.length
    val nSample = data.length
    val stdNormDist = new NormalDistribution(0, 1)

    val pVal = (1 - conf) / 2.0
    val confVal = stdNormDist.inverseCumulativeProbability(1 - pVal) / Math.sqrt(nSample)
    val f = Figure()
    val p = f.subplot(0)

    // Basic information
    p.title = "Auto correlation function"
    p.xlabel = "Lag"
    p.ylabel = "Correlation"

    // plot correlations as vertical lines
    val verticalLines = autoCorrs.zipWithIndex.map { case (corr, ix) =>
      (Array(ix.toDouble, ix.toDouble), Array(0, corr))
      }

    verticalLines.foreach { case (xs, ys) => p += plot(xs, ys) }

    // plot confidence intervals
    Array(confVal, -1 * confVal).foreach { conf =>
      val xs = (1 to nCorrs).toArray.map(_.toDouble)
      val ys = Array.fill(nCorrs)(conf)
       p += plot(xs, ys, '-', colorcode = "red")
      }
  }
}
