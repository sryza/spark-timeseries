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

import org.apache.spark.mllib.linalg._
import breeze.plot._
import com.cloudera.sparkts.models.Autoregression

import org.apache.commons.math3.distribution.NormalDistribution

object EasyPlot {
  def ezplot(vec: Vector, style: Char): Figure = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot((0 until vec.size).map(_.toDouble).toArray, vec.toArray, style = style)
    f
  }

  def ezplot(vec: Vector): Figure = ezplot(vec, '-')

  def ezplot(arr: Array[Double], style: Char): Figure = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot(arr.indices.map(_.toDouble).toArray, arr, style = style)
    f
  }

    def ezplot(arr: Array[Double]): Figure = ezplot(arr, '-')

  def ezplot(vecs: Seq[Vector], style: Char): Figure = {
    val f = Figure()
    val p = f.subplot(0)
    val first = vecs.head
    vecs.foreach { vec =>
      p += plot((0 until first.size).map(_.toDouble).toArray, vec.toArray, style)
    }
    f
  }

  def ezplot(vecs: Seq[Vector]): Figure = ezplot(vecs, '-')

  /**
   * Autocorrelation function plot
   * @param data array of data to analyze
   * @param maxLag maximum lag for autocorrelation
   * @param conf confidence bounds to display
   */
  def acfPlot(data: Array[Double], maxLag: Int, conf: Double = 0.95): Figure = {
    // calculate correlations and confidence bound
    val autoCorrs = UnivariateTimeSeries.autocorr(data, maxLag)
    val confVal = calcConfVal(conf, data.length)

    // Basic plot information
    val f = Figure()
    val p = f.subplot(0)
    p.title = "Autocorrelation function"
    p.xlabel = "Lag"
    p.ylabel = "Autocorrelation"
    drawCorrPlot(autoCorrs, confVal, p)
    f
  }

  /**
   * Partial autocorrelation function plot
   * @param data array of data to analyze
   * @param maxLag maximum lag for partial autocorrelation function
   * @param conf confidence bounds to display
   */
  def pacfPlot(data: Array[Double], maxLag: Int, conf: Double = 0.95): Figure = {
    // create AR(maxLag) model, retrieve coefficients and calculate confidence bound
    val model = Autoregression.fitModel(new DenseVector(data), maxLag)
    val pCorrs = model.coefficients // partial autocorrelations are the coefficients in AR(n) model
    val confVal = calcConfVal(conf, data.length)

    // Basic plot information
    val f = Figure()
    val p = f.subplot(0)
    p.title = "Partial autocorrelation function"
    p.xlabel = "Lag"
    p.ylabel = "Partial Autocorrelation"
    drawCorrPlot(pCorrs, confVal, p)
    f
  }

  private[sparkts] def calcConfVal(conf: Double, n: Int): Double = {
    val stdNormDist = new NormalDistribution(0, 1)
    val pVal = (1 - conf) / 2.0
    stdNormDist.inverseCumulativeProbability(1 - pVal) / Math.sqrt(n)
  }

  private[sparkts] def drawCorrPlot(corrs: Array[Double], confVal: Double, p: Plot): Unit = {
    // make decimal ticks visible
    p.setYAxisDecimalTickUnits()
    // plot correlations as vertical lines
    val verticalLines = corrs.zipWithIndex.map { case (corr, ix) =>
      (Array(ix.toDouble + 1, ix.toDouble + 1), Array(0, corr))
    }
    verticalLines.foreach { case (xs, ys) => p += plot(xs, ys) }
    // plot confidence intervals as horizontal lines
    val n = corrs.length
    Array(confVal, -1 * confVal).foreach { conf =>
      val xs = (0 to n).toArray.map(_.toDouble)
      val ys = Array.fill(n + 1)(conf)
      p += plot(xs, ys, '-', colorcode = "red")
    }
  }
}
