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
}
