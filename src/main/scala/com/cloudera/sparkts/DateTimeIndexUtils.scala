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

import java.time.{ZonedDateTime, ZoneId}

import scala.collection.mutable.{ListBuffer, PriorityQueue}

private[sparkts] object DateTimeIndexUtils {
  implicit val dateTimeIndexOrdering = new Ordering[DateTimeIndex] {
    override def compare(x: DateTimeIndex, y: DateTimeIndex): Int = {
      val c = x.first.compareTo(y.first)
      if (c == 0) x.size.compareTo(y.size) else c
    }
  }

  /**
   * Given an array of indices that are sorted and non-overlapping,
   * consecutive two indices a and b are merged into a new index d
   * according to the following rules:
   *
   *  - a and b are both irregular -> d is irregular
   *  - a and b are both of size 1 -> d is irregular
   *  - either a is irregular and b is of size 1
   *    or a is of size 1 and b is irregular -> d is irregular
   */
  def simplify(indices: Array[DateTimeIndex]): Array[DateTimeIndex] = {
    val simplified = new ListBuffer[DateTimeIndex]

    // a buffer that holds merge candidates
    val indexBuffer = new ListBuffer[DateTimeIndex]
    val lastI = indices.length - 1

    for (i <- 0 to lastI) {
      val currentIndex = indices(i)
      val isAMergeCandidate = currentIndex.size == 1 ||
        currentIndex.isInstanceOf[IrregularDateTimeIndex]

      // if currentIndex is a merge candidate -> append to the buffer
      if (isAMergeCandidate) {
        indexBuffer += currentIndex
      }

      // if the contiguous sequence of merge candidates, if any
      // , is broken by a non-merge candidate or we are at the
      // end of the loop
      if (!isAMergeCandidate || i == lastI) {
        // more than one merge candidate -> do the merge
        if (indexBuffer.length > 1) {
          simplified += new IrregularDateTimeIndex(
            indexBuffer.map(_.toInstantsArray).reduce(_ ++ _),
            indexBuffer.head.zone)
          indexBuffer.clear()
        // only one merge candidate -> no merge
        } else if (indexBuffer.length == 1) {
          simplified += indexBuffer.head
          indexBuffer.clear()
        }

        if (!isAMergeCandidate) simplified += currentIndex
      }
    }

    simplified.toArray
  }

  /**
   * Unions a collection of date-time indices into one date-time index
   */
  def union(indices: Array[DateTimeIndex], zone: ZoneId = ZoneId.systemDefault())
    : DateTimeIndex = {
    val indicesPQ = PriorityQueue.empty[DateTimeIndex](dateTimeIndexOrdering.reverse)
    indices.foreach(indicesPQ.enqueue(_))

    val unionList = new ListBuffer[DateTimeIndex]
    unionList += indicesPQ.dequeue

    while (!indicesPQ.isEmpty) {
      val a = unionList.remove(unionList.length - 1)
      var b = indicesPQ.dequeue
      
      var isBTrimmed = false
      while (a.locAtDateTime(b.first) > -1 && b.size > 0) {
        b = b.islice(1, b.size)
        isBTrimmed = true
      }
      
      if (isBTrimmed) {
        unionList += a
        if (b.size > 0)
          indicesPQ.enqueue(b)
      } else {
        val splitLoc = a.insertionLoc(b.first)
        if (splitLoc < a.size) {
          val aLower = a.islice(0, splitLoc)
          val aUpper = a.islice(splitLoc, a.size)
          unionList += aLower
          unionList += b
          indicesPQ.enqueue(aUpper)
        } else {
          unionList += a
          unionList += b
        }
      }
    }

    val simplified = simplify(unionList.toArray)
    
    new HybridDateTimeIndex(simplified).atZone(zone)
  }

  /**
   * Intersects a collection of date-time indices into one date-time index
   */
  def intersect(indices: Array[DateTimeIndex], zone: ZoneId = ZoneId.systemDefault())
    : Option[DateTimeIndex] = {
    val indicesSorted = indices.sorted.toBuffer

    var mayIntersect = true

    for (i <- 0 until indicesSorted.length - 1; if mayIntersect) {
      val iIndex = indicesSorted(i)
      // the farthest index from iIndex is least likely to intersect with iIndex
      // that helps to cut the loop earlier
      for (j <- (i + 1 until indicesSorted.length).reverse; if mayIntersect) {
        val jIndex = indicesSorted(j)
        mayIntersect = iIndex.last.compareTo(jIndex.first) >= 0
      }
    }

    while (indicesSorted.length > 1 && mayIntersect) {
      val a = indicesSorted.remove(0)
      val b = indicesSorted.remove(0)
      var intersectionBuffer = new ListBuffer[ZonedDateTime]

      var hasPassedEndOfA = false
      val bIter = b.zonedDateTimeIterator
      while (bIter.hasNext && !hasPassedEndOfA) {
        val dt = bIter.next
        hasPassedEndOfA = dt.isAfter(a.last)
        if (!hasPassedEndOfA && a.locAtDateTime(dt) > -1)
          intersectionBuffer += dt
      }

      if (intersectionBuffer.length > 0) {
        val intersectionIndex = DateTimeIndex.irregular(intersectionBuffer.toArray, zone)

        var insertionLoc = indicesSorted.length
        for (i <- 0 until indicesSorted.length;
             if mayIntersect && insertionLoc == indicesSorted.length) {
          val iIndex = indicesSorted(i)
          if (dateTimeIndexOrdering.lteq(iIndex, intersectionIndex)) {
            mayIntersect = iIndex.last.compareTo(intersectionIndex.first) >= 0
          } else {
            insertionLoc = i
            // the farthest index from intersectionIndex is least likely to intersect
            // with intersectionIndex that helps to cut the loop earlier
            for (j <- (i until indicesSorted.length).reverse; if mayIntersect) {
              val jIndex = indicesSorted(j)
              mayIntersect = intersectionIndex.last.compareTo(jIndex.first) >= 0
            }
          }
        }

        if (mayIntersect) {
          indicesSorted.insert(insertionLoc, intersectionIndex)
        }
      } else {
        mayIntersect = false
      }
    }

    if (mayIntersect && indicesSorted.length == 1) {
      Some(indicesSorted.head)
    } else {
      None
    }
  }
}
