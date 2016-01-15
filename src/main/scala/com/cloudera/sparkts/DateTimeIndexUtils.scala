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

import java.time.ZoneId

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
            indexBuffer.map(_.toNanosArray).reduce(_ ++ _),
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
   *
   * Algorithm:
   * 
   * originalList: a list of non-unioned non-sorted indices
   * sortedList: same elements from originalList however sorted s.t.
   *             index i is before index j iff:
   *              i.first < j.first ||
   *              (i.first == j.first && i.size < j.size)
   * unionList: a list that contains the union result that is also
   *            sorted by definition/construction
   *
   * first element from the sortedList is removed and unionList is
   * initialized by that element.
   *
   * as long as there more elements in the sortedList:
   *  - remove the last/largest element from the unionList, let it be a
   *  - remove the first/least element from the sortedList, let it be b
   *  - by construction, a < b
   *  - if there is no overlap between a and b, append a then b to the
   *    unionList
   *  - otherwise, handle the overlap:
   *   - trim the leading instants of b that are already in a
   *     , that will affect the sort order of b as other indices in
   *     sortedList might now be less than the trimmed b, so put back
   *     the rest of b in the sortedList in the new correct sort order
   *     , and put back a to the end of unionList
   *   - no trimming -> split a on b.first into aLower and aUpper, now
   *     aLower < b < aUpper and aLower and b are non-overlapping, however
   *     b and aUpper might be overlapping, hence append aLower and b to
   *     the unionList, and put aUpper in the correct sort order in the
   *     sortedList
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
      while (a.locAtDateTime(b.first) > -1) {
        b = b.islice(1, b.size)
        isBTrimmed = true
      }
      
      if (isBTrimmed && b.size > 0) {
        unionList += a
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
}
