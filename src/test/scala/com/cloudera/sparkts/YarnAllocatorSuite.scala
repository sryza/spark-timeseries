package com.cloudera.sparkts

import org.scalatest.FunSuite
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.Random
import org.apache.hadoop.yarn.api.records.{Priority, Resource}

class YarnAllocatorSuite extends FunSuite {
  test("benchmark") {
    val map = new HashMap[String, Int]
    val numNodes = 1000
    val numTasks = 10000
    val rand = new Random(100)
    var i = 0
    while (i < numTasks) {
      val taskLocations = (0 until 3).map(x => rand.nextInt(numNodes).toString)
      taskLocations.foreach { taskLoc =>
        if (map.contains(taskLoc)) {
          map(taskLoc) += 1
        } else {
          map(taskLoc) = 1
        }
      }
      i += 1
    }
    println("done generating tasks")
    val amClient = AMRMClient.createAMRMClient[ContainerRequest]()
    var existing = new ArrayBuffer[ContainerRequest](0)
    println("done creating amrmclient")
    existing = regenerateContainerRequests(amClient, existing, map)
    val warmupRounds = 1000
    val warmupStart = System.currentTimeMillis
    i = 0
    while (i < warmupRounds) {
      existing = regenerateContainerRequests(amClient, existing, map)
      i += 1
    }
    val warmupTime = System.currentTimeMillis - warmupStart
    println(s"warmup time: $warmupTime")
    val actualRounds = 1000
    val actualStart = System.currentTimeMillis
    i = 0
    while (i < actualRounds) {
      existing = regenerateContainerRequests(amClient, existing, map)
      i += 1
    }
    val actualTime = System.currentTimeMillis - actualStart
    println(s"actual time: $actualTime")
  }

  val _resource = Resource.newInstance(1024, 1)
  val _priority = Priority.newInstance(1)

  def regenerateContainerRequests(
     amClient: AMRMClient[ContainerRequest],
     existingRequests: Seq[ContainerRequest],
     prefs: HashMap[String, Int]): ArrayBuffer[ContainerRequest] = {
    var i = 0
    while (i < existingRequests.length) {
      amClient.removeContainerRequest(existingRequests(i))
      i += 1
    }
    val prefsClone = new HashMap[String, Int]
    prefsClone ++= prefs
    val newRequests = new ArrayBuffer[ContainerRequest]
    while (!prefsClone.isEmpty) {
      val locs = prefsClone.keySet.toArray
      var j = 0
      while (j < locs.length) {
        val num = prefsClone(locs(j))
        if (num == 1) {
          prefsClone.remove(locs(j))
        } else {
          prefsClone(locs(j)) = num - 1
        }
        j += 1
      }
      val request = new ContainerRequest(_resource, null, locs, _priority)
      newRequests += request
      amClient.addContainerRequest(request)
    }
    newRequests
  }
}
