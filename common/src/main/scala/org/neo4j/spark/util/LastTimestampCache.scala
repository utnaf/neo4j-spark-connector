package org.neo4j.spark.util

import org.neo4j.spark.util.LastTimestampCache.cache

import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap

object LastTimestampCache {

  private val cache: ConcurrentHashMap[String, LocalDateTime] = new ConcurrentHashMap[String, LocalDateTime]

}

class LastTimestampCache extends Serializable {
  def get(jobId: String): Option[LocalDateTime] = {
    this.synchronized {
      Option(cache.getOrDefault(jobId, null))
    }
  }

  def set(jobId: String, timestamp: LocalDateTime): Unit = {
    this.synchronized {
      cache.put(jobId, timestamp)
    }
  }
}