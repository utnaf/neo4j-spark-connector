package org.neo4j.spark.streaming

import java.util.concurrent.ConcurrentHashMap
import java.util.function

object OffsetStorage {

  private val cache = new ConcurrentHashMap[String, Long]()

  def getLastOffset(jobId: String) = cache.get(jobId)

  def setLastOffset(jobId: String,
                    neo4jOffset: Long) = cache.compute(jobId, new function.BiFunction[String, Long, Long] {
    override def apply(id: String, offset: Long): Long = {
      if (offset == null || offset < neo4jOffset) {
        neo4jOffset
      } else {
        offset
      }
    }
  })

  def clearForJobId(jobId: String) = cache.remove(jobId)

}
