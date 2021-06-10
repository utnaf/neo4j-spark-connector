package org.neo4j.spark.streaming

import java.util.concurrent.ConcurrentHashMap
import java.util.function

object OffsetStorage {

  private val cache = new ConcurrentHashMap[String, Neo4jOffset]()

  def getLastOffset(jobId: String) = cache.get(jobId)

  def setLastOffset(jobId: String,
                    neo4jOffset: Neo4jOffset) = cache.compute(jobId, new function.BiFunction[String, Neo4jOffset, Neo4jOffset] {
    override def apply(id: String, offset: Neo4jOffset): Neo4jOffset = {
      if (offset == null || offset.offset < neo4jOffset.offset) {
        neo4jOffset
      } else {
        offset
      }
    }
  })

  def clearForJobId(jobId: String) = cache.remove(jobId)

}
