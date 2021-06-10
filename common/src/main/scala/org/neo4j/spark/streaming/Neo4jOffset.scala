package org.neo4j.spark.streaming

import org.apache.spark.sql.sources.v2.reader.streaming.Offset
import org.neo4j.spark.util.StreamingFrom

object Neo4jOffset{
  val ALL = new Neo4jOffset(-1)

  def from(from: StreamingFrom.Value): Neo4jOffset = from match {
    case StreamingFrom.NOW => new Neo4jOffset()
    case StreamingFrom.ALL => ALL
  }
}

class Neo4jOffset(val offset: Long = System.currentTimeMillis()) extends Offset {
  override val json: String = offset.toString
}
