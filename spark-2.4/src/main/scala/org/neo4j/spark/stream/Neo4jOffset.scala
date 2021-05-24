package org.neo4j.spark.stream

import org.apache.spark.sql.execution.streaming.SerializedOffset
import org.apache.spark.sql.sources.v2.reader.streaming.Offset
import org.json4s.DefaultFormats

import java.time.LocalDateTime

class Neo4jOffset(val offset: LocalDateTime) extends Offset {
  implicit val defaultFormats: DefaultFormats = DefaultFormats
  override val json: String = offset.toString

  def update(now: LocalDateTime): Neo4jOffset = {
    new Neo4jOffset(now)
  }
}