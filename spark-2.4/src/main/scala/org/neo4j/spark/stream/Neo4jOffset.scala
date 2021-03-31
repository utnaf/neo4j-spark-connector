package org.neo4j.spark.stream

import org.apache.spark.sql.execution.streaming.SerializedOffset
import org.apache.spark.sql.sources.v2.reader.streaming.Offset
import org.json4s.DefaultFormats

class Neo4jOffset(val offset: Long) extends Offset {
  implicit val defaultFormats: DefaultFormats = DefaultFormats
  override val json: String = offset.toString

  def +(increment: Long): Neo4jOffset = new Neo4jOffset(offset + increment)
  def -(decrement: Long): Neo4jOffset = new Neo4jOffset(offset - decrement)
}

object Neo4jOffset {
  def apply(offset: SerializedOffset) : Neo4jOffset = new Neo4jOffset(offset.json.toLong)

  def convert(offset: Offset): Option[Neo4jOffset] = offset match {
    case lo: Neo4jOffset => Some(lo)
    case so: SerializedOffset => Some(Neo4jOffset(so))
    case _ => None
  }
}