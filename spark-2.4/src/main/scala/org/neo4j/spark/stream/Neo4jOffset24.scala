package org.neo4j.spark.stream

import org.apache.spark.sql.sources.v2.reader.streaming.Offset

case class Neo4jOffset24(offset: Long) extends Offset {
  override def json(): String = offset.toString
}
