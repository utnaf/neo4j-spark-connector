package org.neo4j.spark.stream

import org.apache.spark.sql.connector.read.streaming.Offset

case class Neo4jOffset(offset: Long) extends Offset {
  override def json(): String = offset.toString
}
