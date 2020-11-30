package org.neo4j.spark.reader

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.neo4j.spark.Neo4jOptions

class SimpleScanBuilder(neo4jOptions: Neo4jOptions, jobId: String) extends ScanBuilder {
  override def build(): Scan = new SimpleScan(neo4jOptions, jobId)
}
