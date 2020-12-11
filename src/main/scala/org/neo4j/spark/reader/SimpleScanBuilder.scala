package org.neo4j.spark.reader

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.AccessMode
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Validations}

class SimpleScanBuilder(neo4jOptions: Neo4jOptions, jobId: String, schema: StructType) extends ScanBuilder {

  val validOptions: Neo4jOptions = {
    neo4jOptions.validate(neo4jOptions => Validations.read(neo4jOptions, jobId))
  }

  override def build(): Scan = new SimpleScan(validOptions, jobId, schema)
}
