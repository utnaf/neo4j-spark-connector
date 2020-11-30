package org.neo4j.spark.reader

import java.util.UUID

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.neo4j.spark.{DriverCache, Neo4jOptions}
import org.neo4j.spark.service.SchemaService

import scala.collection.JavaConverters._

class BatchTable(neo4jOptions: Neo4jOptions) extends Table with SupportsRead {
  override def name(): String = this.getClass.toString

  private val jobId: String = UUID.randomUUID().toString

  override def schema(): StructType = {
    val driverCache = new DriverCache(neo4jOptions.connection, jobId)
    new SchemaService(neo4jOptions, driverCache).struct()
  }

  override def capabilities(): java.util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): SimpleScanBuilder = new SimpleScanBuilder(neo4jOptions, jobId)
}
