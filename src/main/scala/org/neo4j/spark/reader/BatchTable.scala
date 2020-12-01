package org.neo4j.spark.reader

import java.util.UUID

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.neo4j.spark.{DriverCache, Neo4jOptions}
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.Validations

import scala.collection.JavaConverters._

class BatchTable(neo4jOptions: Neo4jOptions, jobId: String) extends Table with SupportsRead {

  val validOptions: Neo4jOptions = neo4jOptions.validate(options => Validations.read(options, jobId))

  override def name(): String = this.getClass.toString

  override def schema(): StructType = structType

  private val structType = callSchemaService { schemaService => schemaService
    .struct() }

  private def callSchemaService[T](function: SchemaService => T): T = {
    val driverCache = new DriverCache(validOptions.connection, jobId)
    val schemaService = new SchemaService(validOptions, driverCache)
    var hasError = false
    try {
      function(schemaService)
    } catch {
      case e: Throwable =>
        hasError = true
        throw e
    } finally {
      schemaService.close()
      if (hasError) {
        driverCache.close()
      }
    }
  }

  override def capabilities(): java.util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): SimpleScanBuilder = new SimpleScanBuilder(validOptions, jobId, schema())
}
