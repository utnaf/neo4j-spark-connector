package org.neo4j.spark

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsOverwrite, WriteBuilder}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.neo4j.spark.reader.SimpleScanBuilder
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Validations}
import org.neo4j.spark.writer.Neo4jWriterBuilder

import scala.collection.JavaConverters._

class Neo4jTable(neo4jOptions: Neo4jOptions, jobId: String) extends Table
  with SupportsRead
  with SupportsWrite{

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

  override def capabilities(): java.util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): SimpleScanBuilder = new SimpleScanBuilder(validOptions, jobId, schema())

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new Neo4jWriterBuilder(jobId, info.schema(), SaveMode.Append, validOptions)
  }
}
