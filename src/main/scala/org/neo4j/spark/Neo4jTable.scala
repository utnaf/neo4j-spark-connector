package org.neo4j.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.neo4j.spark.reader.SimpleScanBuilder
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Validations}
import org.neo4j.spark.writer.Neo4jWriterBuilder

import scala.collection.JavaConverters._

class Neo4jTable(neo4jOptions: Neo4jOptions, jobId: String) extends Table
  with SupportsRead
  with SupportsWrite
  with Logging {

  val isRead: Boolean = try {
    Validations.read(neo4jOptions, jobId)
    true
  } catch {
    case _ => logInfo("Options for read are failing, assuming is a Write query")
      false
  }

  override def name(): String = this.getClass.toString

  override def schema(): StructType = if (isRead) {
    callSchemaService { schemaService => schemaService.struct() }
  } else {
    new StructType()
  }

  private def callSchemaService[T](function: SchemaService => T): T = {
    val driverCache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, driverCache)
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

  override def newScanBuilder(options: CaseInsensitiveStringMap): SimpleScanBuilder = {
    val validOptions = neo4jOptions.validate(neo4jOptions => Validations.read(neo4jOptions, jobId))
    new SimpleScanBuilder(validOptions, jobId, schema())
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new Neo4jWriterBuilder(jobId, info.schema(), SaveMode.Append, neo4jOptions)
  }

}
