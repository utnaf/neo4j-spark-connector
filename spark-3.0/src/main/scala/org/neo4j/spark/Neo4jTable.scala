package org.neo4j.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.neo4j.spark.reader.SimpleScanBuilder
import org.neo4j.spark.util.{Neo4jOptions, Validations}
import org.neo4j.spark.writer.Neo4jWriterBuilder

import scala.collection.JavaConverters._

class Neo4jTable(schema: StructType, neo4jOptions: Neo4jOptions, jobId: String) extends Table
  with SupportsRead
  with SupportsWrite
  with Logging {

  /**
   * @todo make the name better based on what we are going to read/write
   *       ex:
   *       * table_User-Admin
   *       * table_User_BOUGHT_Product
   */
  override def name(): String = "neo4j"

  override def schema(): StructType = schema

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
