package org.neo4j.spark.reader

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.{DriverCache, Neo4jOptions}
import org.neo4j.spark.service.{PartitionSkipLimit, SchemaService}

import scala.collection.JavaConverters.seqAsJavaListConverter

class SimpleScan(neo4jOptions: Neo4jOptions, jobId: String) extends Scan with Batch{
  override def readSchema(): StructType = structType

  private val structType = callSchemaService { schemaService => schemaService
    .struct() }

  private def callSchemaService[T](function: SchemaService => T): T = {
    val driverCache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    var hasError = false
    try {
      function(schemaService)
    } catch {
      case e: Throwable => {
        hasError = true
        throw e
      }
    } finally {
      schemaService.close()
      if (hasError) {
        driverCache.close()
      }
    }
  }

  override def toBatch: Batch = this

  private def createPartitions(schema: StructType) = {
    // we get the skip/limit for each partition and execute the "script"
    val (partitionSkipLimitList, scriptResult) = callSchemaService { schemaService =>
      (schemaService.skipLimitFromPartition(), schemaService.execute(neo4jOptions.script)) }
    // we generate a partition for each element
    partitionSkipLimitList
      .map(partitionSkipLimit => new Neo4jInputPartitionReader(neo4jOptions, Array.empty, schema, jobId,
        partitionSkipLimit, scriptResult, new StructType()))
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val schema = readSchema()
    val neo4jPartitions: Seq[Neo4jInputPartitionReader] = createPartitions(schema)
    neo4jPartitions.toArray
  }
  override def createReaderFactory(): PartitionReaderFactory = new SimplePartitionReaderFactory(
    neo4jOptions, Array.empty[Filter], readSchema(), "123", PartitionSkipLimit.EMPTY, List.empty[java.util.Map[String, AnyRef]].asJava, null
  )
}
