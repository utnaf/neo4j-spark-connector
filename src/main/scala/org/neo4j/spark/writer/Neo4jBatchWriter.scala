package org.neo4j.spark.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.AccessMode
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Validations}

class Neo4jBatchWriter(jobId: String,
                       structType: StructType,
                       saveMode: SaveMode,
                       options: Neo4jOptions) extends BatchWrite{
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    schemaService.createOptimizations()
    val scriptResult = schemaService.execute(neo4jOptions.script)
    schemaService.close()

    new Neo4jDataWriterFactory(
      jobId,
      structType,
      saveMode,
      options,
      scriptResult
    )
  }

  private val neo4jOptions: Neo4jOptions = {
    options.session.accessMode = AccessMode.WRITE
    options.validate(neo4jOptions => Validations.writer(neo4jOptions, jobId, saveMode))
  }

  private val driverCache = new DriverCache(neo4jOptions.connection, jobId)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    driverCache.close()
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    driverCache.close()
  }
}