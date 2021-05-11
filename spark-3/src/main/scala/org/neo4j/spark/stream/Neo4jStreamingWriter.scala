package org.neo4j.spark.stream

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.{DriverCache, Neo4jOptions}
import org.neo4j.spark.writer.Neo4jDataWriterFactory

class Neo4jStreamingWriter(jobId: String,
                           structType: StructType,
                           saveMode: SaveMode,
                           neo4jOptions: Neo4jOptions)
  extends StreamingWrite {

  private val driverCache = new DriverCache(neo4jOptions.connection, jobId)

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    schemaService.createOptimizations()
    val scriptResult = schemaService.execute(neo4jOptions.script)
    schemaService.close()

    new Neo4jStreamingDataWriterFactory(
      jobId,
      structType,
      saveMode,
      neo4jOptions,
      scriptResult
    )
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    driverCache.close()
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    driverCache.close()
  }
}