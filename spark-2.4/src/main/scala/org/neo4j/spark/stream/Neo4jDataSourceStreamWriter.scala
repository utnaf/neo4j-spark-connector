package org.neo4j.spark.stream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.AccessMode
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.{DriverCache, Neo4jOptions}
import org.neo4j.spark.writer.Neo4jDataWriterFactory

class Neo4jDataSourceStreamWriter(private val jobId: String,
                                  private val queryId: String,
                                  private val schema: StructType,
                                  private val options: DataSourceOptions)
  extends StreamWriter
    with Logging {

  private val optionsMap = options.asMap()
  optionsMap.put(Neo4jOptions.ACCESS_MODE, AccessMode.WRITE.toString)
  val neo4jOptions = new Neo4jOptions(optionsMap)

  var driverCache = new DriverCache(neo4jOptions.connection, jobId)

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

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = driverCache.close()

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = driverCache.close()

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
      logInfo(s"+++ executing query ${queryId}")
      val scriptResult = callSchemaService { schemaService => {
        schemaService.createOptimizations()
        schemaService.execute(neo4jOptions.script)
      }}
      new Neo4jDataWriterFactory(jobId, schema, SaveMode.Append, neo4jOptions, scriptResult)
  }
}
