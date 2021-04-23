package org.neo4j.spark

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.reader.Neo4jDataSourceReader
import org.neo4j.spark.stream.Neo4jDataSourceStreamWriter
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.stream.Neo4jDataSourceStreamReader
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Validations}
import org.neo4j.spark.writer.Neo4jDataSourceWriter

import java.util.concurrent.ConcurrentHashMap
import java.util.{Optional, UUID}

class DataSource extends DataSourceV2
  with StreamWriteSupport
  with MicroBatchReadSupport
  with ReadSupport
  with DataSourceRegister
  with WriteSupport {

  Validations.version("2.4.*")

  private val dataSourceStreamReaderCache: ConcurrentHashMap[String, Neo4jDataSourceStreamReader] = new ConcurrentHashMap()

  private val jobId: String = UUID.randomUUID().toString

  def createReader(options: DataSourceOptions) = new Neo4jDataSourceReader(options, jobId)

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = new Neo4jDataSourceReader(options, jobId, schema)

  override def shortName: String = "neo4j"

  override def createWriter(jobId: String,
                            structType: StructType,
                            saveMode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] = {
    Validations.supportedSaveMode(saveMode.toString)
    Optional.of(new Neo4jDataSourceWriter(jobId, structType, saveMode, options))
  }

  protected def callSchemaService[T](function: SchemaService => T, neo4jOptions: Neo4jOptions): T = {
    val localDriverCache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, localDriverCache)
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
        localDriverCache.close()
      }
    }
  }

  override def createMicroBatchReader(schema: Optional[StructType], s: String, dataSourceOptions: DataSourceOptions): MicroBatchReader = {
    new Neo4jDataSourceStreamReader(dataSourceOptions,
      schema.orElse(
        callSchemaService({ schemaService => schemaService.struct() }, new Neo4jOptions(dataSourceOptions.asMap()))
      ), jobId)
  }

  @volatile
  private var streamWriter: Neo4jDataSourceStreamWriter = null

  def isNewInstance(queryId: String,
                    schema: StructType,
                    options: DataSourceOptions): Boolean = (streamWriter == null ||
    streamWriter.queryId != queryId ||
    streamWriter.schema != schema ||
    streamWriter.options != options)

  override def createStreamWriter(queryId: String, schema: StructType, mode: OutputMode, options: DataSourceOptions): StreamWriter = {
    val streamingSaveMode = options.get(Neo4jOptions.SAVE_MODE)
      .orElse(Neo4jOptions.DEFAULT_SAVE_MODE.toString)
    Validations.supportedSaveMode(streamingSaveMode)
    if (isNewInstance(queryId, schema, options)) {
      streamWriter = new Neo4jDataSourceStreamWriter(queryId, schema, options, SaveMode.valueOf(streamingSaveMode))
    }
    streamWriter
  }
}