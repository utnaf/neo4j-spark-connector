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
import org.neo4j.spark.util.{Neo4jOptions, Validations}
import org.neo4j.spark.writer.Neo4jDataSourceWriter

import java.util.{Optional, UUID}

class DataSource extends DataSourceV2
  with StreamWriteSupport
  with ReadSupport
  with DataSourceRegister
  with WriteSupport {

  Validations.version("2.4.*")

  private val jobId: String = UUID.randomUUID().toString

  def createReader(options: DataSourceOptions) = new Neo4jDataSourceReader(options, jobId)

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = new Neo4jDataSourceReader(options, jobId, schema)

  override def shortName: String = "neo4j"

  override def createWriter(jobId: String,
                            structType: StructType,
                            saveMode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] =
    if (Neo4jOptions.SUPPORTED_SAVE_MODES.contains(saveMode)) {
      Optional.of(new Neo4jDataSourceWriter(jobId, structType, saveMode, options))
    } else {
      throw new IllegalArgumentException(
        s"""Unsupported SaveMode.
           |You provided $saveMode, supported are:
           |${Neo4jOptions.SUPPORTED_SAVE_MODES.mkString(",")}
           |""".stripMargin)
    }

  override def createStreamWriter(queryId: String, schema: StructType, mode: OutputMode, options: DataSourceOptions): StreamWriter = {
    new Neo4jDataSourceStreamWriter(jobId, queryId, schema, options)
  }
}