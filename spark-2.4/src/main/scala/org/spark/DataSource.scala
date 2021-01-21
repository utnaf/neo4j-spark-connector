package org.spark

import java.util.{Optional, UUID}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.neo4j.util.Neo4jOptions
import org.spark.reader.Neo4jDataSourceReader
import org.spark.writer.Neo4jDataSourceWriter

class DataSource extends DataSourceV2 with ReadSupport with DataSourceRegister with WriteSupport {

  private val jobId: String = UUID.randomUUID().toString

  def createReader(options: DataSourceOptions) = new Neo4jDataSourceReader(options, jobId)

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
}