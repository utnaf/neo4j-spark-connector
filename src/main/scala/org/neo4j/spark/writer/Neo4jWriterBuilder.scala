package org.neo4j.spark.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsOverwrite, WriteBuilder}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.util.Neo4jOptions

class Neo4jWriterBuilder(jobId: String,
                         structType: StructType,
                         saveMode: SaveMode,
                         options: Neo4jOptions) extends WriteBuilder
  with SupportsOverwrite {

  override def buildForBatch(): BatchWrite = new Neo4jBatchWriter(jobId,
    structType,
    saveMode,
    options)

  override def overwrite(filters: Array[Filter]): WriteBuilder = this
}
