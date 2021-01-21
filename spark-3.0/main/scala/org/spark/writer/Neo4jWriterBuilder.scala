package org.spark.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsOverwrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.AccessMode
import org.neo4j.util.{Neo4jOptions, Validations}

class Neo4jWriterBuilder(jobId: String,
                         structType: StructType,
                         saveMode: SaveMode,
                         neo4jOptions: Neo4jOptions) extends WriteBuilder
  with SupportsOverwrite
  with SupportsTruncate {

  val validOptions: Neo4jOptions = {
    neo4jOptions.session.accessMode = AccessMode.WRITE
    neo4jOptions.validate(neo4jOptions => Validations.writer(neo4jOptions, jobId, saveMode))
  }

  override def buildForBatch(): BatchWrite = new Neo4jBatchWriter(jobId,
    structType,
    saveMode,
    validOptions)

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    new Neo4jWriterBuilder(jobId, structType, SaveMode.Overwrite, validOptions)
  }
}
