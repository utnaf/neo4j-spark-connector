package org.neo4j.spark.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.{BatchWrite, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.Neo4jOptions

class Neo4jWriterBuilder(jobId: String,
                         structType: StructType,
                         saveMode: SaveMode,
                         options: Neo4jOptions) extends WriteBuilder{

  override def buildForBatch(): BatchWrite = new Neo4jBatchWriter(jobId,
    structType,
    saveMode,
    options)
}
