package org.spark.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType
import org.neo4j.util.Neo4jOptions

class Neo4jDataWriterFactory(jobId: String,
                             structType: StructType,
                             saveMode: SaveMode,
                             options: Neo4jOptions,
                             scriptResult: java.util.List[java.util.Map[String, AnyRef]]) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = new Neo4jDataWriter(jobId, partitionId, structType, saveMode, options, scriptResult)
}