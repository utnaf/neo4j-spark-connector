package org.spark.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, PhysicalWriteInfo}
import org.apache.spark.sql.types.StructType
import org.neo4j.util.Neo4jOptions

class Neo4jDataWriterFactory(jobId: String,
                             structType: StructType,
                             saveMode: SaveMode,
                             options: Neo4jOptions,
                             scriptResult: java.util.List[java.util.Map[String, AnyRef]]) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] = new Neo4jDataWriter(
    jobId,
    partitionId,
    structType,
    saveMode,
    options,
    scriptResult
  )
}
