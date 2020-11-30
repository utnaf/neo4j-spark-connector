package org.neo4j.spark.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.Neo4jOptions
import org.neo4j.spark.service.PartitionSkipLimit

class SimplePartitionReaderFactory(private val neo4jOptions: Neo4jOptions,
                                   private val filters: Array[Filter],
                                   private val schema: StructType,
                                   private val jobId: String,
                                   private val partitionSkipLimit: PartitionSkipLimit,
                                   private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
                                   private val requiredColumns: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): InputPartition[InternalRow] = new Neo4jInputPartitionReader(
    neo4jOptions,
    filters,
    schema,
    jobId,
    partitionSkipLimit,
    scriptResult,
    requiredColumns
  )
}
