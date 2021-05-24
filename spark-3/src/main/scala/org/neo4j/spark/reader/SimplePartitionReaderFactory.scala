package org.neo4j.spark.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.{Neo4jQueryReadStrategy, PartitionSkipLimit}
import org.neo4j.spark.util.Neo4jOptions

class SimplePartitionReaderFactory(private val neo4jOptions: Neo4jOptions,
                                   private val filters: Array[Filter],
                                   private val schema: StructType,
                                   private val jobId: String,
                                   private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
                                   private val requiredColumns: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val partitionSkipLimit = partition.asInstanceOf[Neo4jPartition].partitionSkipLimit
    new Neo4jPartitionReader(
      neo4jOptions,
      schema,
      jobId,
      partitionSkipLimit,
      scriptResult,
      requiredColumns,
      new Neo4jQueryReadStrategy(filters, partitionSkipLimit, requiredColumns.fields.map(_.name).toSeq)
    )
  }
}
