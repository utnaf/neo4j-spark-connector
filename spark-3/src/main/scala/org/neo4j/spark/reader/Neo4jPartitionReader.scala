package org.neo4j.spark.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.{Neo4jQueryReadStrategy, PartitionSkipLimit}
import org.neo4j.spark.util.Neo4jOptions

class Neo4jPartitionReader(private val options: Neo4jOptions,
                                private val schema: StructType,
                                private val jobId: String,
                                private val partitionSkipLimit: PartitionSkipLimit,
                                private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
                                private val requiredColumns: StructType,
                                private val readStrategy: Neo4jQueryReadStrategy)
  extends BasePartitionReader(options, schema, jobId, partitionSkipLimit, scriptResult, requiredColumns, readStrategy, new java.util.HashMap[String, Object](), null)
  with PartitionReader[InternalRow]