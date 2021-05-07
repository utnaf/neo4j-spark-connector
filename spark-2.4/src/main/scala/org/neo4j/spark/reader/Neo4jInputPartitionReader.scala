package org.neo4j.spark.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.{Neo4jQueryReadStrategy, PartitionSkipLimit}
import org.neo4j.spark.util.Neo4jOptions

class Neo4jInputPartitionReader(private val options: Neo4jOptions,
                                private val filters: Array[Filter],
                                private val schema: StructType,
                                private val jobId: String,
                                private val partitionSkipLimit: PartitionSkipLimit,
                                private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
                                private val requiredColumns: StructType,
                                private val readStrategy: Neo4jQueryReadStrategy)
  extends BasePartitionReader(options, filters, schema, jobId, partitionSkipLimit, scriptResult, requiredColumns, readStrategy)
    with InputPartitionReader[InternalRow]