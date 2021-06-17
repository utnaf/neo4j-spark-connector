package org.neo4j.spark.stream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan, SupportsPushDownFilters}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit}
import org.apache.spark.sql.sources.{Filter, GreaterThan}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.reader.{Neo4jPartition, SimplePartitionReaderFactory}
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.streaming.OffsetStorage
import org.neo4j.spark.util._

import java.util.function.Supplier
import java.util.{Collections, Optional, function}
import java.{lang, util}
import scala.collection.JavaConverters._

class Neo4jMicroBatchReader(private val optionalSchema: Optional[StructType],
                            private val neo4jOptions: Neo4jOptions,
                            private val jobId: String)
  extends MicroBatchStream
    with Logging {

  private val driverCache = new DriverCache(neo4jOptions.connection, jobId)

  private lazy val scriptResult = {
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    schemaService.createOptimizations()
    val scriptResult = schemaService.execute(neo4jOptions.script)
    schemaService.close()
    scriptResult
  }

  private var filters: Array[Filter] = Array[Filter]()

  override def deserializeOffset(json: String): Offset = Neo4jOffset(json.toLong)

  override def commit(end: Offset): Unit = {
    println("+++ committing " + end)
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    this.filters = if (start.asInstanceOf[Neo4jOffset].offset != StreamingFrom.ALL.value()) {
      val prop = Neo4jUtil.getStreamingPropertyName(neo4jOptions)
      Array(GreaterThan(prop, latestOffset().asInstanceOf[Neo4jOffset].offset))
    }
    else {
      this.filters
    }

    val numPartitions = Neo4jUtil.callSchemaService(neo4jOptions, jobId, filters,
      { schemaService => schemaService.skipLimitFromPartition() })

    numPartitions
      .map(p => Neo4jStreamingPartition(p, filters))
      .toArray
  }

  override def stop(): Unit = {
    OffsetStorage.clearForJobId(jobId)
    new DriverCache(neo4jOptions.connection, jobId).close()
  }

  override def latestOffset(): Offset = {
    val lastOffset: lang.Long = OffsetStorage.getLastOffset(jobId)
    if (lastOffset == null) {
      initialOffset()
    }
    else {
      Neo4jOffset(lastOffset)
    }
  }

  override def initialOffset(): Offset = Neo4jOffset(neo4jOptions.streamingOptions.from.value())

  override def createReaderFactory(): PartitionReaderFactory = {
    new SimpleStreamingPartitionReaderFactory(
      neo4jOptions, optionalSchema.orElse(new StructType()), jobId, scriptResult
    )
  }
}
