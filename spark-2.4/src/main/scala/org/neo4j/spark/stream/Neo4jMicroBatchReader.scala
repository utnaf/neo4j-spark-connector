package org.neo4j.spark.stream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{Filter, GreaterThan}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, SupportsPushDownFilters}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.reader.Neo4jInputPartition
import org.neo4j.spark.service
import org.neo4j.spark.service.{PartitionSkipLimit, SchemaService}
import org.neo4j.spark.streaming.{Neo4jOffset, OffsetStorage}
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Neo4jUtil, QueryType, Validations}

import java.time.LocalDateTime
import java.util
import java.util.function.Supplier
import java.util.{Collections, Optional}
import scala.collection.JavaConverters._

class Neo4jMicroBatchReader(private val optionalSchema: Optional[StructType],
                            private val options: DataSourceOptions,
                            private val jobId: String)
  extends MicroBatchReader
    with SupportsPushDownFilters
    with Logging {

  private val neo4jOptions: Neo4jOptions = new Neo4jOptions(options.asMap())
    .validate(options => Validations.read(options, jobId))

  private var filters: Array[Filter] = Array[Filter]()

  private var startOffset: Neo4jOffset = null

  private var endOffset: Neo4jOffset = null

  private val driverCache = new DriverCache(neo4jOptions.connection, jobId)

  protected def callSchemaService[T](function: SchemaService => T): T = {
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    var hasError = false
    try {
      function(schemaService)
    } catch {
      case e: Throwable => {
        hasError = true
        throw e
      }
    } finally {
      schemaService.close()
      if (hasError) {
        driverCache.close()
      }
    }
  }

  private val schema = optionalSchema.orElseGet(new Supplier[StructType] {
    override def get(): StructType = callSchemaService({ schemaService => schemaService.struct() })
  })

  override def readSchema(): StructType = schema

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    val lastOffset = OffsetStorage.getLastOffset(jobId)
    val currOffset: Long = try {
      callSchemaService({schemaService => schemaService.lastOffset() })
        .asInstanceOf[Long]
    } catch {
      case e: Throwable => -1
    }
    if (lastOffset == null || lastOffset.offset < currOffset) {
      startOffset = start.orElseGet(new Supplier[Offset] {
        override def get(): Offset = Neo4jOffset.from(neo4jOptions.streamingOptions.from)
      }).asInstanceOf[Neo4jOffset]
      endOffset = if (lastOffset == null) {
        new Neo4jOffset()
      } else {
        lastOffset
      }
    }
  }

  override def getStartOffset: Offset = startOffset

  override def getEndOffset: Offset = endOffset

  override def deserializeOffset(json: String): Offset = new Neo4jOffset(json.toLong)

  override def commit(end: Offset): Unit = {
    val lastOffset = OffsetStorage.getLastOffset(jobId)
    if (endOffset.offset < lastOffset.offset) {
      startOffset = endOffset
      endOffset = lastOffset
    }
  }

  override def planInputPartitions: util.ArrayList[InputPartition[InternalRow]] = {
    val (filters, partitions) = if (startOffset != Neo4jOffset.ALL) {
      val prop = Neo4jUtil.getStreamingPropertyName(neo4jOptions)
      (this.filters :+ GreaterThan(prop, endOffset.offset),
        Seq(PartitionSkipLimit.EMPTY))
    } else {
      (this.filters, callSchemaService({ schemaService => schemaService.skipLimitFromPartition() }))
    }
    val collection = partitions
      .map(partitionSkipLimit => new Neo4jStreamingInputPartition(neo4jOptions, filters, schema, jobId,
        partitionSkipLimit, Collections.emptyList(), new StructType()))
      .toList.asJavaCollection
    new util.ArrayList[InputPartition[InternalRow]](collection)
  }

  override def stop(): Unit = {
    driverCache.close()
    OffsetStorage.clearForJobId(jobId)
  }

  override def pushFilters(filtersArray: Array[Filter]): Array[Filter] = {
    if (neo4jOptions.pushdownFiltersEnabled) {
      filters = filtersArray
    }

    filtersArray
  }

  override def pushedFilters(): Array[Filter] = filters
}
