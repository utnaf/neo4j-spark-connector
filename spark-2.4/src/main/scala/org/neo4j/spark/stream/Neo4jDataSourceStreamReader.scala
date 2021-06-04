package org.neo4j.spark.stream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, SupportsPushDownFilters}
import org.apache.spark.sql.sources.{Filter, GreaterThan, GreaterThanOrEqual, LessThanOrEqual}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.reader.Neo4jInputPartition
import org.neo4j.spark.service.{Neo4jQueryStreamReadStrategy, PartitionSkipLimit, SchemaService}
import org.neo4j.spark.util.{DriverCache, LastTimestampCache, Neo4jOptions, Validations}

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util
import java.util.Optional
import scala.collection.JavaConverters._

class Neo4jDataSourceStreamReader(private val options: DataSourceOptions, private val schema: StructType, private val jobId: String)
  extends MicroBatchReader
    with SupportsPushDownFilters
    with Logging {

  private val neo4jOptions: Neo4jOptions = new Neo4jOptions(options.asMap())
    .validate(options => Validations.read(options, jobId))

  private var filters: Array[Filter] = Array[Filter]()

  private var startOffset: Neo4jOffset = new Neo4jOffset(LocalDateTime.MIN)

  private val streamingStartOffset: Neo4jOffset = new Neo4jOffset(LocalDateTime.now())

  private var endOffset: Neo4jOffset = new Neo4jOffset(LocalDateTime.now())

  private var gotAll = !neo4jOptions.streamingGetAll

  private val lastTimestampCache = new LastTimestampCache

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

  override def readSchema(): StructType = schema

  private val driverCache = new DriverCache(neo4jOptions.connection, jobId)

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    this.startOffset = if (neo4jOptions.streamingGetAll && !gotAll) {
      start.orElse(this.streamingStartOffset).asInstanceOf[Neo4jOffset]
    }
    else {
      new Neo4jOffset(this.lastTimestampCache.get(jobId).getOrElse(LocalDateTime.now()))
    }
    this.endOffset = new Neo4jOffset(LocalDateTime.now())
  }

  override def getStartOffset: Offset = startOffset

  override def getEndOffset: Offset = endOffset

  override def deserializeOffset(json: String): Offset = new Neo4jOffset(LocalDateTime.parse(json))

  override def commit(end: Offset): Unit = {
    println(s"+++ $end")
  }

  override def planInputPartitions: util.ArrayList[InputPartition[InternalRow]] = {
    this.endOffset = new Neo4jOffset(LocalDateTime.now())

    val partitionSkipLimit = PartitionSkipLimit.EMPTY

    var filtersWithTimestamp = filters

    if (!gotAll) {
      gotAll = true
      filtersWithTimestamp = filters :+ LessThanOrEqual(
        neo4jOptions.streamingTimestampProperty,
        Timestamp.valueOf(endOffset.offset)
      )
    }
    else {
      filtersWithTimestamp = filters :+ GreaterThan(
        neo4jOptions.streamingTimestampProperty,
        Timestamp.valueOf(startOffset.offset)
      )
    }

    val eventsParams: java.util.Map[String, Object] = new java.util.HashMap[String, Object]()
    eventsParams.put("fromTimestamp", startOffset.offset)

    val reader = new Neo4jInputPartition(
      neo4jOptions,
      schema,
      jobId,
      partitionSkipLimit,
      new util.ArrayList[util.Map[String, AnyRef]](),
      new StructType(),
      new Neo4jQueryStreamReadStrategy(filtersWithTimestamp, partitionSkipLimit, Seq()),
      lastTimestampCache,
      eventsParams
    )
    new util.ArrayList[InputPartition[InternalRow]](Seq(reader).asJava)
  }

  override def stop(): Unit = {
    driverCache.close()
  }

  override def pushFilters(filtersArray: Array[Filter]): Array[Filter] = {
    if (neo4jOptions.pushdownFiltersEnabled) {
      filters = filtersArray
    }

    filtersArray
  }

  override def pushedFilters(): Array[Filter] = filters
}