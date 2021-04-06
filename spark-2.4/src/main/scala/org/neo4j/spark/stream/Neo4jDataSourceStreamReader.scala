package org.neo4j.spark.stream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.reader.Neo4jInputPartitionReader
import org.neo4j.spark.service.{PartitionSkipLimit, SchemaService}
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Validations}

import java.util
import java.util.Optional
import scala.collection.JavaConverters._

class Neo4jDataSourceStreamReader(private val options: DataSourceOptions, private val jobId: String)
  extends MicroBatchReader
    with Logging {

  private val BATCH_SIZE = 100;

  private var startOffset: Neo4jOffset = new Neo4jOffset(-1)
  private var endOffset: Neo4jOffset = new Neo4jOffset(-1)

  private var partitionReader: Neo4jInputPartitionReader = _

  private var stopped: Boolean = false

  private val neo4jOptions: Neo4jOptions = new Neo4jOptions(options.asMap())
    .validate(options => Validations.read(options, jobId))

  private val structType = callSchemaService { schemaService =>
    schemaService
      .struct()
  }

  private var lastCommittedOffset = new Neo4jOffset(-1)

  private def countQuery: Long = callSchemaService { schemaService => schemaService.count() }

  override def readSchema(): StructType = structType

  private def callSchemaService[T](function: SchemaService => T): T = {
    val driverCache = new DriverCache(neo4jOptions.connection, jobId)
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

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    this.startOffset = start.orElse(lastCommittedOffset + 1).asInstanceOf[Neo4jOffset]
    this.endOffset = end.orElse(new Neo4jOffset(Math.min(countQuery, BATCH_SIZE))).asInstanceOf[Neo4jOffset]
  }

  override def getStartOffset: Offset = {
    if (startOffset.offset == -1) {
      throw new IllegalStateException("startOffset is -1")
    }
    startOffset
  }

  override def getEndOffset: Offset = {
    if (endOffset.offset == -1) {
      throw new IllegalStateException("endOffset is -1")
    }
    endOffset
  }

  override def deserializeOffset(json: String): Offset = new Neo4jOffset(json.toLong)

  override def commit(end: Offset): Unit = {
    lastCommittedOffset = end.asInstanceOf[Neo4jOffset]
    logInfo(s"+++ committed with offset ${end}")
  }

  override def planInputPartitions: util.ArrayList[InputPartition[InternalRow]] = {
    val startOrdinal = startOffset.offset.toInt
    val endOrdinal = endOffset.offset.toInt - startOrdinal

    logInfo(s"+++ createDataReaderFactories: sOrd: $startOrdinal, eOrd: $endOrdinal")

    val partitionSkipLimit = synchronized {
      val sliceStart = startOrdinal
      val sliceEnd = endOrdinal + 1
      new PartitionSkipLimit(0, sliceStart, sliceEnd)
    }

    val schema = readSchema()

    partitionReader = new Neo4jInputPartitionReader(
      neo4jOptions,
      Array(),
      schema,
      jobId,
      partitionSkipLimit,
      new util.ArrayList[util.Map[String, AnyRef]](),
      new StructType())

    new util.ArrayList[InputPartition[InternalRow]](Seq(partitionReader).asJava)
  }

  override def stop(): Unit = {
    logWarning(s"Stop()")
    stopped = true
  }
}
