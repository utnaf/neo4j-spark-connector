package org.neo4j.spark.reader

import java.util
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.{Neo4jQueryReadStrategy, SchemaService}
import org.neo4j.spark.util.Neo4jImplicits.StructTypeImplicit
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Validations}

import scala.collection.JavaConverters._

class Neo4jDataSourceReader(private val options: DataSourceOptions, private val jobId: String, private val userDefinedSchema: StructType = null) extends DataSourceReader
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var filters: Array[Filter] = Array[Filter]()

  private var requiredColumns: StructType = new StructType()

  private val neo4jOptions: Neo4jOptions = new Neo4jOptions(options.asMap())
    .validate(options => Validations.read(options, jobId))

  private val structType = if (userDefinedSchema != null) {
    userDefinedSchema
  } else {
    callSchemaService { schemaService => schemaService
      .struct() }
  }

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

  override def planInputPartitions: util.ArrayList[InputPartition[InternalRow]] = {
    // we retrieve the schema in order to parse the data correctly
    val schema = readSchema()
    val neo4jPartitions: Seq[Neo4jInputPartition] = createPartitions(schema)
    new util.ArrayList[InputPartition[InternalRow]](neo4jPartitions.asJava)
  }

  private def createPartitions(schema: StructType) = {
    // we get the skip/limit for each partition and execute the "script"
    val (partitionSkipLimitList, scriptResult) = callSchemaService { schemaService =>
      (schemaService.skipLimitFromPartition(), schemaService.execute(neo4jOptions.script)) }
    // we generate a partition for each element
    partitionSkipLimitList
      .map(partitionSkipLimit => new Neo4jInputPartition(neo4jOptions, filters, schema, jobId,
        partitionSkipLimit, scriptResult, requiredColumns, new Neo4jQueryReadStrategy(filters, partitionSkipLimit, requiredColumns.fields.map(_.name).toSeq)))
  }

  override def pushFilters(filtersArray: Array[Filter]): Array[Filter] = {
    if (neo4jOptions.pushdownFiltersEnabled) {
      filters = filtersArray
    }

    filtersArray
  }

  override def pushedFilters(): Array[Filter] = filters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    requiredColumns = if (!neo4jOptions.pushdownColumnsEnabled || neo4jOptions.relationshipMetadata.nodeMap) {
      new StructType()
    } else {
      requiredSchema
    }
  }
}