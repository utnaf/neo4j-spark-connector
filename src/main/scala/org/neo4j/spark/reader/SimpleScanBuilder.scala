package org.neo4j.spark.reader

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.util.{Neo4jOptions, Validations}

class SimpleScanBuilder(neo4jOptions: Neo4jOptions, jobId: String, schema: StructType) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  val validOptions: Neo4jOptions = {
    neo4jOptions.validate(neo4jOptions => Validations.read(neo4jOptions, jobId))
  }

  private var filters: Array[Filter] = Array[Filter]()

  private var requiredColumns: StructType = new StructType()

  override def build(): Scan = {
    new SimpleScan(validOptions, jobId, schema, filters, requiredColumns)
  }

  override def pushFilters(filtersArray: Array[Filter]): Array[Filter] = {
    if (neo4jOptions.pushdownFiltersEnabled) {
      filters = filtersArray
    }

    filtersArray
  }

  override def pushedFilters(): Array[Filter] = filters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    requiredColumns = if (
      !neo4jOptions.pushdownColumnsEnabled || neo4jOptions.relationshipMetadata.nodeMap
      || requiredSchema == schema
    ) {
      new StructType()
    } else {
      requiredSchema
    }
  }
}
