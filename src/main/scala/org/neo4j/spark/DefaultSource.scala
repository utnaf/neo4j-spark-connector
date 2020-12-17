package org.neo4j.spark

import java.util.UUID
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.neo4j.spark.util.Neo4jOptions

class DefaultSource extends TableProvider with DataSourceRegister {

  private val jobId: String = UUID.randomUUID().toString

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    new StructType()
  }

  override def getTable(structType: StructType, transforms: Array[Transform], map: java.util.Map[String, String]): Table = {
    new Neo4jTable(new Neo4jOptions(map), jobId)
  }

  override def shortName(): String = "neo4j"
}
