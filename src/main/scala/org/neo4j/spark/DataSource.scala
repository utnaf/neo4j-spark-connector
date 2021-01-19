package org.neo4j.spark

import java.util.UUID
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.Validations.validateConnection
import org.neo4j.spark.util.{DriverCache, Neo4jOptions}

class DataSource extends TableProvider with DataSourceRegister {

  private val jobId: String = UUID.randomUUID().toString

  private var schema: StructType = null

  private var neo4jOptions: Neo4jOptions = null

  private def callSchemaService[T](neo4jOptions: Neo4jOptions, function: SchemaService => T): T = {
    val driverCache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    try {
      validateConnection(driverCache.getOrCreate().session(neo4jOptions.session.toNeo4jSession))
      function(schemaService)
    } catch {
      case e: Throwable =>
        throw e
    } finally {
      schemaService.close()
      driverCache.close()
    }
  }

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    if (schema == null) {
      schema = callSchemaService(getNeo4jOptions(caseInsensitiveStringMap),  { schemaService => schemaService.struct() })
    }

    schema
  }

  private def getNeo4jOptions(caseInsensitiveStringMap: CaseInsensitiveStringMap) = {
    if(neo4jOptions == null) {
      neo4jOptions = new Neo4jOptions(caseInsensitiveStringMap.asCaseSensitiveMap())
    }

    neo4jOptions
  }

  override def getTable(structType: StructType, transforms: Array[Transform], map: java.util.Map[String, String]): Table = {
    val caseInsensitiveStringMapNeo4jOptions = new CaseInsensitiveStringMap(map);
    new Neo4jTable(inferSchema(caseInsensitiveStringMapNeo4jOptions), getNeo4jOptions(caseInsensitiveStringMapNeo4jOptions), jobId)
  }

  override def shortName(): String = "neo4j"
}
