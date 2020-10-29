package org.neo4j.spark.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.{AccessMode, Session}
import org.neo4j.spark.service.{Neo4jQueryStrategy, SchemaService}
import org.neo4j.spark.util.Neo4jImplicits.StructTypeImplicit
import org.neo4j.spark._

object Validations extends Logging {

  val writer: (Neo4jOptions, String, SaveMode) => Unit = { (neo4jOptions, jobId, saveMode) =>
    ValidationUtil.isFalse(neo4jOptions.session.accessMode == AccessMode.READ,
      s"Mode READ not supported for Data Source writer")
    val cache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, cache)
    try {
      validateConnection(cache.getOrCreate().session(neo4jOptions.session.toNeo4jSession))
      neo4jOptions.query.queryType match {
        case QueryType.QUERY => {
          ValidationUtil.isTrue(schemaService.isValidQuery(
            s"""WITH {} AS ${Neo4jQueryStrategy.VARIABLE_EVENT}, [] as ${Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT}
               |${neo4jOptions.query.value}
               |""".stripMargin,
            org.neo4j.driver.summary.QueryType.WRITE_ONLY, org.neo4j.driver.summary.QueryType.READ_WRITE),
            "Please provide a valid WRITE query")
          neo4jOptions.schemaMetadata.optimizationType match {
            case OptimizationType.NONE => // are valid
            case _ => ValidationUtil.isNotValid(
              s"""With Query Type ${neo4jOptions.query.queryType} you can
                 |only use `${Neo4jOptions.SCHEMA_OPTIMIZATION_TYPE}`
                 |`${OptimizationType.NONE}`
                 |""".stripMargin)
          }
        }
        case QueryType.LABELS => {
          saveMode match {
            case SaveMode.Overwrite => {
              ValidationUtil.isNotEmpty(neo4jOptions.nodeMetadata.nodeKeys,
                s"${Neo4jOptions.NODE_KEYS} is required when Save Mode is Overwrite")
            }
            case _ => Unit
          }
        }
        case QueryType.RELATIONSHIP => {
          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.target.labels,
            s"${Neo4jOptions.RELATIONSHIP_SOURCE_LABELS} is required when Save Mode is Overwrite")
          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.target.labels,
            s"${Neo4jOptions.RELATIONSHIP_TARGET_LABELS} is required when Save Mode is Overwrite")
        }
      }
      neo4jOptions.schemaMetadata.optimizationType match {
        case OptimizationType.NONE => // skip it
        case _ => neo4jOptions.query.queryType match {
          case QueryType.LABELS => ValidationUtil.isTrue(saveMode == SaveMode.Overwrite, "This works only with `mode` `SaveMode.Overwrite`")
          case QueryType.RELATIONSHIP => {
            ValidationUtil.isTrue(neo4jOptions.relationshipMetadata.sourceSaveMode == NodeSaveMode.Overwrite,
              s"This works only with `${Neo4jOptions.RELATIONSHIP_SOURCE_SAVE_MODE}` `Overwrite`")
            ValidationUtil.isTrue(neo4jOptions.relationshipMetadata.targetSaveMode == NodeSaveMode.Overwrite,
              s"This works only with `${Neo4jOptions.RELATIONSHIP_TARGET_SAVE_MODE}` `Overwrite`")
          }
        }
      }
      neo4jOptions.script.foreach(query => ValidationUtil.isTrue(schemaService.isValidQuery(query),
        s"The following query inside the `${Neo4jOptions.SCRIPT}` is not valid, please check the syntax: $query"))
    } finally {
      schemaService.close()
      cache.close()
    }
  }

  val schemaOptions: (Neo4jOptions, StructType) => Unit = { (neo4jOptions, schema) =>
    val missingFieldsMap = Map(
      Neo4jOptions.NODE_KEYS -> schema.getMissingFields(neo4jOptions.nodeMetadata.nodeKeys.keySet),
      Neo4jOptions.NODE_PROPS -> schema.getMissingFields(neo4jOptions.nodeMetadata.nodeProps.keySet),
      Neo4jOptions.RELATIONSHIP_PROPERTIES -> schema.getMissingFields(neo4jOptions.relationshipMetadata.properties.keySet),
      Neo4jOptions.RELATIONSHIP_SOURCE_NODE_PROPS -> schema.getMissingFields(neo4jOptions.relationshipMetadata.source.nodeProps.keySet),
      Neo4jOptions.RELATIONSHIP_SOURCE_NODE_KEYS -> schema.getMissingFields(neo4jOptions.relationshipMetadata.source.nodeKeys.keySet),
      Neo4jOptions.RELATIONSHIP_TARGET_NODE_PROPS -> schema.getMissingFields(neo4jOptions.relationshipMetadata.target.nodeProps.keySet),
      Neo4jOptions.RELATIONSHIP_TARGET_NODE_KEYS -> schema.getMissingFields(neo4jOptions.relationshipMetadata.target.nodeKeys.keySet)
    )

    val optionsWithMissingFields = missingFieldsMap.filter(_._2.nonEmpty)

    if (optionsWithMissingFields.nonEmpty) {
      throw new IllegalArgumentException(
        s"""Write failed due to the following errors:
           |${optionsWithMissingFields.map(field => s" - Schema is missing ${field._2.mkString(", ")} from option `${field._1}`").mkString("\n")}
           |
           |The option key and value might be inverted.""".stripMargin)
    }
  }

  val read: (Neo4jOptions, String) => Unit = { (neo4jOptions, jobId) =>
    val cache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, cache)
    try {
      validateConnection(cache.getOrCreate().session(neo4jOptions.session.toNeo4jSession))
      neo4jOptions.query.queryType match {
        case QueryType.LABELS => {
          ValidationUtil.isNotEmpty(neo4jOptions.nodeMetadata.labels,
            s"You need to set the ${QueryType.LABELS.toString.toLowerCase} option")
        }
        case QueryType.RELATIONSHIP => {
          ValidationUtil.isNotBlank(neo4jOptions.relationshipMetadata.relationshipType,
            s"You need to set the ${QueryType.RELATIONSHIP.toString.toLowerCase} option")

          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.source.labels,
            s"You need to set the ${Neo4jOptions.RELATIONSHIP_SOURCE_LABELS} option")

          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.target.labels,
            s"You need to set the ${Neo4jOptions.RELATIONSHIP_TARGET_LABELS} option")
        }
        case QueryType.QUERY => {
          ValidationUtil.isFalse(neo4jOptions.query.value.matches("(?si).*(LIMIT \\d+|SKIP ?\\d+)\\s*\\z"),
            "SKIP/LIMIT are not allowed at the end of the query")
          ValidationUtil.isTrue(schemaService.isValidQuery(s"""WITH [] as ${Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT}
                  |${neo4jOptions.query.value}
                  |""".stripMargin, org.neo4j.driver.summary.QueryType.READ_ONLY),
            "Please provide a valid READ query")
          if (neo4jOptions.queryMetadata.queryCount.nonEmpty) {
            if (!Neo4jUtil.isLong(neo4jOptions.queryMetadata.queryCount)) {
              ValidationUtil.isTrue(schemaService.isValidQueryCount(neo4jOptions.queryMetadata.queryCount),
                "Please provide a valid READ query count")
            }
          }
        }
      }
      neo4jOptions.script.foreach(query => ValidationUtil.isTrue(schemaService.isValidQuery(query),
        s"The following query inside the `${Neo4jOptions.SCRIPT}` is not valid, please check the syntax: $query"))
    } finally {
      schemaService.close()
      cache.close()
    }
  }

  def checkOptionsConsistency(options: java.util.Map[String, String]): Unit = {
    Seq(
      options.containsKey(QueryType.LABELS.toString.toLowerCase),
      options.containsKey(QueryType.RELATIONSHIP.toString.toLowerCase),
      options.containsKey(QueryType.QUERY.toString.toLowerCase)
    ).count(_ == true) match {
      case 0 => throw new IllegalArgumentException("No valid option found. One of `query`, `labels`, `relationship` is required.")
      case x if x > 1 => throw new IllegalArgumentException("You must specify just one of `query`, `labels`, `relationship`.")
      case _ => logInfo("Options are ok")
    }

//    neo4jOptions.query.queryType match {
//      case QueryType.LABELS => {
//        if (!neo4jOptions.query.value.isEmpty) {
//          ignoreOption(QueryType.QUERY.toString.toLowerCase, QueryType.LABELS.toString.toLowerCase)
//        }
//        if (!neo4jOptions.relationshipMetadata.relationshipType.isEmpty) {
//          ignoreOption(QueryType.RELATIONSHIP.toString.toLowerCase, QueryType.LABELS.toString.toLowerCase)
//        }
//        if(!neo4jOptions.queryMetadata.queryCount.isEmpty) {
//          ignoreOption(Neo4jOptions.QUERY_COUNT, QueryType.LABELS.toString.toLowerCase)
//        }
//      }
//    }
  }

  def ignoreOption(ignoredOption: String, primaryOption: String): Unit =
    logWarning(s"Option `$ignoredOption` is not compatible with `$primaryOption` and will be ignored")

  def validateConnection(session: Session): Unit = {
    try {
      session.run("EXPLAIN RETURN 1").consume()
    }
    finally {
      session.close()
    }
  }
}
