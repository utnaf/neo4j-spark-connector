package org.neo4j.spark.service

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.Filter
import org.neo4j.cypherdsl.core.{Cypher, PropertyContainer}
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.spark.{Neo4jOptions, QueryType}
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.Neo4jUtil

import collection.JavaConverters._

class Neo4jQueryWriteStrategy(private val saveMode: SaveMode) extends Neo4jQueryStrategy {
  override def createStatementForQuery(options: Neo4jOptions): String =
    s"""UNWIND ${"$"}events AS event
       |${options.query.value}
       |""".stripMargin

  override def createStatementForRelationships(options: Neo4jOptions): String = throw new UnsupportedOperationException("TODO implement method")

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val keyword = saveMode match {
      case SaveMode.Overwrite => "MERGE"
      case SaveMode.ErrorIfExists => "CREATE"
      case _ => throw new UnsupportedOperationException(s"SaveMode $saveMode not supported")
    }
    val labels = options.nodeMetadata.labels
      .map(_.quote)
      .mkString(":")
    val keys = options.nodeMetadata.nodeKeys.keys
      .map(_.quote)
      .map(k => s"$k: event.keys.$k")
      .mkString(", ")
    s"""UNWIND ${"$"}events AS event
       |$keyword (node${if (labels.isEmpty) "" else s":$labels"} ${if (keys.isEmpty) "" else s"{$keys}"})
       |SET node += event.properties
       |""".stripMargin
  }
}

class Neo4jQueryReadStrategy(filters: Array[Filter]) extends Neo4jQueryStrategy {
  private val renderer: Renderer = Renderer.getDefaultRenderer

  override def createStatementForQuery(options: Neo4jOptions): String = options.query.value

  override def createStatementForRelationships(options: Neo4jOptions): String = {
    val sourcePrimaryLabel = options.relationshipMetadata.source.labels.head
    val sourceOtherLabels = options.relationshipMetadata.source.labels.takeRight(options.nodeMetadata.labels.size - 1)
    val sourceNode = Cypher.node(sourcePrimaryLabel, sourceOtherLabels.asJava).named(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS)

    val targetPrimaryLabel = options.relationshipMetadata.target.labels.head
    val targetOtherLabels = options.relationshipMetadata.target.labels.takeRight(options.nodeMetadata.labels.size - 1)
    val targetNode = Cypher.node(targetPrimaryLabel, targetOtherLabels.asJava).named(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS)

    val relationship = sourceNode.relationshipBetween(targetNode, options.relationshipMetadata.relationshipType)
      .named(Neo4jUtil.RELATIONSHIP_ALIAS)


    val matchQuery = Cypher.`match`(sourceNode).`match`(targetNode).`match`(relationship)

    if (filters.nonEmpty) {
      val filtersMap: Map[PropertyContainer, Array[Filter]] = filters.map(filter => {
        if (filter.isAttribute(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS)) {
          (sourceNode, filter)
        }
        else if (filter.isAttribute(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS)) {
          (targetNode, filter)
        }
        else if (filter.isAttribute(Neo4jUtil.RELATIONSHIP_ALIAS)) {
          (relationship, filter)
        }
        else {
          throw new IllegalArgumentException(s"Attribute '${filter.getAttribute.get}' is not valid")
        }
      }).groupBy[PropertyContainer](_._1).mapValues(_.map(_._2))

      matchQuery.where(
        filtersMap.flatMap(t => {
          val filters: Array[Filter] = t._2
          filters.map(filter => Neo4jUtil.mapSparkFiltersToCypher(
            filter,
            t._1,
            filter.getAttribute.map(_.split('.').drop(1).mkString("."))
          ))
        }).reduce { (a, b) => a.and(b) }
      )
    }

    renderer.render(matchQuery.returning(sourceNode, relationship, targetNode).build())
  }

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val primaryLabel = options.nodeMetadata.labels.head
    val otherLabels = options.nodeMetadata.labels.takeRight(options.nodeMetadata.labels.size - 1)
    val node = Cypher.node(primaryLabel, otherLabels.asJava).named(Neo4jUtil.NODE_ALIAS)
    val matchQuery = Cypher.`match`(node)

    if (filters.nonEmpty) {
      matchQuery.where(
        filters.map {
          Neo4jUtil.mapSparkFiltersToCypher(_, node)
        } reduce { (a, b) => a.and(b) }
      )
    }

    renderer.render(matchQuery.returning(node).build())
  }
}

abstract class Neo4jQueryStrategy {
  def createStatementForQuery(options: Neo4jOptions): String

  def createStatementForRelationships(options: Neo4jOptions): String

  def createStatementForNodes(options: Neo4jOptions): String
}

class Neo4jQueryService(private val options: Neo4jOptions,
                        val strategy: Neo4jQueryStrategy) extends Serializable {

  def createQuery(): String = options.query.queryType match {
    case QueryType.LABELS => strategy.createStatementForNodes(options)
    case QueryType.RELATIONSHIP => strategy.createStatementForRelationships(options)
    case QueryType.QUERY => strategy.createStatementForQuery(options)
    case _ => throw new UnsupportedOperationException(
      s"""Query Type not supported.
         |You provided ${options.query.queryType},
         |supported types: ${QueryType.values.mkString(",")}""".stripMargin)
  }
}
