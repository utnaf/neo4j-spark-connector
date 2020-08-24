package org.neo4j.spark.service

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.{And, EqualTo, Filter, IsNull, Not, Or}
import org.neo4j.cypherdsl.core.{Condition, Conditions, Cypher, Functions, PropertyContainer, StatementBuilder}
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.spark.{Neo4jOptions, NodeSaveMode, QueryType, RelationshipWriteStrategy}
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.Neo4jUtil

import collection.JavaConverters._

class Neo4jQueryWriteStrategy(private val saveMode: NodeSaveMode.Value) extends Neo4jQueryStrategy {
  override def createStatementForQuery(options: Neo4jOptions): String =
    s"""UNWIND ${"$"}events AS event
       |${options.query.value}
       |""".stripMargin

  override def createStatementForRelationships(options: Neo4jOptions): String = {
    val relationshipKeyword = saveMode match {
      case NodeSaveMode.Overwrite => "MERGE"
      case NodeSaveMode.ErrorIfExists => "CREATE"
      case _ => throw new UnsupportedOperationException(s"SaveMode $saveMode not supported")
    }

    val sourceKeyword = saveMode match {
      case NodeSaveMode.Overwrite => "MERGE"
      case NodeSaveMode.ErrorIfExists => "CREATE"
      case NodeSaveMode.Match => "MATCH"
      case _ => throw new UnsupportedOperationException(s"Source SaveMode $saveMode not supported")
    }

    val targetKeyword = saveMode match {
      case NodeSaveMode.Overwrite => "MERGE"
      case NodeSaveMode.ErrorIfExists => "CREATE"
      case NodeSaveMode.Match => "MATCH"
      case _ => throw new UnsupportedOperationException(s"Target SaveMode $saveMode not supported")
    }

    val relationship = options.relationshipMetadata.relationshipType.quote()
    val sourceLabels = options.relationshipMetadata.source.labels
      .map(_.quote)
      .mkString(":")
    val targetLabels = options.relationshipMetadata.target.labels
      .map(_.quote)
      .mkString(":")

    options.relationshipMetadata.writeStrategy match {
      case RelationshipWriteStrategy.NATIVE => s"""UNWIND ${"$"}events AS event
                                                  |$sourceKeyword (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS}${if (sourceLabels.isEmpty) "" else s":$sourceLabels"} )
                                                  |SET ${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS} = event.${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS}
                                                  |$targetKeyword (${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS}${if (targetLabels.isEmpty) "" else s":$targetLabels"})
                                                  |SET ${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS} = event.${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS}
                                                  |$relationshipKeyword (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS})-[${Neo4jUtil.RELATIONSHIP_ALIAS}:${relationship}]->(${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS})
                                                  |SET ${Neo4jUtil.RELATIONSHIP_ALIAS} = event.${Neo4jUtil.RELATIONSHIP_ALIAS}
                                                  |""".stripMargin
      case RelationshipWriteStrategy.KEYS => {
        val sourceKeys = options.relationshipMetadata.source.nodeKeys.map(key => {
          s"${key._2}:event.source.${key._1}"
        }).mkString(",")
        val targetKeys = options.relationshipMetadata.target.nodeKeys.map(key => {
          s"${key._2}:event.source.${key._1}"
        }).mkString(",")

        s"""UNWIND ${"$"}events AS event
           |$sourceKeyword (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS}${if (sourceLabels.isEmpty) "" else s":$sourceLabels"} ${if (sourceKeys.isEmpty) "" else s"{$sourceKeys}"})
           |$targetKeyword (${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS}${if (targetLabels.isEmpty) "" else s":$targetLabels"} ${if (targetKeys.isEmpty) "" else s"{$targetKeys}"})
           |$relationshipKeyword (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS})-[${Neo4jUtil.RELATIONSHIP_ALIAS}:${relationship}]->(${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS})
           |SET ${Neo4jUtil.RELATIONSHIP_ALIAS} = event.${Neo4jUtil.RELATIONSHIP_ALIAS}
           |""".stripMargin
      }
    }
  }

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val keyword = saveMode match {
      case NodeSaveMode.Overwrite => "MERGE"
      case NodeSaveMode.ErrorIfExists => "CREATE"
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

class Neo4jQueryReadStrategy(filters: Array[Filter] = Array.empty[Filter]) extends Neo4jQueryStrategy {
  private val renderer: Renderer = Renderer.getDefaultRenderer

  override def createStatementForQuery(options: Neo4jOptions): String = options.query.value

  override def createStatementForRelationships(options: Neo4jOptions): String = {
    val sourceNode = createNode(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, options.relationshipMetadata.source.labels)
    val targetNode = createNode(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, options.relationshipMetadata.target.labels)

    val relationship = sourceNode.relationshipTo(targetNode, options.relationshipMetadata.relationshipType)
      .named(Neo4jUtil.RELATIONSHIP_ALIAS)

    val matchQuery = Cypher.`match`(sourceNode).`match`(targetNode).`match`(relationship)

    def getContainer(filter: Filter): PropertyContainer = {
      if (filter.isAttribute(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS)) {
        sourceNode
      }
      else if (filter.isAttribute(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS)) {
        targetNode
      }
      else if (filter.isAttribute(Neo4jUtil.RELATIONSHIP_ALIAS)) {
        relationship
      }
      else {
        throw new IllegalArgumentException(s"Attribute '${filter.getAttribute.get}' is not valid")
      }
    }

    if (filters.nonEmpty) {
      def mapFilter(filter: Filter): Condition = {
        filter match {
          case and: And => mapFilter(and.left).and(mapFilter(and.right))
          case or: Or => mapFilter(or.left).or(mapFilter(or.right))
          case filter: Filter => Neo4jUtil.mapSparkFiltersToCypher(filter, getContainer(filter), filter.getAttributeWithoutEntityName)
        }
      }
      val cypherFilters = filters.map(mapFilter)

      assembleConditionQuery(matchQuery, cypherFilters)
    }

    renderer.render(matchQuery.returning(sourceNode, relationship, targetNode).build())
  }

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val node = createNode(Neo4jUtil.NODE_ALIAS, options.nodeMetadata.labels)
    val matchQuery = Cypher.`match`(node)

    if (filters.nonEmpty) {
      def mapFilter(filter: Filter): Condition = {
        filter match {
          case and: And => mapFilter(and.left).and(mapFilter(and.right))
          case or: Or => mapFilter(or.left).or(mapFilter(or.right))
          case filter: Filter => Neo4jUtil.mapSparkFiltersToCypher(filter, node)
        }
      }

      val cypherFilters = filters.map(mapFilter)
      assembleConditionQuery(matchQuery, cypherFilters)
    }

    renderer.render(matchQuery.returning(node).build())
  }

  private def assembleConditionQuery(matchQuery: StatementBuilder.OngoingReadingWithoutWhere, filters: Array[Condition]): StatementBuilder.OngoingReadingWithWhere = {
    matchQuery.where(
      filters.fold(Conditions.noCondition()) { (a, b) => a.and(b) }
    )
  }

  private def createNode(name: String, labels: Seq[String]) = {
    val primaryLabel = labels.head
    val otherLabels = labels.tail
    Cypher.node(primaryLabel, otherLabels.asJava).named(name)
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
