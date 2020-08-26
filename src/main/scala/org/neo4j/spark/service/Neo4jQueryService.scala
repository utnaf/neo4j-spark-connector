package org.neo4j.spark.service

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.{And, Filter, Or}
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.cypherdsl.core._
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.{Neo4jOptions, NodeSaveMode, QueryType}

import scala.collection.JavaConverters._

class Neo4jQueryWriteStrategy(private val saveMode: SaveMode) extends Neo4jQueryStrategy {
  override def createStatementForQuery(options: Neo4jOptions): String =
    s"""UNWIND ${"$"}events AS $BATCH_VARIABLE
       |${options.query.value}
       |""".stripMargin

  private def keywordFromSaveMode(saveMode: NodeSaveMode.Value): String = {
    saveMode match {
      case NodeSaveMode.Overwrite => "MERGE"
      case NodeSaveMode.ErrorIfExists => "CREATE"
      case NodeSaveMode.Match => "MATCH"
      case _ => throw new UnsupportedOperationException(s"Source SaveMode $saveMode not supported")
    }
  }

  override def createStatementForRelationships(options: Neo4jOptions): String = {
    val relationshipKeyword = saveMode match {
      case SaveMode.Overwrite => "MERGE"
      case SaveMode.ErrorIfExists => "CREATE"
      case _ => throw new UnsupportedOperationException(s"SaveMode $saveMode not supported")
    }

    val sourceKeyword = keywordFromSaveMode(options.relationshipMetadata.sourceSaveMode)
    val targetKeyword = keywordFromSaveMode(options.relationshipMetadata.targetSaveMode)

    val relationship = options.relationshipMetadata.relationshipType.quote()

    val sourceLabels = options.relationshipMetadata.source.labels
      .map(_.quote())
      .mkString(":")

    val targetLabels = options.relationshipMetadata.target.labels
      .map(_.quote())
      .mkString(":")

    val sourceKeys = options.relationshipMetadata.source.nodeKeys.map(key => {
      s"${key._2.quote()}:$BATCH_VARIABLE.source.${Neo4jWriteMappingStrategy.KEYS}.${key._1.removeAlias().quote()}"
    }).mkString(", ")
    val targetKeys = options.relationshipMetadata.target.nodeKeys.map(key => {
      s"${key._2.quote()}:$BATCH_VARIABLE.target.${Neo4jWriteMappingStrategy.KEYS}.${key._1.removeAlias().quote()}"
    }).mkString(", ")

    val sourceQueryPart = createQueryPart(sourceKeyword, sourceLabels, sourceKeys, Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS)
    val targetQueryPart = createQueryPart(targetKeyword, targetLabels, targetKeys, Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, Seq(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS))

    s"""UNWIND ${"$"}events AS $BATCH_VARIABLE
       |$sourceQueryPart
       |$targetQueryPart
       |$relationshipKeyword (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS})-[${Neo4jUtil.RELATIONSHIP_ALIAS}:${relationship}]->(${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS})
       |SET ${Neo4jUtil.RELATIONSHIP_ALIAS} += $BATCH_VARIABLE.${Neo4jUtil.RELATIONSHIP_ALIAS}.${Neo4jWriteMappingStrategy.PROPERTIES}
       |""".stripMargin
  }

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
      .map(k => s"$k: $BATCH_VARIABLE.${Neo4jWriteMappingStrategy.KEYS}.$k")
      .mkString(", ")

    s"""UNWIND ${"$"}events AS $BATCH_VARIABLE
       |$keyword (node${if (labels.isEmpty) "" else s":$labels"} ${if (keys.isEmpty) "" else s"{$keys}"})
       |SET node += $BATCH_VARIABLE.${Neo4jWriteMappingStrategy.PROPERTIES}
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
  val BATCH_VARIABLE = "event"

  def createStatementForQuery(options: Neo4jOptions): String

  def createStatementForRelationships(options: Neo4jOptions): String

  def createStatementForNodes(options: Neo4jOptions): String

  protected def createQueryPart(keyword: String, labels: String, keys: String, alias: String, additionalAliases: Seq[String] = Seq[String]()): String = {
    val withAliases = additionalAliases ++ Seq(BATCH_VARIABLE, alias)
    s"""$keyword ($alias${if (labels.isEmpty) "" else s":$labels"} ${if (keys.isEmpty) "" else s"{$keys}"})
    |SET $alias += $BATCH_VARIABLE.$alias.${Neo4jWriteMappingStrategy.PROPERTIES}
    |WITH ${withAliases.mkString(", ")}"""
  }
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
